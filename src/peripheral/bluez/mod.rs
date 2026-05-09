mod bluez_utils;
mod characteristic_utils;

use crate::{
    error::{Error, ErrorType},
    gatt::{
        peripheral_event::{PeripheralEvent, PeripheralRequest},
        service,
    },
};
use async_trait::async_trait;
use bluer::{
    adv::{Advertisement, AdvertisementHandle},
    gatt::{
        local::{Application, ApplicationHandle, CharacteristicControlEvent},
        CharacteristicWriter,
    },
    Adapter, AdapterEvent, AdapterProperty,
};
use bluez_utils::CharNotifyHandler;
use characteristic_utils::parse_services;
use futures::{channel::oneshot, StreamExt};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use super::PeripheralImpl;

#[derive(Debug)]
pub struct Peripheral {
    adapter: Adapter,
    services: Vec<service::Service>,
    adv_handle: Option<AdvertisementHandle>,
    app_handle: Option<ApplicationHandle>,
    sender_tx: Sender<PeripheralEvent>,
    writers: Arc<Mutex<HashMap<Uuid, Arc<CharacteristicWriter>>>>,
    _drop_tx: oneshot::Sender<()>,
}

#[async_trait]
impl PeripheralImpl for Peripheral {
    type Peripheral = Self;

    async fn new(sender_tx: Sender<PeripheralEvent>) -> Result<Self, Error> {
        let session = bluer::Session::new().await?;
        let adapter = session.default_adapter().await?;
        adapter.set_powered(true).await?;
        log::debug!(
            "Initialize Bluetooth adapter {} with address {}",
            adapter.name(),
            adapter.address().await?
        );

        let (drop_tx, drop_rx) = oneshot::channel();
        if let Ok(mut adapter_stream) = adapter.events().await {
            let sender = sender_tx.clone();
            tokio::spawn(async move {
                let stream_future = async {
                    while let Some(AdapterEvent::PropertyChanged(event)) =
                        adapter_stream.next().await
                    {
                        match event {
                            AdapterProperty::ActiveAdvertisingInstances(i) => {
                                log::debug!("ActiveAdvertisingInstances: {i}")
                            }
                            AdapterProperty::Powered(powered) => {
                                if let Err(err) = sender
                                    .send(PeripheralEvent::StateUpdate {
                                        is_powered: powered,
                                    })
                                    .await
                                {
                                    log::error!("Error sending state update event: {:?}", err);
                                }
                            }
                            _ => {}
                        }
                    }
                };
                tokio::select! {
                    _ = stream_future => {},
                    _ = drop_rx => {}
                }
            });
        }

        Ok(Peripheral {
            adapter,
            services: Vec::new(),
            adv_handle: None,
            app_handle: None,
            sender_tx,
            writers: Arc::new(Mutex::new(HashMap::new())),
            _drop_tx: drop_tx,
        })
    }

    async fn is_powered(&mut self) -> Result<bool, Error> {
        let result = self.adapter.is_powered().await?;
        return Ok(result);
    }

    async fn is_advertising(&mut self) -> Result<bool, Error> {
        let result = self.adapter.active_advertising_instances().await?;
        return Ok(result > 0 && self.adv_handle.is_some());
    }

    async fn start_advertising(&mut self, name: &str, uuids: &[Uuid], manufacturer_data_arg: Option<(u16, Vec<u8>)>) -> Result<(), Error> {
        let mut manufacturer_data = BTreeMap::new();
        if let Some((company_id, data)) = manufacturer_data_arg {
            manufacturer_data.insert(company_id, data);
        }

        let mut services: BTreeSet<Uuid> = BTreeSet::new();
        for uuid in uuids {
            services.insert(*uuid);
        }

        let le_advertisement = Advertisement {
            service_uuids: services,
            manufacturer_data,
            discoverable: Some(true),
            local_name: Some(name.to_string()),
            ..Default::default()
        };
        let adv_handle: AdvertisementHandle = self.adapter.advertise(le_advertisement).await?;

        let (handlers, services) = parse_services(self.services.clone(), self.sender_tx.clone());

        let app_handle = self
            .adapter
            .serve_gatt_application(Application {
                services,
                ..Default::default()
            })
            .await?;

        self.setup_char_handlers(handlers);

        self.adv_handle = Some(adv_handle);
        self.app_handle = Some(app_handle);
        Ok(())
    }

    async fn stop_advertising(&mut self) -> Result<(), Error> {
        self.adv_handle = None;
        self.app_handle = None;
        Ok(())
    }

    async fn add_service(&mut self, service: &service::Service) -> Result<(), Error> {
        self.services.push(service.clone());
        Ok(())
    }

    async fn update_characteristic(
        &mut self,
        characteristic: Uuid,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        let writers = match self.writers.lock() {
            Ok(w) => w,
            Err(err) => return Err(Error::from_string(err.to_string(), ErrorType::Bluez)),
        };
        let writer = writers.get(&characteristic).cloned();
        drop(writers);
        tokio::spawn(async move {
            if let Some(writer) = writer {
                if let Err(err) = writer.send(&value).await {
                    log::error!("Error sending value {err:?}")
                }
            }
        });
        Ok(())
    }
}

impl Peripheral {
    // Handle Characteristic Subscriptions
    fn setup_char_handlers(&mut self, handlers: Vec<CharNotifyHandler>) {
        for mut handler in handlers {
            let sender_tx = self.sender_tx.clone();
            let writers = self.writers.clone();

            tokio::spawn(async move {
                while let Some(CharacteristicControlEvent::Notify(writer)) =
                    handler.control.next().await
                {
                    let writer = Arc::new(writer);

                    let peripheral_request = PeripheralRequest {
                        client: writer.device_address().to_string(),
                        service: handler.service_uuid,
                        characteristic: handler.characteristic_uuid,
                    };

                    if let Err(err) = sender_tx
                        .send(PeripheralEvent::CharacteristicSubscriptionUpdate {
                            request: peripheral_request.clone(),
                            subscribed: true,
                        })
                        .await
                    {
                        log::error!("Error sending read request event: {:?}", err);
                    }

                    if let Ok(mut writers_lock) = writers.lock() {
                        writers_lock.insert(handler.characteristic_uuid, writer.clone());
                    } else {
                        log::error!("Failed to lock writers for adding a writer");
                    }

                    if let Err(err) = writer.closed().await {
                        log::error!("NotifyClosedErr {err:?}");
                    }

                    if let Ok(mut writers_lock) = writers.lock() {
                        writers_lock.remove(&handler.characteristic_uuid);
                    } else {
                        log::error!("Failed to lock writers for removing a writer");
                    }

                    if let Err(err) = sender_tx
                        .send(PeripheralEvent::CharacteristicSubscriptionUpdate {
                            request: peripheral_request,
                            subscribed: false,
                        })
                        .await
                    {
                        log::error!("Error sending read request event: {:?}", err);
                    }
                }
            });
        }
    }
}

impl Drop for Peripheral {
    fn drop(&mut self) {
        // required for drop order
    }
}
