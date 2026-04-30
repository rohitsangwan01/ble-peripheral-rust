use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::gatt::peripheral_event::{
    PeripheralEvent, PeripheralRequest, ReadRequestResponse, RequestResponse, WriteRequestResponse,
};
use crate::peripheral::winrt::win_utils::{
    buffer_to_vec, device_id_from_session, to_uuid, vec_to_buffer,
};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use uuid::Uuid;
use windows::core::IInspectable;
use windows::Devices::Bluetooth::GenericAttributeProfile::{
    GattProtocolError, GattServiceProviderAdvertisementStatus, GattSubscribedClient,
};
use windows::Devices::Radios::{Radio, RadioState};
use windows::Foundation::Collections::IVectorView;
use windows::{
    Devices::Bluetooth::GenericAttributeProfile::{
        GattLocalCharacteristic, GattReadRequestedEventArgs, GattServiceProvider,
        GattServiceProviderAdvertisementStatusChangedEventArgs, GattWriteRequestedEventArgs,
    },
    Foundation::TypedEventHandler,
};

pub struct WinEventHandler {
    sender_tx: Sender<PeripheralEvent>,
    connected_clients: Arc<RwLock<HashMap<(Uuid, Uuid), Vec<String>>>>,
}

impl WinEventHandler {
    pub fn new(sender_tx: Sender<PeripheralEvent>) -> Self {
        Self {
            sender_tx,
            connected_clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_radio_listener(&self) -> TypedEventHandler<Radio, IInspectable> {
        let sender_tx: Sender<PeripheralEvent> = self.sender_tx.clone();

        return TypedEventHandler::new(
            move |originator: &Option<Radio>, _: &Option<IInspectable>| {
                let radio = originator.as_ref().unwrap();
                let is_on = radio.State().unwrap() == RadioState::On;
                futures::executor::block_on(async {
                    if let Err(err) = sender_tx
                        .send(PeripheralEvent::StateUpdate { is_powered: is_on })
                        .await
                    {
                        log::error!("Error sending delegate event: {}", err);
                    }
                });
                Ok(())
            },
        );
    }

    pub fn create_advertisement_status_handler(
        &self,
    ) -> TypedEventHandler<
        GattServiceProvider,
        GattServiceProviderAdvertisementStatusChangedEventArgs,
    > {
        TypedEventHandler::new(move |originator: &Option<GattServiceProvider>, args: &Option<GattServiceProviderAdvertisementStatusChangedEventArgs>| {
            let service = originator.as_ref().unwrap();
            let event_args = args.as_ref().unwrap();
            let status = event_args.Status()?;
            log::debug!("Advertisement Status: {:?}: Started: {:?}", 
                to_uuid(&service.Service().unwrap().Uuid().unwrap()),
                status == GattServiceProviderAdvertisementStatus::Started);
            Ok(())
        })
    }

    pub fn create_subscribe_handler(
        &self,
        service_uuid: Uuid,
    ) -> TypedEventHandler<GattLocalCharacteristic, IInspectable> {
        let connected_clients = Arc::clone(&self.connected_clients);
        let sender_tx: Sender<PeripheralEvent> = self.sender_tx.clone();

        TypedEventHandler::new(
            move |originator: &Option<GattLocalCharacteristic>, _: &Option<IInspectable>| {
                let characteristic: &GattLocalCharacteristic = originator.as_ref().unwrap();
                let characteristic_uuid = to_uuid(&characteristic.Uuid().unwrap());

                let subscribed_clients: IVectorView<GattSubscribedClient> =
                    characteristic.SubscribedClients().unwrap();
                    
                let new_clients: Vec<String> = subscribed_clients
                    .into_iter()
                    .map(|client| device_id_from_session(client.Session().unwrap()))
                    .collect();

                let mut old_clients_store = connected_clients.write().unwrap();
                let mut added_clients: Vec<String> = Vec::new();
                let mut removed_clients: Vec<String> = Vec::new();

                if let Some(old_clients) = old_clients_store
                    .get_mut(&(service_uuid, to_uuid(&characteristic.Uuid().unwrap())))
                {
                    for client in &new_clients {
                        if !old_clients.contains(client) {
                            added_clients.push(client.clone());
                        }
                    }
                    for client in old_clients.clone() {
                        if !new_clients.contains(&client) {
                            removed_clients.push(client.clone());
                        }
                    }

                    *old_clients = new_clients;
                } else {
                    old_clients_store
                        .insert((service_uuid, characteristic_uuid), new_clients.clone());
                    added_clients.extend(new_clients.clone());
                }

                // Update Newly added/removed clients
                futures::executor::block_on(async {
                    for client in added_clients {
                        if let Err(err) = sender_tx
                            .send(PeripheralEvent::CharacteristicSubscriptionUpdate {
                                request: PeripheralRequest {
                                    client,
                                    service: service_uuid,
                                    characteristic: characteristic_uuid,
                                    mtu: None,
                                },
                                subscribed: true,
                            })
                            .await
                        {
                            log::error!("Error sending delegate event: {}", err);
                        }
                    }

                    for client in removed_clients {
                        if let Err(err) = sender_tx
                            .send(PeripheralEvent::CharacteristicSubscriptionUpdate {
                                request: PeripheralRequest {
                                    client,
                                    service: service_uuid,
                                    characteristic: characteristic_uuid,
                                    mtu: None,
                                },
                                subscribed: false,
                            })
                            .await
                        {
                            log::error!("Error sending delegate event: {}", err);
                        }
                    }
                });
                Ok(())
            },
        )
    }

    pub fn create_read_handler(
        &mut self,
        service_uuid: Uuid,
    ) -> TypedEventHandler<GattLocalCharacteristic, GattReadRequestedEventArgs> {
        let sender_tx: Sender<PeripheralEvent> = self.sender_tx.clone();

        TypedEventHandler::new(
            move |originator: &Option<GattLocalCharacteristic>,
                  args: &Option<GattReadRequestedEventArgs>| {
                let event_args: &GattReadRequestedEventArgs = args.as_ref().unwrap();
                let characteristic = originator.as_ref().unwrap();

                futures::executor::block_on(async {
                    let request = event_args.GetRequestAsync().unwrap().await;
                    if let Ok(request) = request {
                        // let mtu = event_args.Session().unwrap().MaxPduSize().unwrap();
                        let (resp_tx, resp_rx) = oneshot::channel::<ReadRequestResponse>();
                        if let Err(e) = sender_tx
                            .send(PeripheralEvent::ReadRequest {
                                request: PeripheralRequest {
                                    client: device_id_from_session(event_args.Session().unwrap()),
                                    service: service_uuid,
                                    characteristic: to_uuid(&characteristic.Uuid().unwrap()),
                                    mtu: None,
                                },
                                offset: request.Offset().unwrap() as u64,
                                responder: resp_tx,
                            })
                            .await
                        {
                            log::error!("Error sending delegate event: {}", e);
                            return;
                        }

                        if let Ok(result) = resp_rx.await {
                            if result.response == RequestResponse::Success {
                                request
                                    .RespondWithValue(&vec_to_buffer(result.value))
                                    .unwrap();
                                return;
                            }
                            request
                                .RespondWithProtocolError(result.response.to_gatt_protocol_error())
                                .unwrap();
                            return;
                        }

                        request
                            .RespondWithProtocolError(GattProtocolError::UnlikelyError().unwrap())
                            .unwrap();
                    }
                });

                return Ok(());
            },
        )
    }

    pub fn create_write_handler(
        &self,
        service_uuid: Uuid,
    ) -> TypedEventHandler<GattLocalCharacteristic, GattWriteRequestedEventArgs> {
        let sender_tx = self.sender_tx.clone();

        TypedEventHandler::new(
            move |originator: &Option<GattLocalCharacteristic>,
                  args: &Option<GattWriteRequestedEventArgs>| {
                let event_args = args.as_ref().unwrap();
                let characteristic = originator.as_ref().unwrap();
                futures::executor::block_on(async {
                    if let Ok(request) = event_args.GetRequestAsync().unwrap().await {
                        // let offset = request.Offset().unwrap();
                        // let mtu = event_args.Session().unwrap().MaxPduSize().unwrap();
                        let (resp_tx, resp_rx) = oneshot::channel::<WriteRequestResponse>();
                        let char_uuid = to_uuid(&characteristic.Uuid().unwrap());
                        if let Err(e) = sender_tx
                            .send(PeripheralEvent::WriteRequest {
                                request: PeripheralRequest {
                                    client: device_id_from_session(event_args.Session().unwrap()),
                                    service: service_uuid,
                                    characteristic: char_uuid,
                                    mtu: None,
                                },
                                value: buffer_to_vec(&request.Value().unwrap()),
                                offset: request.Offset().unwrap() as u64,
                                responder: resp_tx,
                            })
                            .await
                        {
                            log::error!("Error sending delegate event: {}", e);
                            return;
                        }

                        if let Ok(result) = resp_rx.await {
                            if result.response == RequestResponse::Success {
                                request.Respond().unwrap();
                                return;
                            }
                            request
                                .RespondWithProtocolError(result.response.to_gatt_protocol_error())
                                .unwrap();
                            return;
                        }

                        request
                            .RespondWithProtocolError(GattProtocolError::UnlikelyError().unwrap())
                            .unwrap();
                    }
                });

                return Ok(());
            },
        )
    }
}

impl RequestResponse {
    fn to_gatt_protocol_error(self) -> u8 {
        let result = match self {
            RequestResponse::Success => Ok(0),
            RequestResponse::InvalidHandle => GattProtocolError::InvalidHandle(),
            RequestResponse::RequestNotSupported => GattProtocolError::RequestNotSupported(),
            RequestResponse::InvalidOffset => GattProtocolError::InvalidOffset(),
            RequestResponse::UnlikelyError => GattProtocolError::UnlikelyError(),
        };
        if let Ok(value) = result {
            return value;
        }
        return 0;
    }
}
