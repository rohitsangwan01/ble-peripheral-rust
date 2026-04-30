use super::mac_extensions::UuidExtension;
use crate::{
    error::{Error, ErrorType},
    gatt::peripheral_event::{
        PeripheralEvent, PeripheralRequest, ReadRequestResponse, RequestResponse,
        WriteRequestResponse,
    },
};
use objc2::{declare_class, msg_send_id, mutability, rc::Retained, ClassType, DeclaredClass};
use objc2_core_bluetooth::{
    CBATTError, CBATTRequest, CBCentral, CBCharacteristic, CBManagerState, CBPeripheralManager,
    CBPeripheralManagerDelegate, CBService,
};
use objc2_foundation::{NSArray, NSData, NSError, NSObject, NSObjectProtocol};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc::Sender, oneshot};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

pub struct IVars {
    pub sender: Sender<PeripheralEvent>,
    pub services_resolver: Arc<Mutex<HashMap<Uuid, oneshot::Sender<Option<String>>>>>,
    pub advertisement_resolver: Arc<Mutex<Option<oneshot::Sender<Option<String>>>>>,
}

declare_class!(
    #[derive(Debug)]
    pub struct PeripheralDelegate;

    unsafe impl ClassType for PeripheralDelegate {
        type Super = NSObject;
        type Mutability = mutability::InteriorMutable;
        const NAME: &'static str = "PeripheralManagerDelegate";
    }

    impl DeclaredClass for PeripheralDelegate {
        type Ivars = IVars;
    }

    unsafe impl NSObjectProtocol for PeripheralDelegate {}

    unsafe impl CBPeripheralManagerDelegate for PeripheralDelegate {
        #[method(peripheralManagerDidUpdateState:)]
         fn delegate_peripheralmanagerdidupdatestate(&self, peripheral: &CBPeripheralManager){
                let state = unsafe { peripheral.state() };
                self.send_event(PeripheralEvent::StateUpdate { is_powered : state == CBManagerState::PoweredOn });
         }

        #[method(peripheralManagerDidStartAdvertising:error:)]
        fn delegate_peripheralmanagerdidstartadvertising_error(&self, _: &CBPeripheralManager,error: Option<&NSError>){
            let mut error_desc: Option<String> = None;
            if let Some(error) = error {
                error_desc = Some(error.localizedDescription().to_string());
            }
            log::debug!("Advertising, Error: {error_desc:?}");
            if let Ok(mut resolver) = self.ivars().advertisement_resolver.lock() {
                let sender_opt = resolver.take();
                drop(resolver);
                if let Some(sender) = sender_opt {
                    let _ = sender.send(error_desc);
                }
            }
        }

        #[method(peripheralManager:didAddService:error:)]
         fn delegate_peripheralmanager_didaddservice_error(&self, _: &CBPeripheralManager,service: &CBService, error: Option<&NSError>){
            let mut error_desc: Option<String> = None;
            if let Some(error) = error {
                error_desc = Some(error.localizedDescription().to_string());
            }
            log::debug!("AddServices, Error: {error_desc:?}");

            if let  Ok(mut resolver) = self.ivars().services_resolver.lock() {
                if let Some(sender) = resolver.remove(&service.get_uuid()) {
                    drop(resolver);
                    let _ = sender.send(error_desc);
                }
            }
        }

        #[method(peripheralManager:central:didSubscribeToCharacteristic:)]
         fn delegate_peripheralmanager_central_didsubscribetocharacteristic(
            &self,
            _: &CBPeripheralManager,
            central: &CBCentral,
            characteristic: &CBCharacteristic,
        ){
            unsafe{
                let service: Option<Retained<CBService>> = characteristic.service();
                if service.is_none() {
                    return;
                }
                self.send_event(PeripheralEvent::CharacteristicSubscriptionUpdate {
                    request: PeripheralRequest {
                        client: central.identifier().to_string(),
                        service: characteristic.service().unwrap().get_uuid(),
                        characteristic: characteristic.get_uuid(),
                        mtu: Some(central.maximumUpdateValueLength() as usize),
                    },
                    subscribed: true,
                });
            }
        }

        #[method(peripheralManager:central:didUnsubscribeFromCharacteristic:)]
         fn delegate_peripheralmanager_central_didunsubscribefromcharacteristic(
            &self,
            _: &CBPeripheralManager,
            central: &CBCentral,
            characteristic: &CBCharacteristic,
        ){  unsafe{
            let service: Option<Retained<CBService>> = characteristic.service();
            if service.is_none() {
                return;
            }

            self.send_event(PeripheralEvent::CharacteristicSubscriptionUpdate {
               request: PeripheralRequest {
                    client: central.identifier().to_string(),
                    service: characteristic.service().unwrap().get_uuid(),
                    characteristic: characteristic.get_uuid(),
                    mtu: Some(central.maximumUpdateValueLength() as usize),
                },
                subscribed: false,
            });
        }}

        #[method(peripheralManager:didReceiveReadRequest:)]
         fn delegate_peripheralmanager_didreceivereadrequest(
            &self,
            manager: &CBPeripheralManager,
            request: &CBATTRequest,
        ){
            unsafe{
                let service = request.characteristic().service();
                if service.is_none() {
                    return;
                }
                let central = request.central();
                let characteristic = request.characteristic();

                self.send_read_request(
                    PeripheralRequest{
                        client: central.identifier().to_string(),
                        service: characteristic.service().unwrap().get_uuid(),
                        characteristic: characteristic.get_uuid(),
                        mtu: Some(central.maximumUpdateValueLength() as usize),
                    },
                    manager,
                    request,
                );
            }
        }

        #[method(peripheralManager:didReceiveWriteRequests:)]
         fn delegate_peripheralmanager_didreceivewriterequests(
            &self,
            manager: &CBPeripheralManager,
            requests: &NSArray<CBATTRequest>,
        ){
            for request in requests {
                unsafe{
                    let service = request.characteristic().service();
                    if service.is_none() {
                        return;
                    }
                    let mut value: Vec<u8> = Vec::new();
                    if let Some(ns_data) = request.value() {
                       value = ns_data.bytes().to_vec();
                    }
                    let central = request.central();
                    let characteristic = request.characteristic();

                    self.send_write_request(
                        PeripheralRequest{
                             client: central.identifier().to_string(),
                            service: characteristic.service().unwrap().get_uuid(),
                            characteristic: characteristic.get_uuid(),
                            mtu: Some(central.maximumUpdateValueLength() as usize),
                        },
                        manager,
                        request,
                        value,
                    );
                }
            }
        }
    }
);

impl PeripheralDelegate {
    pub fn new(sender: Sender<PeripheralEvent>) -> Retained<PeripheralDelegate> {
        let this = PeripheralDelegate::alloc().set_ivars(IVars {
            sender,
            services_resolver: Arc::new(Mutex::new(HashMap::new())),
            advertisement_resolver: Arc::new(Mutex::new(None)),
        });
        return unsafe { msg_send_id![super(this), init] };
    }

    pub fn is_waiting_for_advertisement_result(&self) -> bool {
        if let Ok(resolver) = self.ivars().advertisement_resolver.lock() {
            return resolver.is_some();
        }
        return false;
    }

    /// Wait for delegate to ensure advertisement started successfully
    pub async fn ensure_advertisement_started(&self) -> Result<(), Error> {
        let (sender, receiver) = oneshot::channel::<Option<String>>();
        {
            if let Ok(mut resolver) = self.ivars().advertisement_resolver.lock() {
                *resolver = Some(sender);
            }
        }

        let event = timeout(Duration::from_secs(5), receiver).await;

        {
            if let Ok(mut resolver) = self.ivars().advertisement_resolver.lock() {
                *resolver = None;
            }
        }
        return self.resolve_event(event);
    }

    pub fn is_waiting_for_service_result(&self, service: Uuid) -> bool {
        if let Ok(resolver) = self.ivars().services_resolver.lock() {
            return resolver.get(&service).is_some();
        }
        return false;
    }

    // Wait for event from delegate if service added successfully
    pub async fn ensure_service_added(&self, service: Uuid) -> Result<(), Error> {
        let (sender, receiver) = oneshot::channel::<Option<String>>();
        {
            if let Ok(mut resolver) = self.ivars().services_resolver.lock() {
                resolver.insert(service, sender);
            }
        }

        let event = timeout(Duration::from_secs(5), receiver).await;

        {
            if let Ok(mut resolver) = self.ivars().services_resolver.lock() {
                resolver.remove(&service);
            }
        }

        return self.resolve_event(event);
    }

    fn resolve_event(
        &self,
        event: Result<
            Result<Option<String>, oneshot::error::RecvError>,
            tokio::time::error::Elapsed,
        >,
    ) -> Result<(), Error> {
        let event = match event {
            Ok(Ok(event)) => event,
            Ok(Err(e)) => {
                return Err(Error::from_string(
                    format!("Channel error while waiting: {}", e),
                    ErrorType::CoreBluetooth,
                ));
            }
            Err(_) => {
                return Err(Error::from_string(
                    "Timeout waiting for event".to_string(),
                    ErrorType::CoreBluetooth,
                ));
            }
        };

        if let Some(error) = event {
            return Err(Error::from_string(error, ErrorType::CoreBluetooth));
        }

        return Ok(());
    }
}

/// Event handler
impl PeripheralDelegate {
    fn send_event(&self, event: PeripheralEvent) {
        let sender = self.ivars().sender.clone();
        futures::executor::block_on(async {
            if let Err(e) = sender.send(event).await {
                log::error!("Error sending delegate event: {}", e);
            }
        });
    }

    fn send_read_request(
        &self,
        peripheral_request: PeripheralRequest,
        manager: &CBPeripheralManager,
        request: &CBATTRequest,
    ) {
        let sender = self.ivars().sender.clone();
        unsafe {
            futures::executor::block_on(async {
                let (resp_tx, resp_rx) = oneshot::channel::<ReadRequestResponse>();

                if let Err(e) = sender
                    .send(PeripheralEvent::ReadRequest {
                        request: peripheral_request,
                        offset: request.offset() as u64,
                        responder: resp_tx,
                    })
                    .await
                {
                    log::error!("Error sending delegate event: {}", e);
                    return;
                }

                let mut cb_att_error = CBATTError::InvalidHandle;
                if let Ok(result) = resp_rx.await {
                    cb_att_error = result.response.to_cb_error();
                    request.setValue(Some(&NSData::from_vec(result.value)));
                }
                manager.respondToRequest_withResult(request, cb_att_error);
            });
        };
    }

    fn send_write_request(
        &self,
        peripheral_request: PeripheralRequest,
        manager: &CBPeripheralManager,
        request: &CBATTRequest,
        value: Vec<u8>,
    ) {
        let sender = self.ivars().sender.clone();
        unsafe {
            futures::executor::block_on(async {
                let (resp_tx, resp_rx) = oneshot::channel::<WriteRequestResponse>();

                if let Err(e) = sender
                    .send(PeripheralEvent::WriteRequest {
                        request: peripheral_request,
                        value,
                        offset: request.offset() as u64,
                        responder: resp_tx,
                    })
                    .await
                {
                    log::error!("Error sending delegate event: {}", e);
                    return;
                }

                let mut cb_att_error = CBATTError::InvalidHandle;
                if let Ok(result) = resp_rx.await {
                    cb_att_error = result.response.to_cb_error();
                }

                manager.respondToRequest_withResult(request, cb_att_error);
            });
        };
    }
}

impl RequestResponse {
    fn to_cb_error(self) -> CBATTError {
        match self {
            RequestResponse::Success => CBATTError::Success,
            RequestResponse::InvalidHandle => CBATTError::InvalidHandle,
            RequestResponse::RequestNotSupported => CBATTError::RequestNotSupported,
            RequestResponse::InvalidOffset => CBATTError::InvalidOffset,
            RequestResponse::UnlikelyError => CBATTError::UnlikelyError,
        }
    }
}
