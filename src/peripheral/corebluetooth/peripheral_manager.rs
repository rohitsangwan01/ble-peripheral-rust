use super::mac_utils;
use super::peripheral_delegate::PeripheralDelegate;
use super::{characteristic_utils::parse_characteristic, mac_extensions::uuid_to_cbuuid};
use crate::error::{Error, ErrorType};
use crate::gatt::peripheral_event::PeripheralEvent;
use crate::gatt::service::Service;
use objc2::msg_send_id;
use objc2::{rc::Retained, runtime::AnyObject, ClassType};
use objc2_core_bluetooth::{
    CBAdvertisementDataLocalNameKey, CBAdvertisementDataServiceUUIDsKey, CBCharacteristic,
    CBManager, CBManagerAuthorization, CBManagerState, CBMutableCharacteristic, CBMutableService,
    CBPeripheralManager,
};
use objc2_foundation::{NSArray, NSData, NSDictionary, NSString};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::thread;
use tokio::runtime;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use uuid::Uuid;

pub(crate) enum ManagerEvent {
    IsPowered {
        responder: oneshot::Sender<Result<bool, Error>>,
    },
    IsAdvertising {
        responder: oneshot::Sender<Result<bool, Error>>,
    },
    StartAdvertising {
        name: String,
        uuids: Vec<Uuid>,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    StopAdvertising {
        responder: oneshot::Sender<Result<(), Error>>,
    },
    AddService {
        service: Service,
        responder: oneshot::Sender<Result<(), Error>>,
    },
    UpdateCharacteristic {
        characteristic: Uuid,
        value: Vec<u8>,
        responder: oneshot::Sender<Result<(), Error>>,
    },
}

static PERIPHERAL_THREAD: OnceCell<()> = OnceCell::new();

// Handle Peripheral Manager and all communication in a separate thread
pub fn run_peripheral_thread(sender: Sender<PeripheralEvent>, listener: Receiver<ManagerEvent>) {
    PERIPHERAL_THREAD.get_or_init(|| {
        thread::spawn(move || {
            let runtime = runtime::Builder::new_current_thread().enable_time().build();
            if runtime.is_err() {
                log::error!("Failed to create runtime");
                return;
            }
            runtime.unwrap().block_on(async move {
                let mut peripheral_manager = PeripheralManager::new(sender, listener);
                loop {
                    peripheral_manager.handle_event().await;
                }
            })
        });
    });
}

#[derive(Debug)]
struct PeripheralManager {
    manager_event: Receiver<ManagerEvent>,
    cb_peripheral_manager: Retained<CBPeripheralManager>,
    peripheral_delegate: Retained<PeripheralDelegate>,
    cached_characteristics: HashMap<Uuid, Retained<CBMutableCharacteristic>>,
}

impl PeripheralManager {
    fn new(sender_tx: mpsc::Sender<PeripheralEvent>, listener: Receiver<ManagerEvent>) -> Self {
        let delegate: Retained<PeripheralDelegate> = PeripheralDelegate::new(sender_tx);
        let label: CString = CString::new("CBqueue").unwrap();
        let queue: *mut std::ffi::c_void = unsafe {
            mac_utils::dispatch_queue_create(label.as_ptr(), mac_utils::DISPATCH_QUEUE_SERIAL)
        };
        let queue: *mut AnyObject = queue.cast();
        let peripheral_manager: Retained<CBPeripheralManager> = unsafe {
            msg_send_id![CBPeripheralManager::alloc(), initWithDelegate: &**delegate, queue: queue]
        };

        Self {
            manager_event: listener,
            cb_peripheral_manager: peripheral_manager,
            peripheral_delegate: delegate,
            cached_characteristics: HashMap::new(),
        }
    }

    async fn handle_event(&mut self) {
        if let Some(event) = self.manager_event.recv().await {
            let _ = match event {
                ManagerEvent::IsPowered { responder } => {
                    let _ = responder.send(Ok(self.is_powered()));
                }
                ManagerEvent::IsAdvertising { responder } => {
                    let _ = responder.send(Ok(self.is_advertising()));
                }
                ManagerEvent::StartAdvertising {
                    name,
                    uuids,
                    responder,
                } => {
                    let _ = responder.send(self.start_advertising(&name, &uuids).await);
                }
                ManagerEvent::StopAdvertising { responder } => {
                    let _ = responder.send(Ok(self.stop_advertising()));
                }
                ManagerEvent::AddService { service, responder } => {
                    let _ = responder.send(self.add_service(&service).await);
                }
                ManagerEvent::UpdateCharacteristic {
                    characteristic,
                    value,
                    responder,
                } => {
                    let _ = responder.send(self.update_characteristic(characteristic, value).await);
                }
            };
        }
    }

    fn is_powered(self: &Self) -> bool {
        unsafe {
            let state = self.cb_peripheral_manager.state();
            state == CBManagerState::PoweredOn
        }
    }

    async fn start_advertising(self: &Self, name: &str, uuids: &[Uuid]) -> Result<(), Error> {
        if self
            .peripheral_delegate
            .is_waiting_for_advertisement_result()
        {
            return Err(Error::from_string(
                "Already in progress".to_string(),
                ErrorType::CoreBluetooth,
            ));
        }

        let mut keys: Vec<&NSString> = vec![];
        let mut objects: Vec<Retained<AnyObject>> = vec![];

        unsafe {
            keys.push(CBAdvertisementDataLocalNameKey);
            objects.push(Retained::cast(NSString::from_str(name)));

            keys.push(CBAdvertisementDataServiceUUIDsKey);
            objects.push(Retained::cast(NSArray::from_vec(
                uuids.iter().map(|u| uuid_to_cbuuid(u.clone())).collect(),
            )));
        }

        let advertising_data: Retained<NSDictionary<NSString, AnyObject>> =
            NSDictionary::from_vec(&keys, objects);

        unsafe {
            self.cb_peripheral_manager
                .startAdvertising(Some(&advertising_data));
        }

        return self
            .peripheral_delegate
            .ensure_advertisement_started()
            .await;
    }

    fn stop_advertising(self: &Self) {
        unsafe {
            self.cb_peripheral_manager.stopAdvertising();
        }
    }

    fn is_advertising(self: &Self) -> bool {
        unsafe { self.cb_peripheral_manager.isAdvertising() }
    }

    async fn update_characteristic(
        &mut self,
        characteristic: Uuid,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        if let Some(char) = self.cached_characteristics.get(&characteristic) {
            let success = unsafe {
                self.cb_peripheral_manager
                    .updateValue_forCharacteristic_onSubscribedCentrals(
                        &NSData::from_vec(value.clone()),
                        char,
                        None,
                    )
            };
            if !success {
                return Err(Error::from_string(
                    "Transmit queue is full".to_string(),
                    ErrorType::CoreBluetooth,
                ));
            }
        }
        return Ok(());
    }

    // Peripheral with cache value must only have Read permission, else it will crash
    // TODO: throw proper error, or catch Objc errors
    async fn add_service(&mut self, service: &Service) -> Result<(), Error> {
        if self
            .peripheral_delegate
            .is_waiting_for_service_result(service.uuid)
        {
            return Err(Error::from_string(
                "Already in progress".to_string(),
                ErrorType::CoreBluetooth,
            ));
        }

        unsafe {
            let mut characteristics: Vec<Retained<CBCharacteristic>> = Vec::new();

            for char in service.characteristics.iter() {
                let cb_char = parse_characteristic(char);
                characteristics.push(Retained::into_super(cb_char.clone()));
                self.cached_characteristics.insert(char.uuid, cb_char);
            }

            let mutable_service: Retained<CBMutableService> =
                CBMutableService::initWithType_primary(
                    CBMutableService::alloc(),
                    &uuid_to_cbuuid(service.uuid),
                    service.primary,
                );

            if !characteristics.is_empty() {
                let chars = NSArray::from_vec(characteristics);
                mutable_service.setCharacteristics(Some(&chars));
            }

            self.cb_peripheral_manager.addService(&mutable_service);

            return self
                .peripheral_delegate
                .ensure_service_added(service.uuid)
                .await;
        }
    }
}

pub fn is_authorized() -> bool {
    let authorization = unsafe { CBManager::authorization_class() };
    return authorization != CBManagerAuthorization::Restricted
        && authorization != CBManagerAuthorization::Denied;
}
