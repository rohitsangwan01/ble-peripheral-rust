use super::bluez_utils::CharNotifyHandler;
use crate::gatt::peripheral_event::{
    PeripheralEvent, PeripheralRequest, ReadRequestResponse, RequestResponse, WriteRequestResponse,
};
use crate::gatt::properties::AttributePermission;
use crate::gatt::{characteristic, descriptor, properties, service};
use bluer::gatt::local::{
    characteristic_control, service_control, Characteristic, CharacteristicControl,
    CharacteristicControlHandle, CharacteristicNotify, CharacteristicNotifyMethod,
    CharacteristicWrite, CharacteristicWriteMethod, CharacteristicWriteRequest, Descriptor,
    DescriptorRead, DescriptorReadRequest, DescriptorWrite, DescriptorWriteRequest, ReqError,
    Service,
};
use bluer::gatt::local::{CharacteristicRead, CharacteristicReadRequest};
use futures::FutureExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use uuid::Uuid;

pub fn parse_services(
    gatt_services: Vec<service::Service>,
    sender_tx: Sender<PeripheralEvent>,
) -> (Vec<CharNotifyHandler>, Vec<Service>) {
    let mut services: Vec<Service> = vec![];
    let mut char_notify_handlers: Vec<CharNotifyHandler> = vec![];

    for service in gatt_services.iter().clone() {
        let (_, service_handle) = service_control();

        let mut characteristics: Vec<Characteristic> = Vec::new();
        let service_uuid = service.uuid.clone();

        for char in service.characteristics.clone() {
            let result = parse_characteristic(char.clone(), service.uuid, sender_tx.clone());

            if let Some(char_control) = result.1 {
                char_notify_handlers.push(CharNotifyHandler {
                    service_uuid: service_uuid.clone(),
                    characteristic_uuid: char.uuid.clone(),
                    control: char_control,
                });
            }

            characteristics.push(result.0);
        }

        let service = Service {
            uuid: service.uuid,
            primary: service.primary,
            characteristics,
            control_handle: service_handle,
            ..Default::default()
        };

        services.push(service);
    }
    (char_notify_handlers, services)
}

fn parse_characteristic(
    characteristic: characteristic::Characteristic,
    service_uuid: Uuid,
    sender_tx: Sender<PeripheralEvent>,
) -> (Characteristic, Option<CharacteristicControl>) {
    let descriptors: Vec<Descriptor> = characteristic
        .descriptors
        .iter()
        .map(|data| parse_descriptor(data.clone()))
        .collect();

    let char_notify = get_characteristic_notify(characteristic.clone());

    let mut control: Option<CharacteristicControl> = None;

    let control_handle = match char_notify {
        Some(_) => {
            let (ctrl, handle) = characteristic_control();
            control = Some(ctrl);
            handle
        }
        None => CharacteristicControlHandle::default(),
    };

    let char = Characteristic {
        uuid: characteristic.uuid,
        read: get_characteristic_read(characteristic.clone(), service_uuid, sender_tx.clone()),
        write: get_characteristic_write(characteristic.clone(), service_uuid, sender_tx.clone()),
        notify: char_notify,
        broadcast: characteristic
            .properties
            .contains(&properties::CharacteristicProperty::Broadcast),
        control_handle,
        descriptors, // TODO: fix descriptors
        ..Default::default()
    };
    return (char, control);
}

fn get_characteristic_read(
    characteristic: characteristic::Characteristic,
    service_uuid: Uuid,
    sender_tx: Sender<PeripheralEvent>,
) -> Option<CharacteristicRead> {
    if !characteristic
        .properties
        .contains(&properties::CharacteristicProperty::Read)
    {
        return None;
    }

    let is_secure = characteristic
        .permissions
        .contains(&AttributePermission::ReadEncryptionRequired);

    return Some(CharacteristicRead {
        read: true,
        secure_read: is_secure,
        fun: Box::new(move |request: CharacteristicReadRequest| {
            let sender_tx_clone = sender_tx.clone();
            async move {
                return on_read_request(
                    sender_tx_clone,
                    request,
                    service_uuid,
                    characteristic.uuid,
                )
                .await;
            }
            .boxed()
        }),
        ..Default::default()
    });
}

fn get_characteristic_write(
    characteristic: characteristic::Characteristic,
    service_uuid: Uuid,
    sender_tx: Sender<PeripheralEvent>,
) -> Option<CharacteristicWrite> {
    let is_write = characteristic
        .properties
        .contains(&properties::CharacteristicProperty::Write);
    let is_write_with_response = characteristic
        .properties
        .contains(&properties::CharacteristicProperty::WriteWithoutResponse);
    let is_authnticated_signed_write = characteristic
        .properties
        .contains(&properties::CharacteristicProperty::AuthenticatedSignedWrites);

    if !is_write && !is_write_with_response && !is_authnticated_signed_write {
        return None;
    }

    let is_write_encryption = characteristic
        .permissions
        .contains(&AttributePermission::WriteEncryptionRequired);

    return Some(CharacteristicWrite {
        write: is_write,
        write_without_response: is_write_with_response,
        authenticated_signed_writes: is_authnticated_signed_write,
        secure_write: is_write_encryption,
        method: CharacteristicWriteMethod::Fun(Box::new(
            move |value: Vec<u8>, request: CharacteristicWriteRequest| {
                let sender_tx_clone = sender_tx.clone();
                async move {
                    return on_write_request(
                        sender_tx_clone,
                        request,
                        service_uuid,
                        characteristic.uuid,
                        value,
                    )
                    .await;
                }
                .boxed()
            },
        )),
        ..Default::default()
    });
}

fn get_characteristic_notify(
    characteristic: characteristic::Characteristic,
) -> Option<CharacteristicNotify> {
    let notify = characteristic
        .properties
        .contains(&properties::CharacteristicProperty::Notify);
    let notify_encryption_required = characteristic
        .properties
        .contains(&properties::CharacteristicProperty::NotifyEncryptionRequired);
    let indicate = characteristic
        .properties
        .contains(&properties::CharacteristicProperty::Indicate);
    let indicate_encryption_required = characteristic
        .properties
        .contains(&properties::CharacteristicProperty::IndicateEncryptionRequired);

    if !notify && !notify_encryption_required && !indicate && !indicate_encryption_required {
        return None;
    }

    return Some(CharacteristicNotify {
        notify: notify || notify_encryption_required,
        indicate: indicate || indicate_encryption_required,
        method: CharacteristicNotifyMethod::Io,
        ..Default::default()
    });
}

fn parse_descriptor(descriptor: descriptor::Descriptor) -> Descriptor {
    return Descriptor {
        uuid: descriptor.uuid,
        read: get_descriptor_read(descriptor.clone()),
        write: get_descriptor_write(descriptor.clone()),
        ..Default::default()
    };
}

fn get_descriptor_read(descriptor: descriptor::Descriptor) -> Option<DescriptorRead> {
    if !descriptor
        .properties
        .contains(&properties::CharacteristicProperty::Read)
    {
        return None;
    }

    let is_secure = descriptor
        .permissions
        .contains(&AttributePermission::ReadEncryptionRequired);
    let value = descriptor.value;
    return Some(DescriptorRead {
        read: true,
        secure_read: is_secure,
        fun: Box::new(move |_: DescriptorReadRequest| {
            let value_clone = value.clone();
            async move {
                if value_clone.is_none() {
                    return Err(ReqError::Failed);
                }
                return Ok(value_clone.unwrap());
            }
            .boxed()
        }),
        ..Default::default()
    });
}

fn get_descriptor_write(descriptor: descriptor::Descriptor) -> Option<DescriptorWrite> {
    let is_write = descriptor
        .properties
        .contains(&properties::CharacteristicProperty::Write);
    let is_write_with_response = descriptor
        .properties
        .contains(&properties::CharacteristicProperty::WriteWithoutResponse);
    let is_authnticated_signed_write = descriptor
        .properties
        .contains(&properties::CharacteristicProperty::AuthenticatedSignedWrites);

    if !is_write && !is_write_with_response && !is_authnticated_signed_write {
        return None;
    }

    let is_write_encryption = descriptor
        .permissions
        .contains(&AttributePermission::WriteEncryptionRequired);

    return Some(DescriptorWrite {
        write: is_write || is_write_with_response,
        encrypt_authenticated_write: is_authnticated_signed_write,
        secure_write: is_write_encryption,
        fun: Box::new(move |value: Vec<u8>, _: DescriptorWriteRequest| {
            async move {
                log::info!("Descriptor Write {value:?}");
                return Ok(());
            }
            .boxed()
        }),
        ..Default::default()
    });
}

/// Handle Requests
async fn on_read_request(
    sender_tx: Sender<PeripheralEvent>,
    request: CharacteristicReadRequest,
    service_uuid: Uuid,
    characteristic: Uuid,
) -> Result<Vec<u8>, ReqError> {
    let (res_tx, res_rx) = oneshot::channel::<ReadRequestResponse>();
    if let Err(err) = sender_tx
        .send(PeripheralEvent::ReadRequest {
            request: PeripheralRequest {
                client: request.device_address.to_string(),
                service: service_uuid,
                characteristic,
                mtu: None,
            },
            offset: request.offset as u64,
            responder: res_tx,
        })
        .await
    {
        eprintln!("Error sending read request event: {:?}", err);
    }

    if let Ok(res) = res_rx.await {
        if let Some(err) = res.response.to_req_err() {
            return Err(err);
        }
        return Ok(res.value);
    }
    return Err(ReqError::Failed);
}

async fn on_write_request(
    sender_tx: Sender<PeripheralEvent>,
    request: CharacteristicWriteRequest,
    service_uuid: Uuid,
    characteristic: Uuid,
    value: Vec<u8>,
) -> Result<(), ReqError> {
    let (res_tx, res_rx) = oneshot::channel::<WriteRequestResponse>();
    if let Err(err) = sender_tx
        .send(PeripheralEvent::WriteRequest {
            request: PeripheralRequest {
                client: request.device_address.to_string(),
                service: service_uuid,
                characteristic,
                mtu: None,
            },
            offset: request.offset as u64,
            value,
            responder: res_tx,
        })
        .await
    {
        eprintln!("Error sending read request event: {:?}", err);
    }

    if let Ok(res) = res_rx.await {
        if let Some(err) = res.response.to_req_err() {
            return Err(err);
        }
        return Ok(());
    }
    return Err(ReqError::Failed);
}

impl RequestResponse {
    fn to_req_err(self) -> Option<ReqError> {
        match self {
            RequestResponse::Success => None,
            RequestResponse::InvalidHandle => Some(ReqError::Failed),
            RequestResponse::RequestNotSupported => Some(ReqError::NotSupported),
            RequestResponse::InvalidOffset => Some(ReqError::InvalidOffset),
            RequestResponse::UnlikelyError => Some(ReqError::Failed),
        }
    }
}
