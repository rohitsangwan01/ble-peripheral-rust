mod characteristic_utils;
pub mod error;
mod mac_extensions;
mod mac_utils;
pub mod peripheral_delegate;
mod peripheral_manager;

use crate::{
    error::{Error, ErrorType},
    gatt::{peripheral_event::PeripheralEvent, service::Service},
};
use async_trait::async_trait;
use peripheral_manager::{is_authorized, run_peripheral_thread, ManagerEvent};
use tokio::sync::{mpsc::Sender, oneshot};
use uuid::Uuid;

use super::PeripheralImpl;

pub struct Peripheral {
    manager_tx: Sender<ManagerEvent>,
}

#[async_trait]
impl PeripheralImpl for Peripheral {
    type Peripheral = Self;

    async fn new(sender_tx: Sender<PeripheralEvent>) -> Result<Self, Error> {
        if !is_authorized() {
            return Err(Error::from_type(ErrorType::PermissionDenied));
        }
        let (manager_tx, manager_rx) = tokio::sync::mpsc::channel(256);
        run_peripheral_thread(sender_tx, manager_rx);
        Ok(Peripheral { manager_tx })
    }

    async fn is_powered(&mut self) -> Result<bool, Error> {
        let (responder, responder_rx) = oneshot::channel();
        self.manager_tx
            .send(ManagerEvent::IsPowered { responder })
            .await?;
        return responder_rx.await?;
    }

    async fn is_advertising(&mut self) -> Result<bool, Error> {
        let (responder, responder_rx) = oneshot::channel();
        self.manager_tx
            .send(ManagerEvent::IsAdvertising { responder })
            .await?;
        return responder_rx.await?;
    }

    async fn start_advertising(&mut self, name: &str, uuids: &[Uuid], manufacturer_data: Option<(u16, Vec<u8>)>) -> Result<(), Error> {
        let (responder, responder_rx) = oneshot::channel();
        self.manager_tx
            .send(ManagerEvent::StartAdvertising {
                name: name.to_string(),
                uuids: uuids.to_vec(),
                manufacturer_data,
                responder,
            })
            .await?;
        return responder_rx.await?;
    }

    async fn stop_advertising(&mut self) -> Result<(), Error> {
        let (responder, responder_rx) = oneshot::channel();
        self.manager_tx
            .send(ManagerEvent::StopAdvertising { responder })
            .await?;
        return responder_rx.await?;
    }

    async fn add_service(&mut self, service: &Service) -> Result<(), Error> {
        let (responder, responder_rx) = oneshot::channel();
        self.manager_tx
            .send(ManagerEvent::AddService {
                service: service.clone(),
                responder,
            })
            .await?;
        return responder_rx.await?;
    }

    async fn update_characteristic(
        &mut self,
        characteristic: Uuid,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        let (responder, responder_rx) = oneshot::channel();
        self.manager_tx
            .send(ManagerEvent::UpdateCharacteristic {
                characteristic,
                value,
                responder,
            })
            .await?;
        return responder_rx.await?;
    }
}
