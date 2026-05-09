#[cfg(any(target_os = "macos", target_os = "ios"))]
mod corebluetooth;
#[cfg(any(target_os = "macos", target_os = "ios"))]
pub use self::corebluetooth::Peripheral;

#[cfg(any(target_os = "linux", target_os = "android"))]
mod bluez;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use self::bluez::Peripheral;

#[cfg(target_os = "windows")]
mod winrt;
#[cfg(target_os = "windows")]
pub use self::winrt::Peripheral;

use crate::{
    error::Error,
    gatt::{peripheral_event::PeripheralEvent, service::Service},
};
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

#[async_trait]
pub trait PeripheralImpl: Send + Sync {
    type Peripheral: PeripheralImpl + Send + Sync;

    async fn new(sender_tx: Sender<PeripheralEvent>) -> Result<Peripheral, Error>;

    async fn is_powered(&mut self) -> Result<bool, Error>;

    async fn is_advertising(&mut self) -> Result<bool, Error>;

    async fn start_advertising(&mut self, name: &str, uuids: &[Uuid], manufacturer_data: Option<(u16, Vec<u8>)>) -> Result<(), Error>;

    async fn stop_advertising(&mut self) -> Result<(), Error>;

    async fn add_service(&mut self, service: &Service) -> Result<(), Error>;

    async fn update_characteristic(
        &mut self,
        characteristic: Uuid,
        value: Vec<u8>,
    ) -> Result<(), Error>;
}
