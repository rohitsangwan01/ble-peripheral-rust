use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug)]
pub enum PeripheralEvent {
    StateUpdate {
        is_powered: bool,
    },
    CharacteristicSubscriptionUpdate {
        request: PeripheralRequest,
        subscribed: bool,
    },
    ReadRequest {
        request: PeripheralRequest,
        offset: u64,
        responder: oneshot::Sender<ReadRequestResponse>,
    },
    WriteRequest {
        request: PeripheralRequest,
        value: Vec<u8>,
        offset: u64,
        responder: oneshot::Sender<WriteRequestResponse>,
    },
}

#[derive(Debug, Clone)]
pub struct PeripheralRequest {
    pub client: String,
    pub service: Uuid,
    pub characteristic: Uuid,
    pub mtu: Option<usize>,
}

#[derive(Debug)]
pub struct ReadRequestResponse {
    pub value: Vec<u8>,
    pub response: RequestResponse,
}

#[derive(Debug)]
pub struct WriteRequestResponse {
    pub response: RequestResponse,
}

#[derive(Debug, PartialEq)]
pub enum RequestResponse {
    Success,
    InvalidHandle,
    RequestNotSupported,
    InvalidOffset,
    UnlikelyError,
}
