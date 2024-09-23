use std::sync::Arc;

use tokio::net::UdpSocket;

#[derive(Clone, Debug)]
pub(crate) struct MessageSender {
    socket: Arc<UdpSocket>,
}

impl MessageSender {
    pub(crate) fn new(socket: Arc<UdpSocket>) -> Self {
        Self { socket }
    }

    pub(crate) async fn send_ping(&self) {
        tracing::info!("Sending PING");
    }

    pub(crate) async fn send_ping_req(&self) {
        tracing::info!("Sending PING_REQ");
    }
}
