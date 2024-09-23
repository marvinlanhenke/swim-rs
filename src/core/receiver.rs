use std::sync::Arc;

use crate::{
    error::Result,
    pb::swim_message::{Ack, Ping, PingReq},
};

use tokio::net::UdpSocket;

#[derive(Clone, Debug)]
pub(crate) struct MessageReceiver {
    socket: Arc<UdpSocket>,
}

impl MessageReceiver {
    pub(crate) fn new(socket: Arc<UdpSocket>) -> Self {
        Self { socket }
    }

    pub(crate) async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        Ok(self.socket.recv(buf).await?)
    }

    pub(crate) async fn handle_ping(&self, action: &Ping) {
        println!("handle {action:?}");
    }

    pub(crate) async fn handle_ping_req(&self, action: &PingReq) {
        println!("handle {action:?}");
    }

    pub(crate) async fn handle_ack(&self, action: &Ack) {
        println!("handle {action:?}");
    }
}
