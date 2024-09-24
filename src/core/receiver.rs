use std::sync::Arc;

use crate::{
    core::utils::send_action,
    error::Result,
    pb::swim_message::{Ack, Action, Ping, PingReq},
};

use tokio::net::UdpSocket;

#[derive(Clone, Debug)]
pub(crate) struct MessageReceiver {
    addr: String,
    socket: Arc<UdpSocket>,
}

impl MessageReceiver {
    pub(crate) fn new(addr: impl Into<String>, socket: Arc<UdpSocket>) -> Self {
        let addr = addr.into();
        Self { addr, socket }
    }

    pub(crate) async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        Ok(self.socket.recv(buf).await?)
    }

    pub(crate) async fn handle_ping(&self, action: &Ping) -> Result<usize> {
        tracing::info!("[{}] handling {action:?}", &self.addr);

        let from = self.addr.clone();
        let forward_to = action.requested_by.clone();
        let gossip = None;

        let message = Action::Ack(Ack {
            from,
            forward_to,
            gossip,
        });

        // We check if the PING was `requested_by` PING_REQ,
        // if it was we send the ACK to the original issuer.
        let target = match action.requested_by.is_empty() {
            true => &action.from,
            false => &action.requested_by,
        };

        send_action(&self.socket, &message, target).await
    }

    pub(crate) async fn handle_ping_req(&self, action: &PingReq) {
        tracing::info!("[{}] handling {action:?}", &self.addr);
    }

    pub(crate) async fn handle_ack(&self, action: &Ack) {
        tracing::info!("[{}] handling {action:?}", &self.addr);
    }
}
