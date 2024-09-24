use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::core::utils::send_action;
use crate::error::Result;
use crate::pb::swim_message::{Action, Ping};

#[derive(Clone, Debug)]
pub(crate) struct MessageSender {
    addr: String,
    socket: Arc<UdpSocket>,
}

impl MessageSender {
    pub(crate) fn new(addr: impl Into<String>, socket: Arc<UdpSocket>) -> Self {
        let addr = addr.into();

        Self { addr, socket }
    }

    pub(crate) async fn send_ping(&self) -> Result<usize> {
        let from = self.addr.clone();
        // TODO: get random target
        let target = from.clone();
        let requested_by = "".to_string();
        let gossip = None;

        let message = Action::Ping(Ping {
            from,
            requested_by,
            gossip,
        });

        send_action(&self.socket, &message, &target).await
    }

    pub(crate) async fn send_ping_req(&self) {
        tracing::info!("Sending PING_REQ");
    }
}
