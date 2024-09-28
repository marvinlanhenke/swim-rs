use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::broadcast::{self, Receiver};

use crate::core::node::SwimNode;
use crate::error::Result;
use crate::pb::gossip::Event;
use crate::{init_tracing, MembershipList};

use super::config::SwimConfig;

#[derive(Clone, Debug)]
pub struct SwimCluster {
    node: Arc<SwimNode<UdpSocket>>,
}

impl SwimCluster {
    pub async fn try_new(addr: impl AsRef<str>, config: SwimConfig) -> Result<Self> {
        let socket = UdpSocket::bind(addr.as_ref()).await?;
        let (tx, _) = broadcast::channel::<Event>(32);
        let node = Arc::new(SwimNode::try_new(socket, config, tx)?);

        Ok(Self { node })
    }

    pub fn addr(&self) -> &str {
        self.node.addr()
    }

    pub fn config(&self) -> &SwimConfig {
        self.node.config()
    }

    pub fn subscribe(&self) -> Receiver<Event> {
        self.node.subscribe()
    }

    pub fn membership_list(&self) -> &MembershipList {
        self.node.membership_list()
    }

    pub async fn run(&self) {
        init_tracing();

        tracing::info!("[{}] starting SwimNode...", self.node.addr());
        self.node.run().await;
    }
}
