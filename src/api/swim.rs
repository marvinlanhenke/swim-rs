use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::core::node::SwimNode;
use crate::error::Result;
use crate::init_tracing;

use super::config::SwimConfig;

#[derive(Clone, Debug)]
pub struct SwimCluster {
    node: Arc<SwimNode<UdpSocket>>,
}

impl SwimCluster {
    pub async fn try_new(addr: impl AsRef<str>, config: SwimConfig) -> Result<Self> {
        let socket = UdpSocket::bind(addr.as_ref()).await?;
        let node = Arc::new(SwimNode::try_new(socket, config)?);

        Ok(Self { node })
    }

    pub fn addr(&self) -> &str {
        self.node.addr()
    }

    pub fn config(&self) -> &SwimConfig {
        self.node.config()
    }

    pub async fn run(&self) {
        init_tracing();

        tracing::info!("[{}] starting SwimNode...", self.node.addr());
        self.node.run().await;
    }
}
