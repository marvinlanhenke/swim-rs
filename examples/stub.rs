use std::time::Duration;

use swim_rs::{config::SwimConfig, core::node::SwimNode, error::Result};
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "0.0.0.0:8080";
    let socket = UdpSocket::bind(addr).await?;
    let node = SwimNode::try_new(addr, socket, SwimConfig::new()).await?;

    node.run().await;

    tokio::time::sleep(Duration::from_secs(100)).await;

    Ok(())
}
