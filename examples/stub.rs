use std::time::Duration;

use swim_rs::{config::SwimConfig, core::node::SwimNode, error::Result};
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:8080").await?;
    let node1 = SwimNode::try_new(socket, SwimConfig::new()).await?;

    let socket = UdpSocket::bind("0.0.0.0:8081").await?;
    let node2 = SwimNode::try_new(
        socket,
        SwimConfig::builder()
            .with_known_peers(&["0.0.0.0:8080"])
            .build(),
    )
    .await?;

    node1.run().await;
    node2.run().await;

    tokio::time::sleep(Duration::from_secs(100)).await;

    Ok(())
}
