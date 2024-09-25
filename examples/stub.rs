use std::time::Duration;

use swim_rs::{config::SwimConfig, core::node::SwimNode, error::Result};
use tokio::net::UdpSocket;

async fn create_node(addr: &str, known_peers: &[&str]) -> Result<SwimNode> {
    let socket = UdpSocket::bind(addr).await?;
    SwimNode::try_new(
        socket,
        SwimConfig::builder().with_known_peers(known_peers).build(),
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let n1 = create_node("127.0.0.1:8080", &[]).await?;
    let n2 = create_node("127.0.0.1:8081", &["127.0.0.1:8080"]).await?;

    n1.run().await;
    n2.run().await;

    tokio::time::sleep(Duration::from_secs(100)).await;

    Ok(())
}
