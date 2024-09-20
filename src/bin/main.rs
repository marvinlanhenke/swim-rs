use swim_rs::{error::Result, SwimNode};

use tokio::net::UdpSocket;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .with_span_events(FmtSpan::FULL)
        .with_level(true)
        .init();

    let n1 = SwimNode::try_new("0.0.0.0:8080", None).await?;
    let n2 = SwimNode::try_new("0.0.0.0:8081", Some(&["0.0.0.0:8080"])).await?;
    let n3 = SwimNode::try_new("0.0.0.0:8082", Some(&["0.0.0.0:8080"])).await?;

    n1.run().await?;
    n2.run().await?;
    let (_p3, _d3) = n3.run().await?;

    // use std::time::Duration;
    // tokio::time::sleep(Duration::from_secs(5)).await;
    // tracing::info!("[0.0.0.0:8082] shutting down");
    // p3.abort();
    // d3.abort();

    // simulate some application server
    let socket = UdpSocket::bind("0.0.0.0:9090").await?;
    let mut buf = [0u8; 1024];
    loop {
        socket.recv_from(&mut buf).await?;
    }
}
