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

    for i in 0..3 {
        let peers: Option<&[&str]> = if i == 0 {
            None
        } else {
            Some(&["0.0.0.0:8080"])
        };
        let addr = format!("0.0.0.0:808{i}");
        SwimNode::try_new(&addr, peers).await?.run().await?;
    }

    // simulate some application server
    let socket = UdpSocket::bind("0.0.0.0:9090").await?;
    let mut buf = [0u8; 1024];
    loop {
        socket.recv_from(&mut buf).await?;
    }
}
