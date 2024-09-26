use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use tokio::net::ToSocketAddrs;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::{core::transport::TransportLayer, pb::SwimMessage};

#[derive(Clone, Debug, Default)]
pub(crate) struct MockUdpSocket {
    transmitted: Arc<Mutex<Vec<SwimMessage>>>,
    received: Arc<Mutex<Vec<SwimMessage>>>,
}

impl MockUdpSocket {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add_transmitted(&self, message: SwimMessage) {
        let mut tx = self.transmitted.lock().await;
        tx.push(message);
    }

    pub async fn add_received(&self, message: SwimMessage) {
        let mut rx = self.received.lock().await;
        rx.push(message);
    }
}

#[async_trait]
impl TransportLayer for MockUdpSocket {
    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        let message = SwimMessage::decode(&*buf)?;
        self.add_received(message).await;

        Ok(buf.len())
    }

    async fn send_to<A>(&self, buf: &[u8], _target: A) -> Result<usize>
    where
        A: ToSocketAddrs + Send,
    {
        let message = SwimMessage::decode(buf)?;
        self.add_transmitted(message).await;

        Ok(buf.len())
    }

    fn local_addr(&self) -> Result<String> {
        Ok("MockUdpSocket".to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        core::transport::TransportLayer,
        pb::swim_message::{Action, Ping},
        utils::MockUdpSocket,
    };

    #[tokio::test]
    async fn test_mock_udp_socket_send_to() {
        let socket = MockUdpSocket::new();

        let message = Action::Ping(Ping {
            from: "localhost".to_string(),
            requested_by: "".to_string(),
            gossip: None,
        });

        let mut buf = vec![];
        message.encode(&mut buf);

        socket.send_to(&buf, "test_socket").await.unwrap();
        println!("{socket:?}");
    }

    #[tokio::test]
    async fn test_mock_udp_socket_received() {
        let socket = MockUdpSocket::new();

        let message = Action::Ping(Ping {
            from: "localhost".to_string(),
            requested_by: "".to_string(),
            gossip: None,
        });

        let mut buf = vec![];
        message.encode(&mut buf);

        socket.recv(&mut buf).await.unwrap();
        println!("{socket:?}");
    }
}
