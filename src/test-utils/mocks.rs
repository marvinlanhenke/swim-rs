//! # Mock UDP Socket Module
//!
//! This module provides a mock implementation of the `TransportLayer` trait for testing purposes.
//! The `MockUdpSocket` struct simulates a UDP socket by storing transmitted and received messages
//! in memory, allowing tests to verify the behavior of components that rely on network communication.
use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use tokio::net::ToSocketAddrs;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::{core::transport::TransportLayer, pb::SwimMessage};

/// A mock implementation of a UDP socket for testing purposes.
///
/// The `MockUdpSocket` struct simulates sending and receiving UDP messages by storing them in
/// memory. It implements the `TransportLayer` trait, allowing it to be used in place of a real
/// UDP socket during testing.
#[derive(Clone, Debug, Default)]
pub(crate) struct MockUdpSocket {
    /// A list of transmitted messages stored in an `Arc<Mutex<Vec<SwimMessage>>>`.
    transmitted: Arc<Mutex<Vec<SwimMessage>>>,
    /// A list of received messages stored in an `Arc<Mutex<Vec<SwimMessage>>>`.
    received: Arc<Mutex<Vec<SwimMessage>>>,
}

impl MockUdpSocket {
    /// Creates a new `MockUdpSocket` instance.
    ///
    /// # Returns
    ///
    /// A new `MockUdpSocket` with empty transmitted and received message lists.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Retrieves the list of transmitted messages.
    pub(crate) async fn transmitted(&self) -> Vec<SwimMessage> {
        let tx = self.transmitted.lock().await;
        (*tx).clone()
    }

    /// Adds a message to the list of transmitted messages.
    pub(crate) async fn add_transmitted(&self, message: SwimMessage) {
        let mut tx = self.transmitted.lock().await;
        tx.push(message);
    }

    /// Retrieves the list of received messages.
    pub(crate) async fn received(&self) -> Vec<SwimMessage> {
        let rx = self.received.lock().await;
        (*rx).clone()
    }

    /// Adds a message to the list of received messages.
    pub(crate) async fn add_received(&self, message: SwimMessage) {
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
        pb::{swim_message::Action, SwimMessage},
        test_utils::mocks::MockUdpSocket,
    };

    #[tokio::test]
    async fn test_mock_udp_socket_send_to() {
        let socket = MockUdpSocket::new();
        let action = Action::new_ping("localhost", "", vec![]);
        let mut buf = vec![];
        action.encode(&mut buf);

        socket.send_to(&buf, "test_socket").await.unwrap();

        let message = SwimMessage {
            action: Some(action),
        };

        assert_eq!(&message, &socket.transmitted().await[0]);
    }

    #[tokio::test]
    async fn test_mock_udp_socket_received() {
        let socket = MockUdpSocket::new();
        let action = Action::new_ping("localhost", "", vec![]);
        let mut buf = vec![];
        action.encode(&mut buf);

        socket.recv(&mut buf).await.unwrap();

        let message = SwimMessage {
            action: Some(action),
        };

        assert_eq!(&message, &socket.received().await[0]);
    }
}
