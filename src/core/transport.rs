//! # Transport Layer Module
//!
//! This module defines the `TransportLayer` trait, which abstracts the network transport layer
//! used by the SWIM protocol implementation. It provides asynchronous methods for sending and
//! receiving messages over the network, allowing for different underlying transport mechanisms.
use crate::error::Result;
use async_trait::async_trait;
use tokio::net::{ToSocketAddrs, UdpSocket};

/// The `TransportLayer` trait abstracts the network transport layer.
#[async_trait]
pub trait TransportLayer {
    /// Receives data from the socket and writes it into the provided buffer.
    async fn recv(&self, buf: &mut [u8]) -> Result<usize>;

    /// Sends data to the specified target address.
    async fn send_to<A>(&self, buf: &[u8], target: A) -> Result<usize>
    where
        A: ToSocketAddrs + Send;

    /// Retrieves the local address that the socket is bound to.
    fn local_addr(&self) -> Result<String>;
}

#[async_trait]
impl TransportLayer for UdpSocket {
    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        Ok(self.recv(buf).await?)
    }

    async fn send_to<A>(&self, buf: &[u8], target: A) -> Result<usize>
    where
        A: ToSocketAddrs + Send,
    {
        Ok(self.send_to(buf, target).await?)
    }

    fn local_addr(&self) -> Result<String> {
        let addr = self.local_addr()?;
        Ok(addr.to_string())
    }
}
