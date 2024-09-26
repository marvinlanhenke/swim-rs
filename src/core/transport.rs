use crate::error::Result;
use async_trait::async_trait;
use tokio::net::{ToSocketAddrs, UdpSocket};

#[async_trait]
pub(crate) trait TransportLayer {
    async fn recv(&self, buf: &mut [u8]) -> Result<usize>;

    async fn send_to<A>(&self, buf: &[u8], target: A) -> Result<usize>
    where
        A: ToSocketAddrs + Send;

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
