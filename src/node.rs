use std::sync::Arc;

use prost::Message;
use tokio::{net::UdpSocket, task::JoinHandle};

use super::config::SwimConfig;

use crate::{
    core::config::DEFAULT_BUFFER_SIZE,
    error::Result,
    pb::{
        swim_message::{self, Ack, Ping, PingReq},
        SwimMessage,
    },
};

#[derive(Clone, Debug)]
struct MessageReceiver {
    socket: Arc<UdpSocket>,
}

impl MessageReceiver {
    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        Ok(self.socket.recv(buf).await?)
    }

    async fn handle_ping(&self, action: &Ping) {
        println!("handle {action:?}");
    }

    async fn handle_ping_req(&self, action: &PingReq) {
        println!("handle {action:?}");
    }

    async fn handle_ack(&self, action: &Ack) {
        println!("handle {action:?}");
    }
}

#[derive(Clone, Debug)]
pub struct SwimNode {
    addr: String,
    config: SwimConfig,
    receiver: MessageReceiver,
}

impl SwimNode {
    pub async fn try_new(
        addr: impl Into<String>,
        socket: UdpSocket,
        config: SwimConfig,
    ) -> Result<Self> {
        let addr = addr.into();
        let receiver = MessageReceiver {
            socket: Arc::new(socket),
        };

        Ok(Self {
            addr,
            config,
            receiver,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn config(&self) -> &SwimConfig {
        &self.config
    }

    pub async fn run(&self) -> JoinHandle<()> {
        self.dispatch().await
    }

    async fn dispatch(&self) -> JoinHandle<()> {
        use swim_message::Action::*;

        let receiver = self.receiver.clone();

        tokio::spawn(async move {
            loop {
                let mut buf = [0u8; DEFAULT_BUFFER_SIZE];

                match receiver.recv(&mut buf).await {
                    Ok(len) => {
                        match SwimMessage::decode(&buf[..len]) {
                            Ok(message) => match message.action {
                                Some(action) => match action {
                                    Ping(ping) => receiver.handle_ping(&ping).await,
                                    PingReq(ping_req) => receiver.handle_ping_req(&ping_req).await,
                                    Ack(ack) => receiver.handle_ack(&ack).await,
                                },
                                None => eprintln!("Error invalid message action"),
                            },
                            Err(e) => {
                                eprintln!("Error while decoding message {e}")
                            }
                        };
                    }
                    Err(_) => eprint!("Error while receiving message"),
                }
            }
        })
    }
}
