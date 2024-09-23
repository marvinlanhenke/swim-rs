use std::sync::Arc;

use prost::Message;
use tokio::{net::UdpSocket, task::JoinHandle};

use crate::{
    config::{SwimConfig, DEFAULT_BUFFER_SIZE},
    error::Result,
    pb::{swim_message, SwimMessage},
};

use super::receiver::MessageReceiver;

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
        let socket = Arc::new(socket);
        let receiver = MessageReceiver::new(socket);

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
