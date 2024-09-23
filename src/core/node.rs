use std::sync::Arc;

use prost::Message;
use tokio::{net::UdpSocket, task::JoinHandle};

use super::config::SwimConfig;

use crate::{
    error::Result,
    pb::{swim_message, SwimMessage},
};

#[derive(Clone, Debug)]
pub struct SwimNode {
    addr: String,
    config: Arc<SwimConfig>,
    socket: Arc<UdpSocket>,
}

impl SwimNode {
    pub async fn try_new(
        addr: impl Into<String>,
        socket: UdpSocket,
        config: SwimConfig,
    ) -> Result<Self> {
        let addr = addr.into();
        let socket = Arc::new(socket);
        let config = Arc::new(config);

        Ok(Self {
            addr,
            config,
            socket,
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

        let socket = self.socket.clone();

        tokio::spawn(async move {
            loop {
                let mut buf = [0u8; 1536];

                match socket.recv(&mut buf).await {
                    Ok(len) => {
                        println!("Received bytes {len:?}");
                        match SwimMessage::decode(&buf[..len]) {
                            Ok(message) => match message.action {
                                Some(action) => match action {
                                    Ping(ping) => println!("Received Ping: {ping:?}"),
                                    PingReq(ping_req) => println!("Received PingReq: {ping_req:?}"),
                                    Ack(ack) => println!("Received Ack: {ack:?}"),
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
