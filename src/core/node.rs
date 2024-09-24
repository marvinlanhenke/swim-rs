use std::sync::Arc;

use prost::Message;
use tokio::{net::UdpSocket, task::JoinHandle};

use crate::{
    config::{SwimConfig, DEFAULT_BUFFER_SIZE},
    error::Result,
    init_tracing,
    pb::{swim_message, SwimMessage},
};

use super::{member::MembershipList, receiver::MessageReceiver, sender::MessageSender};

#[derive(Clone, Debug)]
pub struct SwimNode {
    addr: String,
    receiver: MessageReceiver,
    sender: MessageSender,
    membership_list: Arc<MembershipList>,
    config: Arc<SwimConfig>,
}

impl SwimNode {
    pub async fn try_new(socket: UdpSocket, config: SwimConfig) -> Result<Self> {
        let addr = socket.local_addr()?.to_string();
        let socket = Arc::new(socket);
        let config = Arc::new(config);
        let membership_list = Arc::new(MembershipList::new(&addr));

        let receiver = MessageReceiver::new(&addr, socket.clone());
        let sender = MessageSender::new(&addr, socket, membership_list.clone(), config.clone());

        Ok(Self {
            addr,
            config,
            receiver,
            sender,
            membership_list,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn config(&self) -> &SwimConfig {
        &self.config
    }

    pub async fn run(&self) -> (JoinHandle<()>, JoinHandle<()>) {
        init_tracing();

        (self.dispatch().await, self.healthcheck().await)
    }

    async fn healthcheck(&self) -> JoinHandle<()> {
        let sender = self.sender.clone();
        let interval = self.config.ping_interval();

        tokio::spawn(async move {
            loop {
                // within send ping we wait for ping_timeout
                let _ = sender.send_ping().await;
                // within send_ping_req we issue a ping_req is necessary and wait for timeout
                sender.send_ping_req().await;
                // if after ping_timeout and ping_req_timeout we still have suspects, we mark them
                // as deceased; then we wait for next interval
                tokio::time::sleep(interval).await;
            }
        })
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
                                    Ping(ping) => {
                                        let _ = receiver.handle_ping(&ping).await;
                                    }
                                    PingReq(ping_req) => receiver.handle_ping_req(&ping_req).await,
                                    Ack(ack) => receiver.handle_ack(&ack).await,
                                },
                                None => tracing::error!(
                                    "InvalidMessageError. You must provide a valid 'action'"
                                ),
                            },
                            Err(e) => {
                                tracing::error!("DecodeError: {}", e.to_string());
                            }
                        };
                    }
                    Err(e) => tracing::error!("ReceiveMessageError: {}", e.to_string()),
                }
            }
        })
    }
}
