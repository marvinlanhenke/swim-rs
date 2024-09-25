use std::sync::Arc;

use tokio::{net::UdpSocket, task::JoinHandle};

use crate::{config::SwimConfig, error::Result, init_tracing};

use super::{
    member::MembershipList,
    message::{MessageHandler, MessageHandlerState},
};

#[derive(Clone, Debug)]
pub struct SwimNode {
    addr: String,
    config: Arc<SwimConfig>,
    message_handler: Arc<MessageHandler>,
    membership_list: Arc<MembershipList>,
}

impl SwimNode {
    pub async fn try_new(
        socket: UdpSocket,
        membership_list: MembershipList,
        config: SwimConfig,
    ) -> Result<Self> {
        let addr = socket.local_addr()?.to_string();
        let config = Arc::new(config);
        let socket = Arc::new(socket);
        let membership_list = Arc::new(membership_list);

        let message_handler = Arc::new(MessageHandler::new(
            &addr,
            socket.clone(),
            config.clone(),
            membership_list.clone(),
        ));

        Ok(Self {
            addr,
            config,
            message_handler,
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

        self.dispatch_join_request().await;
        let dispatch_handle = self.dispatch().await;
        let healthcheck_handle = self.healthcheck().await;

        (dispatch_handle, healthcheck_handle)
    }

    async fn healthcheck(&self) -> JoinHandle<()> {
        let message_handler = self.message_handler.clone();

        tokio::spawn(async move {
            loop {
                let state = message_handler.state().await;
                match state {
                    MessageHandlerState::SendingPing => {
                        tracing::info!("sending ping");
                        if let Err(e) = message_handler.send_ping().await {
                            tracing::error!("HealthcheckError: {}", e.to_string());
                        }
                    }
                    MessageHandlerState::SendingPingReq { target } => {
                        tracing::info!("sending ping_req");
                        if let Err(e) = message_handler.send_ping_req(&target).await {
                            tracing::error!("HealthcheckError: {}", e.to_string());
                        }
                    }
                    MessageHandlerState::WaitingForAck { target, ack_type } => {
                        tracing::info!("sending waiting for ack");
                        if let Err(e) = message_handler.wait_for_ack(target, &ack_type).await {
                            tracing::error!("HealthcheckError: {}", e.to_string());
                        }
                    }
                    MessageHandlerState::DeclaringNodeAsDead { target } => {
                        tracing::info!("declaring node as dead");
                        if let Err(e) = message_handler.declare_node_as_dead(&target).await {
                            tracing::error!("HealthcheckError: {}", e.to_string());
                        }
                    }
                }
            }
        })
    }

    async fn dispatch(&self) -> JoinHandle<()> {
        let message_handler = self.message_handler.clone();

        tokio::spawn(async move {
            if let Err(e) = message_handler.dispatch_action().await {
                tracing::error!("DispatchError: {}", e.to_string());
            }
        })
    }

    async fn dispatch_join_request(&self) {
        if self.membership_list.members().len() == 1 && !self.config.known_peers().is_empty() {
            if let Some(target) = self.config().known_peers().first() {
                if let Err(e) = self.message_handler.send_join_req(target).await {
                    tracing::error!("JoinRequestError: {}", e.to_string());
                }
            }
        }
    }
}
