use std::sync::Arc;

use tokio::{net::UdpSocket, task::JoinHandle};

use crate::{config::SwimConfig, error::Result, init_tracing};

use super::{
    detection::{FailureDetector, FailureDetectorState},
    member::MembershipList,
    message::MessageHandler,
};

macro_rules! await_and_log_error {
    ($expr:expr, $name:expr) => {{
        if let Err(e) = $expr.await {
            tracing::error!("$name: {}", e.to_string());
        }
    }};
}

#[derive(Clone, Debug)]
pub struct SwimNode {
    addr: String,
    config: Arc<SwimConfig>,
    failure_detector: Arc<FailureDetector>,
    message_handler: Arc<MessageHandler>,
    membership_list: Arc<MembershipList>,
}

impl SwimNode {
    pub fn try_new(socket: UdpSocket, config: SwimConfig) -> Result<Self> {
        let addr = socket.local_addr()?.to_string();
        let config = Arc::new(config);
        let socket = Arc::new(socket);
        let membership_list = Arc::new(MembershipList::new(&addr));

        let failure_detector = Arc::new(FailureDetector::new(
            &addr,
            socket.clone(),
            config.clone(),
            membership_list.clone(),
        ));
        let message_handler = Arc::new(MessageHandler::new(
            &addr,
            socket.clone(),
            membership_list.clone(),
        ));

        Self::try_new_impl(
            addr,
            config,
            failure_detector,
            message_handler,
            membership_list,
        )
    }

    pub(crate) fn try_new_impl(
        addr: String,
        config: Arc<SwimConfig>,
        failure_detector: Arc<FailureDetector>,
        message_handler: Arc<MessageHandler>,
        membership_list: Arc<MembershipList>,
    ) -> Result<Self> {
        Ok(Self {
            addr,
            config,
            failure_detector,
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
        let detection_handle = self.failure_detection().await;

        (dispatch_handle, detection_handle)
    }

    async fn failure_detection(&self) -> JoinHandle<()> {
        let failure_detector = self.failure_detector.clone();
        let membership_list = self.membership_list.clone();

        tokio::spawn(async move {
            loop {
                membership_list.wait_for_members().await;

                let state = failure_detector.state().await;
                match state {
                    FailureDetectorState::SendingPing => {
                        await_and_log_error!(failure_detector.send_ping(), "FailureDetectionError");
                    }
                    FailureDetectorState::SendingPingReq { target } => {
                        await_and_log_error!(
                            failure_detector.send_ping_req(&target),
                            "FailureDetectionError"
                        );
                    }
                    FailureDetectorState::WaitingForAck { target, ack_type } => {
                        await_and_log_error!(
                            failure_detector.wait_for_ack(&target, &ack_type),
                            "FailureDetectionError"
                        );
                    }
                    FailureDetectorState::DeclaringNodeAsDead { target } => {
                        await_and_log_error!(
                            failure_detector.declare_node_as_dead(&target),
                            "FailureDetectionError"
                        );
                    }
                }
            }
        })
    }

    async fn dispatch(&self) -> JoinHandle<()> {
        let message_handler = self.message_handler.clone();

        tokio::spawn(async move {
            loop {
                await_and_log_error!(message_handler.dispatch_action(), "DispatchError");
            }
        })
    }

    async fn dispatch_join_request(&self) {
        if self.membership_list.members().len() == 1 && !self.config.known_peers().is_empty() {
            if let Some(target) = self.config().known_peers().first() {
                await_and_log_error!(
                    self.message_handler.send_join_req(target),
                    "DispatchJoinRequestError"
                )
            }
        }
    }
}
