use std::sync::Arc;

use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    api::config::{SwimConfig, DEFAULT_BUFFER_SIZE},
    error::Result,
    pb::{gossip::Event, Member},
    NodeState,
};

use super::{
    detection::{FailureDetector, FailureDetectorState},
    disseminate::Disseminator,
    member::MembershipList,
    message::MessageHandler,
    transport::TransportLayer,
};

macro_rules! await_and_log_error {
    ($expr:expr, $name:expr) => {{
        if let Err(e) = $expr.await {
            tracing::error!("{}: {}", $name, e.to_string());
        }
    }};
}

#[derive(Clone, Debug)]
pub(crate) struct SwimNode<T: TransportLayer> {
    addr: String,
    config: Arc<SwimConfig>,
    failure_detector: Arc<FailureDetector<T>>,
    message_handler: Arc<MessageHandler<T>>,
    membership_list: Arc<MembershipList>,
    tx: Sender<Event>,
}

impl<T: TransportLayer + Send + Sync + 'static> SwimNode<T> {
    pub(crate) async fn try_new(socket: T, config: SwimConfig, tx: Sender<Event>) -> Result<Self> {
        let addr = socket.local_addr()?;
        let membership_list = MembershipList::new(&addr, 0);
        let iter = config
            .known_peers()
            .iter()
            .map(|addr| Member::new(addr, NodeState::Alive, 0));
        membership_list.update_from_iter(iter).await;

        Self::try_new_with_membership_list(socket, config, membership_list, tx)
    }

    pub(crate) fn try_new_with_membership_list(
        socket: T,
        config: SwimConfig,
        membership_list: MembershipList,
        tx: Sender<Event>,
    ) -> Result<Self> {
        let addr = socket.local_addr()?;
        let config = Arc::new(config);
        let socket = Arc::new(socket);
        let membership_list = Arc::new(membership_list);

        let disseminator = Arc::new(Disseminator::new(
            config.gossip_max_messages(),
            DEFAULT_BUFFER_SIZE,
            config.gossip_send_constant(),
        ));
        let failure_detector = Arc::new(FailureDetector::new(
            &addr,
            socket.clone(),
            config.clone(),
            membership_list.clone(),
            disseminator.clone(),
            tx.clone(),
        ));
        let message_handler = Arc::new(MessageHandler::new(
            &addr,
            socket.clone(),
            membership_list.clone(),
            disseminator.clone(),
            tx.clone(),
        ));

        Ok(Self {
            addr,
            config,
            failure_detector,
            message_handler,
            membership_list,
            tx,
        })
    }

    pub(crate) fn addr(&self) -> &str {
        &self.addr
    }

    pub(crate) fn config(&self) -> &SwimConfig {
        &self.config
    }

    pub(crate) fn membership_list(&self) -> &MembershipList {
        &self.membership_list
    }

    pub(crate) fn subscribe(&self) -> Receiver<Event> {
        self.tx.subscribe()
    }

    pub(crate) async fn run(&self) -> (JoinHandle<()>, JoinHandle<()>) {
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
                        await_and_log_error!(failure_detector.send_ping(), "SendPingError");
                    }
                    FailureDetectorState::SendingPingReq {
                        target,
                        incarnation,
                    } => {
                        await_and_log_error!(
                            failure_detector.send_ping_req(&target, incarnation),
                            "SendPingReqError"
                        );
                    }
                    FailureDetectorState::WaitingForAck {
                        target,
                        ack_type,
                        incarnation,
                    } => {
                        await_and_log_error!(
                            failure_detector.wait_for_ack(&target, &ack_type, incarnation),
                            "WaitingForAckError"
                        );
                    }
                    FailureDetectorState::DeclaringNodeAsDead {
                        target,
                        incarnation,
                    } => {
                        await_and_log_error!(
                            failure_detector.declare_node_as_dead(&target, incarnation),
                            "DeclareNodeAsDeadError"
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
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast;

    use crate::{api::config::SwimConfig, core::node::SwimNode, test_utils::mocks::MockUdpSocket};

    #[tokio::test]
    async fn test_node_init_with_known_peers() {
        let socket = MockUdpSocket::new();
        let config = SwimConfig::builder().with_known_peers(&["NODE_B"]).build();
        let (tx, _) = broadcast::channel(32);

        let node = SwimNode::try_new(socket, config, tx).await.unwrap();
        assert_eq!(node.membership_list().len(), 2);
    }
}
