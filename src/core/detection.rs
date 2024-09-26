use std::sync::Arc;

use tokio::sync::RwLock;

use crate::api::config::SwimConfig;
use crate::core::utils::send_action;
use crate::error::Result;
use crate::pb::swim_message::{Action, Ping, PingReq};
use crate::pb::NodeState;

use super::member::MembershipList;
use super::transport::TransportLayer;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum AckType {
    PingAck,
    PingReqAck,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FailureDetectorState {
    SendingPing,
    SendingPingReq { target: String },
    WaitingForAck { target: String, ack_type: AckType },
    DeclaringNodeAsDead { target: String },
}

#[derive(Clone, Debug)]
pub(crate) struct FailureDetector<T: TransportLayer> {
    addr: String,
    socket: Arc<T>,
    state: Arc<RwLock<FailureDetectorState>>,
    config: Arc<SwimConfig>,
    membership_list: Arc<MembershipList>,
}

impl<T: TransportLayer> FailureDetector<T> {
    pub(crate) fn new(
        addr: impl Into<String>,
        socket: Arc<T>,
        config: Arc<SwimConfig>,
        membership_list: Arc<MembershipList>,
    ) -> Self {
        let addr = addr.into();
        let state = Arc::new(RwLock::new(FailureDetectorState::SendingPing));

        Self {
            addr,
            socket,
            state,
            config,
            membership_list,
        }
    }

    pub(crate) async fn state(&self) -> FailureDetectorState {
        let state = self.state.read().await;
        (*state).clone()
    }

    pub(crate) async fn send_ping(&self) -> Result<()> {
        tokio::time::sleep(self.config.ping_interval()).await;

        let action = Action::Ping(Ping {
            from: self.addr.clone(),
            requested_by: "".to_string(),
            gossip: None,
        });

        if let Some((target, _)) = self.membership_list.get_random_member_list(1, None).first() {
            tracing::debug!("[{}] sending PING to {}", &self.addr, &target);
            send_action(&*self.socket, &action, &target).await?;

            self.membership_list
                .update_member(target, NodeState::Pending);

            let mut state = self.state.write().await;
            *state = FailureDetectorState::WaitingForAck {
                target: target.clone(),
                ack_type: AckType::PingAck,
            };
        };

        Ok(())
    }

    pub(crate) async fn send_ping_req(&self, suspect: impl Into<String>) -> Result<()> {
        let suspect = suspect.into();

        self.membership_list
            .update_member(&suspect, NodeState::Suspected);

        let probe_group = self
            .membership_list
            .get_random_member_list(self.config.ping_req_group_size(), Some(&suspect));

        for (target, _) in &probe_group {
            let action = Action::PingReq(PingReq {
                from: self.addr.clone(),
                suspect: suspect.clone(),
            });

            tracing::debug!("[{}] sending PING_REQ to {}", &self.addr, &target);
            send_action(&*self.socket, &action, target).await?;
        }

        let mut state = self.state.write().await;
        *state = FailureDetectorState::WaitingForAck {
            target: suspect.clone(),
            ack_type: AckType::PingReqAck,
        };

        Ok(())
    }

    pub(crate) async fn wait_for_ack(
        &self,
        target: impl Into<String>,
        ack_type: &AckType,
    ) -> Result<()> {
        let target = target.into();

        match ack_type {
            AckType::PingAck => {
                tracing::debug!("[{}] waiting for PING_ACK from {}", &self.addr, &target);
                tokio::time::sleep(self.config.ping_timeout()).await;

                let mut state = self.state.write().await;

                match self.membership_list.member_state(&target)? {
                    NodeState::Alive => *state = FailureDetectorState::SendingPing,
                    _ => *state = FailureDetectorState::SendingPingReq { target },
                };

                Ok(())
            }
            AckType::PingReqAck => {
                tracing::debug!("[{}] waiting for PING_REQ_ACK from {}", &self.addr, &target);
                tokio::time::sleep(self.config.ping_req_timeout()).await;

                let mut state = self.state.write().await;

                match self.membership_list.member_state(&target)? {
                    NodeState::Alive => *state = FailureDetectorState::SendingPing,
                    _ => *state = FailureDetectorState::DeclaringNodeAsDead { target },
                };

                Ok(())
            }
        }
    }

    pub(crate) async fn declare_node_as_dead(&self, target: impl AsRef<str>) -> Result<()> {
        let target = target.as_ref();

        tracing::debug!("[{}] declaring NODE {} as deceased", &self.addr, target);
        self.membership_list.members().remove(target);

        // TODO: update disseminator, with deceased node to update all other nodes in the cluster
        let mut state = self.state.write().await;
        *state = FailureDetectorState::SendingPing;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::{
        api::config::SwimConfig,
        core::{
            detection::{AckType, FailureDetectorState},
            member::MembershipList,
        },
        pb::{
            swim_message::{Action, Ping},
            NodeState, SwimMessage,
        },
        test_utils::mocks::MockUdpSocket,
    };

    use super::FailureDetector;

    fn create_failure_detector() -> FailureDetector<MockUdpSocket> {
        let socket = Arc::new(MockUdpSocket::new());
        let config = Arc::new(
            SwimConfig::builder()
                .with_ping_interval(Duration::from_millis(0))
                .with_ping_timeout(Duration::from_millis(0))
                .with_ping_req_timeout(Duration::from_millis(0))
                .build(),
        );
        let membership_list = Arc::new(MembershipList::new("NODE_A"));
        membership_list.add_member("NODE_B");

        FailureDetector::new("NODE_A", socket, config, membership_list)
    }

    #[tokio::test]
    async fn test_detection_send_ping() {
        let failure_detector = create_failure_detector();

        failure_detector.send_ping().await.unwrap();

        let result = failure_detector
            .membership_list
            .member_state("NODE_B")
            .unwrap();
        let expected = NodeState::Pending;
        assert_eq!(result, expected);

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::WaitingForAck {
            target: "NODE_B".to_string(),
            ack_type: AckType::PingAck,
        };
        assert_eq!(result, expected);

        let result = &failure_detector.socket.transmitted().await[0];
        let expected = SwimMessage {
            action: Some(Action::Ping(Ping {
                from: "NODE_A".to_string(),
                requested_by: "".to_string(),
                gossip: None,
            })),
        };
        assert_eq!(result, &expected);
    }
}
