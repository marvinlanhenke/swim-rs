use std::sync::Arc;

use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;

use crate::api::config::SwimConfig;
use crate::core::utils::send_action;
use crate::error::Result;
use crate::pb::gossip::{NodeDeceased, NodeSuspected};
use crate::pb::swim_message::{Action, Ping, PingReq};
use crate::pb::{Member, NodeState};
use crate::Event;

use super::disseminate::{Disseminator, DisseminatorUpdate};
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
    SendingPingReq {
        target: String,
        incarnation: u64,
    },
    WaitingForAck {
        target: String,
        ack_type: AckType,
        incarnation: u64,
    },
    DeclaringNodeAsDead {
        target: String,
        incarnation: u64,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct FailureDetector<T: TransportLayer> {
    addr: String,
    socket: Arc<T>,
    state: Arc<RwLock<FailureDetectorState>>,
    config: Arc<SwimConfig>,
    membership_list: Arc<MembershipList>,
    disseminator: Arc<Disseminator>,
    tx: Sender<Event>,
}

impl<T: TransportLayer> FailureDetector<T> {
    pub(crate) fn new(
        addr: impl Into<String>,
        socket: Arc<T>,
        config: Arc<SwimConfig>,
        membership_list: Arc<MembershipList>,
        disseminator: Arc<Disseminator>,
        tx: Sender<Event>,
    ) -> Self {
        let addr = addr.into();
        let state = Arc::new(RwLock::new(FailureDetectorState::SendingPing));

        Self {
            addr,
            socket,
            state,
            config,
            membership_list,
            disseminator,
            tx,
        }
    }

    pub(crate) async fn state(&self) -> FailureDetectorState {
        let state = self.state.read().await;
        (*state).clone()
    }

    pub(crate) async fn send_ping(&self) -> Result<()> {
        tokio::time::sleep(self.config.ping_interval()).await;

        let gossip = self
            .disseminator
            .get_gossip(self.membership_list.len())
            .await;

        let action = Action::Ping(Ping {
            from: self.addr.clone(),
            requested_by: "".to_string(),
            gossip,
        });

        if let Some(member) = self.membership_list.get_random_member_list(1, None).first() {
            let target = member.addr.clone();
            tracing::debug!("[{}] sending PING to {}", &self.addr, &target);
            send_action(&*self.socket, &action, &target).await?;

            let updated_member = Member {
                addr: target.clone(),
                state: NodeState::Pending as i32,
                incarnation: member.incarnation,
            };
            self.membership_list.update_member(updated_member);

            let mut state = self.state.write().await;
            *state = FailureDetectorState::WaitingForAck {
                target,
                ack_type: AckType::PingAck,
                incarnation: member.incarnation,
            };
        };

        Ok(())
    }

    pub(crate) async fn send_ping_req(
        &self,
        suspect: impl Into<String>,
        incarnation: u64,
    ) -> Result<()> {
        let suspect = suspect.into();

        self.membership_list.update_member(Member {
            addr: suspect.clone(),
            state: NodeState::Suspected as i32,
            incarnation,
        });

        let event = Event::NodeSuspected(NodeSuspected {
            from: self.addr.to_string(),
            suspect: suspect.clone(),
            suspect_incarnation_no: incarnation,
        });

        self.disseminator
            .push(DisseminatorUpdate::NodesAlive(event.clone()))
            .await;

        if let Err(e) = self.tx.send(event) {
            tracing::error!("SendEventError: {}", e.to_string());
        }

        let probe_group = self
            .membership_list
            .get_random_member_list(self.config.ping_req_group_size(), Some(&suspect));

        for probe_member in &probe_group {
            let gossip = self
                .disseminator
                .get_gossip(self.membership_list.len())
                .await;
            let action = Action::PingReq(PingReq {
                from: self.addr.clone(),
                suspect: suspect.clone(),
                gossip,
            });

            tracing::debug!(
                "[{}] sending PING_REQ to {}",
                &self.addr,
                &probe_member.addr
            );
            send_action(&*self.socket, &action, &probe_member.addr).await?;
        }

        let mut state = self.state.write().await;
        *state = FailureDetectorState::WaitingForAck {
            target: suspect.clone(),
            ack_type: AckType::PingReqAck,
            incarnation,
        };

        Ok(())
    }

    pub(crate) async fn wait_for_ack(
        &self,
        target: impl Into<String>,
        ack_type: &AckType,
        incarnation: u64,
    ) -> Result<()> {
        let target = target.into();

        match ack_type {
            AckType::PingAck => {
                tracing::debug!("[{}] waiting for PING_ACK from {}", &self.addr, &target);
                tokio::time::sleep(self.config.ping_timeout()).await;

                let mut state = self.state.write().await;

                match self.membership_list.member_state(&target) {
                    Some(node_state) => match node_state? {
                        NodeState::Alive => *state = FailureDetectorState::SendingPing,
                        _ => {
                            *state = FailureDetectorState::SendingPingReq {
                                target,
                                incarnation,
                            }
                        }
                    },
                    None => *state = FailureDetectorState::SendingPing,
                };

                Ok(())
            }
            AckType::PingReqAck => {
                tracing::debug!("[{}] waiting for PING_REQ_ACK from {}", &self.addr, &target);
                tokio::time::sleep(self.config.ping_req_timeout()).await;

                let mut state = self.state.write().await;

                match self.membership_list.member_state(&target) {
                    Some(node_state) => match node_state? {
                        NodeState::Alive => *state = FailureDetectorState::SendingPing,
                        _ => {
                            *state = FailureDetectorState::DeclaringNodeAsDead {
                                target,
                                incarnation,
                            }
                        }
                    },
                    None => *state = FailureDetectorState::SendingPing,
                };

                Ok(())
            }
        }
    }

    pub(crate) async fn declare_node_as_dead(
        &self,
        target: impl Into<String>,
        incarnation: u64,
    ) -> Result<()> {
        let addr = self.addr.clone();
        let target = target.into();
        let membership_list = self.membership_list.clone();
        let suspect_timeout = self.config.suspect_timeout();
        let disseminator = self.disseminator.clone();
        let tx = self.tx.clone();

        tokio::spawn(async move {
            tokio::time::sleep(suspect_timeout).await;
            tracing::debug!("[{}] declaring NODE {} as deceased", &addr, &target);
            membership_list.members().remove(&target);

            let event = Event::NodeDeceased(NodeDeceased {
                from: addr.clone(),
                deceased: target.clone(),
                deceased_incarnation_no: incarnation,
            });

            disseminator
                .push(DisseminatorUpdate::NodesDeceased(event.clone()))
                .await;

            if let Err(e) = tx.send(event) {
                tracing::error!("SendEventError: {}", e.to_string());
            };
        });

        let mut state = self.state.write().await;
        *state = FailureDetectorState::SendingPing;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::broadcast;

    use crate::{
        api::config::{SwimConfig, DEFAULT_BUFFER_SIZE, DEFAULT_GOSSIP_MAX_MESSAGES},
        core::{
            detection::{AckType, FailureDetectorState},
            disseminate::Disseminator,
            member::MembershipList,
        },
        pb::{
            swim_message::{Action, Ping},
            Member, NodeState, SwimMessage,
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
        let membership_list = Arc::new(MembershipList::new("NODE_A", 0));
        membership_list.add_member("NODE_B", 0);

        let (tx, _) = broadcast::channel(32);
        let disseminator = Arc::new(Disseminator::new(
            DEFAULT_GOSSIP_MAX_MESSAGES,
            DEFAULT_BUFFER_SIZE,
            1,
        ));

        FailureDetector::new("NODE_A", socket, config, membership_list, disseminator, tx)
    }

    #[tokio::test]
    async fn test_detection_declare_node_as_dead() {
        let failure_detector = create_failure_detector();
        failure_detector
            .declare_node_as_dead("NODE_B", 0)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::SendingPing;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_detection_wait_for_ack_ping_req() {
        let failure_detector = create_failure_detector();
        failure_detector.membership_list.update_member(Member {
            addr: "NODE_B".to_string(),
            state: NodeState::Suspected as i32,
            incarnation: 0,
        });

        failure_detector
            .wait_for_ack("NODE_B", &AckType::PingReqAck, 0)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::DeclaringNodeAsDead {
            target: "NODE_B".to_string(),
            incarnation: 0,
        };
        assert_eq!(result, expected);

        failure_detector.membership_list.update_member(Member {
            addr: "NODE_B".to_string(),
            state: NodeState::Alive as i32,
            incarnation: 1,
        });

        failure_detector
            .wait_for_ack("NODE_B", &AckType::PingReqAck, 1)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::SendingPing;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_detection_wait_for_ack_ping() {
        let failure_detector = create_failure_detector();
        failure_detector.membership_list.update_member(Member {
            addr: "NODE_B".to_string(),
            state: NodeState::Pending as i32,
            incarnation: 0,
        });

        failure_detector
            .wait_for_ack("NODE_B", &AckType::PingAck, 0)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::SendingPingReq {
            target: "NODE_B".to_string(),
            incarnation: 0,
        };
        assert_eq!(result, expected);

        failure_detector.membership_list.update_member(Member {
            addr: "NODE_B".to_string(),
            state: NodeState::Alive as i32,
            incarnation: 1,
        });

        failure_detector
            .wait_for_ack("NODE_B", &AckType::PingAck, 1)
            .await
            .unwrap();

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::SendingPing;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_detection_send_ping_req() {
        let failure_detector = create_failure_detector();
        failure_detector.membership_list.add_member("NODE_C", 0);

        failure_detector.send_ping_req("NODE_B", 0).await.unwrap();

        let result = failure_detector
            .membership_list
            .member_state("NODE_B")
            .unwrap()
            .unwrap();
        let expected = NodeState::Suspected;
        assert_eq!(result, expected);

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::WaitingForAck {
            target: "NODE_B".to_string(),
            ack_type: AckType::PingReqAck,
            incarnation: 0,
        };
        assert_eq!(result, expected);

        let result = matches!(
            &failure_detector.socket.transmitted().await[0],
            SwimMessage {
                action: Some(Action::PingReq(_))
            }
        );
        assert!(result);
    }

    #[tokio::test]
    async fn test_detection_send_ping() {
        let failure_detector = create_failure_detector();
        let gossip = failure_detector
            .disseminator
            .get_gossip(failure_detector.membership_list.len())
            .await;

        failure_detector.send_ping().await.unwrap();

        let result = failure_detector
            .membership_list
            .member_state("NODE_B")
            .unwrap()
            .unwrap();
        let expected = NodeState::Pending;
        assert_eq!(result, expected);

        let result = failure_detector.state().await;
        let expected = FailureDetectorState::WaitingForAck {
            target: "NODE_B".to_string(),
            ack_type: AckType::PingAck,
            incarnation: 0,
        };
        assert_eq!(result, expected);

        let result = &failure_detector.socket.transmitted().await[0];
        let expected = SwimMessage {
            action: Some(Action::Ping(Ping {
                from: "NODE_A".to_string(),
                requested_by: "".to_string(),
                gossip,
            })),
        };
        assert_eq!(result, &expected);
    }
}
