use pb::{
    gossip::{Ack, GossipType, JoinRequest, JoinResponse, Ping, PingReq},
    Gossip, NodeState,
};
use prost::Message;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use snafu::location;
use tokio::{net::UdpSocket, sync::RwLock};

use crate::error::{Error, Result};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

pub mod error;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/swim.rs"));
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct SwimNode {
    addr: SocketAddr,
    peers: Arc<Option<Vec<SocketAddr>>>,
    members: Arc<RwLock<HashMap<String, i32>>>,
    socket: Arc<UdpSocket>,
    pending: Arc<RwLock<HashMap<String, i32>>>,
}

impl SwimNode {
    pub async fn try_new(addr: &str, peers: Option<&[&str]>) -> Result<Self> {
        let addr: SocketAddr = addr.parse()?;
        let peers = peers
            .map(|peer_list| {
                peer_list
                    .iter()
                    .map(|peer_str| {
                        peer_str.parse().map_err(|_| Error::AddrParse {
                            message: "failed to parse peer address".to_string(),
                            location: location!(),
                        })
                    })
                    .collect::<Result<Vec<SocketAddr>>>()
            })
            .transpose()?;

        let socket = UdpSocket::bind(&addr).await?;
        let members = HashMap::from_iter([(addr.to_string(), NodeState::Alive as i32)]);

        Ok(Self {
            addr,
            peers: Arc::new(peers),
            members: Arc::new(RwLock::new(members)),
            socket: Arc::new(socket),
            pending: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn members(&self) {
        tracing::info!("[{}] current members: {:?}", &self.addr, &self.members);
    }

    pub async fn run(&self) -> Result<()> {
        tracing::info!("SwimNode listening on {}", self.addr);

        // if peer fails to answer -> mark as peer as dead
        // log warning -> node could not connect to cluster -> best-practices to handle that?
        if let Some(peer_list) = self.peers.as_ref() {
            if let Some(target) = peer_list.first() {
                self.send_join_request(target).await?;
            }
        }

        self.dispatch_message().await?;
        self.send_ping().await?;

        Ok(())
    }

    async fn dispatch_message(&self) -> Result<()> {
        let this = self.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 1536];

            loop {
                match this.socket.recv_from(&mut buf).await {
                    Ok((bytes, _)) => {
                        let gossip = Gossip::decode(&buf[..bytes]);

                        match gossip {
                            Ok(msg) => match msg.gossip_type {
                                Some(GossipType::JoinRequest(req)) => {
                                    let _ = Self::handle_join_request(&this, &req.from).await;
                                }
                                Some(GossipType::JoinResponse(req)) => {
                                    let _ = Self::handle_join_response(&this, &req).await;
                                }
                                Some(GossipType::Ping(req)) => {
                                    let _ = Self::handle_ping(&this, &req.from).await;
                                }
                                Some(GossipType::PingReq(req)) => {
                                    let _ = Self::handle_ping_req(&this, &req).await;
                                }
                                Some(GossipType::Ack(req)) => {
                                    let _ = Self::handle_ack(&this, &req.from).await;
                                }
                                _ => {}
                            },
                            Err(e) => {
                                tracing::error!("[{}] DecodeError: {}", &this.addr, e.to_string())
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[{}] DispatchError: {}", &this.addr, e.to_string())
                    }
                };
            }
        });

        Ok(())
    }

    async fn send_join_request(&self, target: &SocketAddr) -> Result<()> {
        let message = JoinRequest {
            from: self.addr.to_string(),
        };

        let gossip = GossipType::JoinRequest(message);

        let mut buf = vec![];
        gossip.encode(&mut buf);

        self.socket.send_to(&buf, target).await?;

        Ok(())
    }

    async fn handle_join_request(this: &SwimNode, from: &str) -> Result<()> {
        tracing::info!("[{}] handling JoinRequest from: [{}]", this.addr, from);

        let mut members = this.members.write().await;

        members.insert(from.to_string(), NodeState::Alive as i32);

        let gossip = GossipType::JoinResponse(JoinResponse {
            members: members.clone(),
        });

        let mut buf = vec![];
        gossip.encode(&mut buf);

        for addr in members.keys() {
            if addr == &this.addr.to_string() {
                continue;
            }
            this.socket.send_to(&buf, &addr).await?;
        }

        Ok(())
    }

    async fn handle_join_response(this: &SwimNode, response: &JoinResponse) -> Result<()> {
        tracing::info!("[{}] handling JoinResponse {:?}", this.addr, response);

        let mut members = this.members.write().await;
        *members = response.members.clone();

        Ok(())
    }

    async fn send_ping(&self) -> Result<()> {
        let this = self.clone();
        let mut rng: StdRng = SeedableRng::from_entropy();

        let gossip = GossipType::Ping(Ping {
            from: this.addr.to_string(),
        });

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(3000)).await;

                let members = this.members.read().await;

                if members.len() <= 1 {
                    continue;
                }

                let node_ids = members.keys().collect::<Vec<_>>();
                let target = loop {
                    if let Some(&addr) = node_ids.choose(&mut rng) {
                        // if not the node itself and is not already pending
                        let pending = this.pending.read().await;
                        if addr != &this.addr.to_string() && !pending.contains_key(addr) {
                            break addr;
                        }
                        continue;
                    };
                };

                let mut buf = vec![];
                gossip.encode(&mut buf);
                let _ = this.socket.send_to(&buf, target).await;

                let mut pending = this.pending.write().await;
                pending.insert(target.clone(), NodeState::Pending as i32);

                let _ = Self::send_ping_req(&this).await;
            }
        });

        Ok(())
    }

    async fn send_ping_req(this: &SwimNode) -> Result<()> {
        let this = this.clone();
        let mut rng: StdRng = SeedableRng::from_entropy();
        let probe_size = 2;

        // for each interval check if we have some 'pending'
        // send a ping_request to `probe_size` n nodes.
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1000)).await;

                let pending = this.pending.read().await;

                if pending.is_empty() {
                    tracing::info!("[{}] no pending: {:?}", &this.addr, pending);
                    break;
                }

                tracing::info!("[{}] currently pending: {:?}", &this.addr, &this.pending);

                // update node_state -> suspect
                let mut members = this.members.write().await;
                let mut requests = Vec::with_capacity(pending.len());

                for suspect in pending.keys() {
                    members.insert(suspect.clone(), NodeState::Suspect as i32);

                    let gossip = GossipType::PingReq(PingReq {
                        from: this.addr.to_string(),
                        suspect: suspect.clone(),
                    });
                    requests.push(gossip);
                }

                for _ in 0..probe_size {
                    let node_ids = members.keys().collect::<Vec<_>>();
                    let target = loop {
                        if let Some(&addr) = node_ids.choose(&mut rng) {
                            // if not the node itself and is not already pending
                            let pending = this.pending.read().await;
                            if addr != &this.addr.to_string() && !pending.contains_key(addr) {
                                break addr;
                            }
                            continue;
                        };
                    };

                    for request in &requests {
                        let mut buf = vec![];
                        request.encode(&mut buf);
                        let _ = this.socket.send_to(&buf, target).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_ping(this: &SwimNode, from: &str) -> Result<()> {
        tracing::info!("[{}] handling Ping from [{}]", this.addr, from);

        let gossip = GossipType::Ack(Ack {
            from: this.addr.to_string(),
        });

        let mut buf = vec![];
        gossip.encode(&mut buf);

        this.socket.send_to(&buf, from).await?;

        Ok(())
    }

    async fn handle_ping_req(this: &SwimNode, req: &PingReq) -> Result<()> {
        tracing::info!("[{}] handling PingReq for [{}]", this.addr, req.suspect);
        Ok(())
    }

    async fn handle_ack(this: &SwimNode, from: &str) -> Result<()> {
        tracing::info!("[{}] handling Ack from [{}]", this.addr, from);

        let mut pending = this.pending.write().await;
        pending.remove(from);

        Ok(())
    }
}
