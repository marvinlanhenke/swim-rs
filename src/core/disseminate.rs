use std::{
    collections::{binary_heap::PeekMut, BinaryHeap, HashSet},
    sync::Arc,
};

use prost::Message;
use tokio::sync::{RwLock, RwLockWriteGuard};
use uuid::Uuid;

use crate::{pb::Gossip, Event};

#[derive(Debug)]
struct GossipHeapEntry {
    id: Uuid,
    gossip: Gossip,
    num_send: usize,
    size: usize,
}

impl GossipHeapEntry {
    fn new(gossip: Gossip) -> Self {
        let id = Uuid::new_v4();
        let size = gossip.encoded_len();

        Self {
            id,
            gossip,
            num_send: 0,
            size,
        }
    }
}

impl PartialEq for GossipHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.num_send == other.num_send
    }
}

impl Eq for GossipHeapEntry {}

impl Ord for GossipHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.num_send.cmp(&self.num_send)
    }
}

impl PartialOrd for GossipHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub(crate) enum DisseminatorUpdate {
    NodesAlive(Event),
    NodesDeceased(Event),
}

#[derive(Clone, Debug)]
pub(crate) struct Disseminator {
    nodes_alive: Arc<RwLock<BinaryHeap<GossipHeapEntry>>>,
    nodes_deceased: Arc<RwLock<BinaryHeap<GossipHeapEntry>>>,
    max_selected: usize,
    max_size: usize,
    max_send_constant: usize,
}

impl Disseminator {
    pub(crate) fn new(max_selected: usize, max_size: usize, max_send_constant: usize) -> Self {
        Self {
            nodes_alive: Arc::new(RwLock::new(BinaryHeap::new())),
            nodes_deceased: Arc::new(RwLock::new(BinaryHeap::new())),
            max_selected,
            max_size,
            max_send_constant,
        }
    }

    pub(crate) async fn push(&self, update: DisseminatorUpdate) {
        match update {
            DisseminatorUpdate::NodesAlive(event) => {
                let mut nodes_alive = self.nodes_alive.write().await;
                let item = GossipHeapEntry::new(Gossip { event: Some(event) });
                nodes_alive.push(item);
            }
            DisseminatorUpdate::NodesDeceased(event) => {
                let mut nodes_deceased = self.nodes_deceased.write().await;
                let item = GossipHeapEntry::new(Gossip { event: Some(event) });
                nodes_deceased.push(item);
            }
        }
    }

    pub(crate) async fn get_gossip(&self, num_members: usize) -> Vec<Gossip> {
        let max_send =
            (((num_members as f64).log10() + 1f64) * self.max_send_constant as f64).ceil() as usize;
        let max_selected_alive = self.max_selected / 2 + self.max_selected % 2;
        let max_selected_deceased = self.max_selected - max_selected_alive;

        let mut gossip = Vec::new();

        let mut nodes_alive = self.nodes_alive.write().await;
        let mut nodes_deceased = self.nodes_deceased.write().await;

        let mut current_size = 0;
        let mut nodes_alive_selected = 0;
        let mut nodes_deceased_selected = 0;
        let mut seen = HashSet::new();

        loop {
            if (nodes_alive.is_empty() && nodes_deceased.is_empty())
                || (nodes_alive_selected >= max_selected_alive
                    && nodes_deceased_selected >= max_selected_deceased)
            {
                break;
            }

            let made_progress_alive = Self::process_heap_entry(
                &mut nodes_alive,
                &mut gossip,
                &mut seen,
                &mut nodes_alive_selected,
                &mut current_size,
                self.max_size,
                max_send,
            );

            let made_progress_deceased = Self::process_heap_entry(
                &mut nodes_deceased,
                &mut gossip,
                &mut seen,
                &mut nodes_deceased_selected,
                &mut current_size,
                self.max_size,
                max_send,
            );

            if !made_progress_alive && !made_progress_deceased {
                break;
            }
        }

        gossip
    }

    fn process_heap_entry(
        heap: &mut RwLockWriteGuard<BinaryHeap<GossipHeapEntry>>,
        gossip: &mut Vec<Gossip>,
        seen: &mut HashSet<Uuid>,
        num_selected: &mut usize,
        current_size: &mut usize,
        max_size: usize,
        max_send: usize,
    ) -> bool {
        match heap.peek_mut() {
            Some(mut entry) => {
                if *current_size + entry.size > max_size {
                    return false;
                }

                if seen.contains(&entry.id) {
                    return false;
                }

                gossip.push(entry.gossip.clone());

                entry.num_send += 1;
                *current_size += entry.size;
                *num_selected += 1;

                if entry.num_send >= max_send {
                    PeekMut::pop(entry);
                } else {
                    drop(entry);
                }

                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        pb::{
            gossip::{NodeDeceased, NodeJoined},
            Gossip,
        },
        Event,
    };

    use super::{Disseminator, DisseminatorUpdate};

    #[tokio::test]
    async fn test_disseminator_pop_both_no_space_left() {
        let disseminator = Disseminator::new(6, 32, 1);
        let event1 = Event::NodeJoined(NodeJoined {
            from: "NODE_A".to_string(),
            new_member: "NODE_B".to_string(),
        });
        let event2 = Event::NodeJoined(NodeJoined {
            from: "NODE_C".to_string(),
            new_member: "NODE_D".to_string(),
        });
        let event3 = Event::NodeDeceased(NodeDeceased {
            from: "NODE_A".to_string(),
            deceased: "NODE_B".to_string(),
            deceased_incarnation_no: 0,
        });
        let event4 = Event::NodeDeceased(NodeDeceased {
            from: "NODE_C".to_string(),
            deceased: "NODE_D".to_string(),
            deceased_incarnation_no: 0,
        });
        let update1 = DisseminatorUpdate::NodesAlive(event1.clone());
        let update2 = DisseminatorUpdate::NodesAlive(event2.clone());
        let update3 = DisseminatorUpdate::NodesDeceased(event3.clone());
        let update4 = DisseminatorUpdate::NodesDeceased(event4.clone());
        disseminator.push(update1).await;
        disseminator.push(update2).await;
        disseminator.push(update3).await;
        disseminator.push(update4).await;

        let result = disseminator.get_gossip(0).await;

        assert_eq!(
            result,
            vec![Gossip {
                event: Some(event1)
            },],
        );
        assert_eq!(disseminator.nodes_alive.read().await.len(), 1);
        assert_eq!(disseminator.nodes_deceased.read().await.len(), 2);
    }

    #[tokio::test]
    async fn test_disseminator_pop_both_same_len() {
        let disseminator = Disseminator::new(6, 128, 6);
        let event1 = Event::NodeJoined(NodeJoined {
            from: "NODE_A".to_string(),
            new_member: "NODE_B".to_string(),
        });
        let event2 = Event::NodeJoined(NodeJoined {
            from: "NODE_C".to_string(),
            new_member: "NODE_D".to_string(),
        });
        let event3 = Event::NodeDeceased(NodeDeceased {
            from: "NODE_A".to_string(),
            deceased: "NODE_B".to_string(),
            deceased_incarnation_no: 0,
        });
        let event4 = Event::NodeDeceased(NodeDeceased {
            from: "NODE_C".to_string(),
            deceased: "NODE_D".to_string(),
            deceased_incarnation_no: 0,
        });
        let update1 = DisseminatorUpdate::NodesAlive(event1.clone());
        let update2 = DisseminatorUpdate::NodesAlive(event2.clone());
        let update3 = DisseminatorUpdate::NodesDeceased(event3.clone());
        let update4 = DisseminatorUpdate::NodesDeceased(event4.clone());
        disseminator.push(update1).await;
        disseminator.push(update2).await;
        disseminator.push(update3).await;
        disseminator.push(update4).await;

        let result = disseminator.get_gossip(0).await;

        assert_eq!(
            result,
            vec![
                Gossip {
                    event: Some(event1)
                },
                Gossip {
                    event: Some(event3)
                },
                Gossip {
                    event: Some(event2)
                },
                Gossip {
                    event: Some(event4)
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_disseminator_pop_both_different_len() {
        let disseminator = Disseminator::new(6, 128, 6);
        let event1 = Event::NodeJoined(NodeJoined {
            from: "NODE_A".to_string(),
            new_member: "NODE_B".to_string(),
        });
        let event2 = Event::NodeJoined(NodeJoined {
            from: "NODE_C".to_string(),
            new_member: "NODE_D".to_string(),
        });
        let event3 = Event::NodeDeceased(NodeDeceased {
            from: "NODE_A".to_string(),
            deceased: "NODE_B".to_string(),
            deceased_incarnation_no: 0,
        });
        let update1 = DisseminatorUpdate::NodesAlive(event1.clone());
        let update2 = DisseminatorUpdate::NodesAlive(event2.clone());
        let update3 = DisseminatorUpdate::NodesDeceased(event3.clone());
        disseminator.push(update1).await;
        disseminator.push(update2).await;
        disseminator.push(update3).await;

        let result = disseminator.get_gossip(0).await;

        assert_eq!(
            result,
            vec![
                Gossip {
                    event: Some(event1)
                },
                Gossip {
                    event: Some(event3)
                },
                Gossip {
                    event: Some(event2)
                }
            ]
        );
    }

    #[tokio::test]
    async fn test_disseminator_pop_both() {
        let disseminator = Disseminator::new(6, 128, 0);
        let event1 = Event::NodeJoined(NodeJoined {
            from: "NODE_A".to_string(),
            new_member: "NODE_B".to_string(),
        });
        let event2 = Event::NodeDeceased(NodeDeceased {
            from: "NODE_A".to_string(),
            deceased: "NODE_B".to_string(),
            deceased_incarnation_no: 0,
        });
        let update1 = DisseminatorUpdate::NodesAlive(event1.clone());
        let update2 = DisseminatorUpdate::NodesDeceased(event2.clone());
        disseminator.push(update1).await;
        disseminator.push(update2).await;

        let result = disseminator.get_gossip(0).await;

        assert_eq!(disseminator.nodes_alive.read().await.len(), 0);
        assert_eq!(disseminator.nodes_deceased.read().await.len(), 0);
        assert_eq!(
            result,
            vec![
                Gossip {
                    event: Some(event1)
                },
                Gossip {
                    event: Some(event2)
                }
            ]
        );
    }

    #[tokio::test]
    async fn test_disseminator_push() {
        let disseminator = Disseminator::new(6, 128, 5);
        let event = Event::NodeJoined(NodeJoined {
            from: "NODE_A".to_string(),
            new_member: "NODE_B".to_string(),
        });
        let update = DisseminatorUpdate::NodesAlive(event);
        disseminator.push(update).await;

        let event = Event::NodeDeceased(NodeDeceased {
            from: "NODE_A".to_string(),
            deceased: "NODE_B".to_string(),
            deceased_incarnation_no: 0,
        });
        let update = DisseminatorUpdate::NodesDeceased(event);
        disseminator.push(update).await;

        assert_eq!(disseminator.nodes_alive.read().await.len(), 1);
        assert_eq!(disseminator.nodes_deceased.read().await.len(), 1);
    }
}
