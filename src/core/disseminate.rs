use std::{
    collections::{binary_heap::PeekMut, BinaryHeap},
    sync::Arc,
};

use itertools::{EitherOrBoth, Itertools};
use prost::Message;
use tokio::sync::RwLock;

use crate::{pb::Gossip, Event};

#[derive(Debug)]
struct GossipHeapEntry {
    gossip: Gossip,
    num_send: usize,
    size: usize,
}

impl GossipHeapEntry {
    fn new(gossip: Gossip) -> Self {
        let size = gossip.encoded_len();

        Self {
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
    max_send: usize,
    max_size: usize,
}

impl Disseminator {
    pub(crate) fn new(max_send: usize, max_size: usize) -> Self {
        Self {
            nodes_alive: Arc::new(RwLock::new(BinaryHeap::new())),
            nodes_deceased: Arc::new(RwLock::new(BinaryHeap::new())),
            max_send,
            max_size,
        }
    }

    pub(crate) async fn nodes_alive_size(&self) -> usize {
        self.nodes_alive.read().await.len()
    }

    pub(crate) async fn nodes_deceased_size(&self) -> usize {
        self.nodes_deceased.read().await.len()
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

    pub(crate) async fn pop(&self) -> Vec<Gossip> {
        let mut gossip = Vec::new();

        let mut nodes_alive = self.nodes_alive.write().await;
        let mut nodes_deceased = self.nodes_deceased.write().await;

        let mut current_size = 0;
        let mut nodes_alive_selected = 0;
        let mut nodes_deceased_selected = 0;

        loop {
            // when to break:
            // -> current_size + new_entries >= max_size
            // -> we processed all entries from both buffers
            // -> or all buffers are empty

            // peek_mut from buffer a, if is not empty and we have not processed
            // all messages already for this iteration
            // -> check if size fits?
            // -> increase num_send += 1
            // -> push gossip
            // -> if num_send >= max_send => PeekMut::pop(entry)

            // peek_mut from buffer a, if is not empty and we have not processed
            // all messages already for this iteration
            break;
        }

        gossip
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
    async fn test_disseminator_pop() {
        let disseminator = Disseminator::new(1, 128);
        let event1 = Event::NodeJoined(NodeJoined {
            from: "NODE_A".to_string(),
            new_member: "NODE_B".to_string(),
        });
        let event2 = Event::NodeJoined(NodeJoined {
            from: "NODE_C".to_string(),
            new_member: "NODE_D".to_string(),
        });
        let update1 = DisseminatorUpdate::NodesAlive(event1.clone());
        let update2 = DisseminatorUpdate::NodesAlive(event2.clone());
        disseminator.push(update1).await;
        disseminator.push(update2).await;

        let result1 = disseminator.pop().await;
        let result2 = disseminator.pop().await;

        assert_eq!(disseminator.nodes_alive_size().await, 0);
        assert_eq!(
            result1,
            vec![Gossip {
                event: Some(event1)
            }]
        );
        assert_eq!(
            result2,
            vec![Gossip {
                event: Some(event2)
            }]
        );
    }

    #[tokio::test]
    async fn test_disseminator_push() {
        let disseminator = Disseminator::new(5, 128);
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

        assert_eq!(disseminator.nodes_alive_size().await, 1);
        assert_eq!(disseminator.nodes_deceased_size().await, 1);
    }
}
