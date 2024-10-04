//! # Disseminator Module
//!
//! This module provides the `Disseminator` struct and related types,
//! which manage the dissemination of gossip messages in the SWIM protocol.
//! The disseminator maintains gossip messages to be sent to other nodes,
//! ensuring that updates are propagated efficiently and reliably.
//!
//! It handles both nodes that are alive and nodes that are deceased,
//! tracking how many times each gossip message has been sent,
//! and removes messages once they have been sent a sufficient number of times.
use std::{
    collections::{binary_heap::PeekMut, BinaryHeap, HashSet},
    hash::Hash,
    sync::Arc,
};

use prost::Message;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::{pb::Gossip, Event};

/// A `GossipHeapEntry` represents an entry in the disseminator's gossip buffers.
///
/// It contains a gossip message along with metadata such as the number of times
/// the message has been sent and its size.
///
/// The entries are stored in a binary heap, ordered by the number of times
/// they have been sent, to prioritize less frequently sent messages.
#[derive(Clone, Debug)]
struct GossipHeapEntry {
    /// The gossip message to be disseminated.
    gossip: Gossip,
    /// The number of times this gossip message has been sent.
    num_send: usize,
    /// The size of the gossip message in bytes.
    size: usize,
}

impl GossipHeapEntry {
    /// Creates a new `GossipHeapEntry` with the given gossip message.
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

impl Hash for GossipHeapEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.gossip.hash(state);
    }
}

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

/// An update to be processed by the `Disseminator`.
///
/// This enum represents different types of gossip updates that can be pushed
/// to the disseminator for eventual dissemination to other nodes.
#[derive(Clone, Debug)]
pub(crate) enum DisseminatorUpdate {
    /// An event indicating that a node is not yet deceased.
    NodesAlive(Event),
    /// An event indicating that a node is deceased.
    NodesDeceased(Event),
}

/// The `Disseminator` manages the dissemination of gossip messages in the SWIM protocol.
///
/// It maintains separate heaps for alive and deceased node gossip messages,
/// ensuring that updates are sent a specified number of times before being removed.
/// The disseminator selects gossip messages to send based on the configured parameters,
/// such as maximum number of messages, maximum size, and send constants.
#[derive(Clone, Debug)]
pub(crate) struct Disseminator {
    /// A buffer of gossip messages for nodes that are alive.
    nodes_alive: Arc<RwLock<BinaryHeap<GossipHeapEntry>>>,
    /// A buffer of gossip messages for nodes that are deceased.
    nodes_deceased: Arc<RwLock<BinaryHeap<GossipHeapEntry>>>,
    /// Maximum number of gossip messages to select.
    max_selected: usize,
    /// Maximum total size of gossip messages to send.
    max_size: usize,
    /// Constant determining how many times a message should be sent.
    max_send_constant: usize,
}

impl Disseminator {
    /// Creates a new `Disseminator` with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `max_selected` - Maximum number of gossip messages to select.
    /// * `max_size` - Maximum total size (in bytes) of gossip messages to send.
    /// * `max_send_constant` - Constant determining how many times a message should be sent.
    pub(crate) fn new(max_selected: usize, max_size: usize, max_send_constant: usize) -> Self {
        Self {
            nodes_alive: Arc::new(RwLock::new(BinaryHeap::new())),
            nodes_deceased: Arc::new(RwLock::new(BinaryHeap::new())),
            max_selected,
            max_size,
            max_send_constant,
        }
    }

    /// Pushes a new update into the disseminator for dissemination.
    ///
    /// Depending on the type of the update (`NodesAlive` or `NodesDeceased`),
    /// the update is added to the corresponding buffer.
    pub(crate) async fn push(&self, update: DisseminatorUpdate) {
        match update {
            DisseminatorUpdate::NodesAlive(event) => {
                let item = GossipHeapEntry::new(Gossip { event: Some(event) });
                let mut nodes_alive = self.nodes_alive.write().await;
                nodes_alive.push(item);
            }
            DisseminatorUpdate::NodesDeceased(event) => {
                let item = GossipHeapEntry::new(Gossip { event: Some(event) });
                let mut nodes_deceased = self.nodes_deceased.write().await;
                nodes_deceased.push(item);
            }
        }
    }

    /// Checks if a node is (recently) deceased
    /// by looking up in the disseminator's deceased nodes buffer.
    ///
    /// Returns the deceased incarnation number if the node is found, or `None` otherwise.
    pub(crate) async fn is_deceased(&self, addr: impl AsRef<str>) -> Option<u64> {
        let addr = addr.as_ref();

        let nodes_deceased = self.nodes_deceased.read().await;

        nodes_deceased
            .iter()
            .find_map(|gossip| match &gossip.gossip.event {
                Some(Event::NodeDeceased(e)) => {
                    if e.deceased == addr {
                        Some(e.deceased_incarnation_no)
                    } else {
                        None
                    }
                }
                _ => None,
            })
    }

    /// Retrieves a list of gossip messages to send, based on the disseminator's configuration.
    ///
    /// This method selects gossip messages from both alive and deceased buffers,
    /// ensuring that the total size and number of messages do not exceed the configured limits.
    ///
    /// It prioritizes messages that have been sent fewer times.
    pub(crate) async fn get_gossip(&self, num_members: usize) -> Vec<Gossip> {
        let max_send =
            (((num_members as f64).log10() + 1f64) * self.max_send_constant as f64).ceil() as usize;
        let max_selected_alive = self.max_selected / 2 + self.max_selected % 2;
        let max_selected_deceased = self.max_selected - max_selected_alive;

        let mut current_size = 0;
        let mut nodes_alive_selected = 0;
        let mut nodes_deceased_selected = 0;
        let mut seen = HashSet::new();
        let mut gossip = Vec::new();

        let mut nodes_alive = self.nodes_alive.write().await;
        let mut nodes_deceased = self.nodes_deceased.write().await;

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

    /// Processes a single heap entry from the given heap and potentially adds it to the gossip list.
    ///
    /// This method updates the send count for the entry, removes it if it has been sent enough times,
    /// and updates the selection counts and current size.
    fn process_heap_entry(
        heap: &mut RwLockWriteGuard<BinaryHeap<GossipHeapEntry>>,
        gossip: &mut Vec<Gossip>,
        seen: &mut HashSet<GossipHeapEntry>,
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

                if seen.contains(&entry) {
                    return false;
                }

                gossip.push(entry.gossip.clone());

                entry.num_send += 1;
                *current_size += entry.size;
                *num_selected += 1;
                seen.insert(entry.clone());

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
    use crate::{pb::Gossip, Event};

    use super::{Disseminator, DisseminatorUpdate};

    #[tokio::test]
    async fn test_disseminator_pop_both_no_space_left() {
        let disseminator = Disseminator::new(6, 32, 1);
        let updates = [
            DisseminatorUpdate::NodesAlive(Event::new_node_joined("NODE_A", "NODE_B", 0)),
            DisseminatorUpdate::NodesAlive(Event::new_node_joined("NODE_C", "NODE_D", 0)),
            DisseminatorUpdate::NodesDeceased(Event::new_node_deceased("NODE_A", "NODE_B", 0)),
            DisseminatorUpdate::NodesDeceased(Event::new_node_deceased("NODE_C", "NODE_D", 0)),
        ];

        for update in updates {
            disseminator.push(update).await;
        }

        let result = disseminator.get_gossip(0).await;
        let expected = vec![Gossip {
            event: Some(Event::new_node_joined("NODE_A", "NODE_B", 0)),
        }];

        assert_eq!(result, expected);
        assert_eq!(disseminator.nodes_alive.read().await.len(), 1);
        assert_eq!(disseminator.nodes_deceased.read().await.len(), 2);
    }

    #[tokio::test]
    async fn test_disseminator_pop_both_same_len() {
        let disseminator = Disseminator::new(6, 128, 6);
        let event1 = Event::new_node_joined("NODE_A", "NODE_B", 0);
        let event2 = Event::new_node_joined("NODE_C", "NODE_D", 0);
        let event3 = Event::new_node_deceased("NODE_A", "NODE_B", 0);
        let event4 = Event::new_node_deceased("NODE_C", "NODE_D", 0);
        let updates = [
            DisseminatorUpdate::NodesAlive(event1.clone()),
            DisseminatorUpdate::NodesAlive(event2.clone()),
            DisseminatorUpdate::NodesDeceased(event3.clone()),
            DisseminatorUpdate::NodesDeceased(event4.clone()),
        ];

        for update in updates {
            disseminator.push(update).await;
        }

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
        let event1 = Event::new_node_joined("NODE_A", "NODE_B", 0);
        let event2 = Event::new_node_joined("NODE_C", "NODE_D", 0);
        let event3 = Event::new_node_deceased("NODE_A", "NODE_B", 0);
        let updates = [
            DisseminatorUpdate::NodesAlive(event1.clone()),
            DisseminatorUpdate::NodesAlive(event2.clone()),
            DisseminatorUpdate::NodesDeceased(event3.clone()),
        ];

        for update in updates {
            disseminator.push(update).await;
        }

        let result = disseminator.get_gossip(3).await;

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
        let event1 = Event::new_node_joined("NODE_A", "NODE_B", 0);
        let event2 = Event::new_node_deceased("NODE_A", "NODE_B", 0);
        let updates = [
            DisseminatorUpdate::NodesAlive(event1.clone()),
            DisseminatorUpdate::NodesAlive(event2.clone()),
        ];

        for update in updates {
            disseminator.push(update).await;
        }

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
        let update = DisseminatorUpdate::NodesAlive(Event::new_node_joined("NODE_A", "NODE_B", 0));
        disseminator.push(update).await;

        let update =
            DisseminatorUpdate::NodesDeceased(Event::new_node_deceased("NODE_A", "NODE_B", 0));
        disseminator.push(update).await;

        assert_eq!(disseminator.nodes_alive.read().await.len(), 1);
        assert_eq!(disseminator.nodes_deceased.read().await.len(), 1);
    }
}
