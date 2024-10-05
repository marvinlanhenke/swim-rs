//! # Membership List Module
//!
//! This module defines the `MembershipList` and related structures used to manage
//! the list of nodes participating in the SWIM protocol implementation.
//! It maintains the list of members, their states, and provides methods to
//! add, remove, and update members, as well as to get subsets of members for
//! gossiping and failure detection.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use snafu::location;
use tokio::sync::{Notify, RwLock};

use crate::error::{Error, Result};
use crate::pb::{Member, NodeState};

impl Member {
    /// Creates a new `Member` instance with the specified address, state, and incarnation number.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the member node (e.g., "127.0.0.1:8080").
    /// * `state` - The state of the node (e.g., `NodeState::Alive`).
    /// * `incarnation` - The incarnation number of the node.
    pub(crate) fn new(addr: impl Into<String>, state: NodeState, incarnation: u64) -> Self {
        Self {
            addr: addr.into(),
            state: state as i32,
            incarnation,
        }
    }
}

/// An index that maintains the positions of members in the membership list.
///
/// Used for selecting members in a round-robin fashion and shuffling the list
/// periodically to ensure randomness in member selection.
#[derive(Clone, Debug)]
struct MembershipListIndex {
    /// The list of member addresses.
    index: Arc<RwLock<Vec<String>>>,
    /// The current position in the index.
    pos: Arc<AtomicUsize>,
    /// The length of the index.
    len: Arc<AtomicUsize>,
}

impl MembershipListIndex {
    /// Creates a new [`MembershipListIndex`]
    /// from a slice of member addresses and a starting position.
    ///
    /// # Arguments
    ///
    /// * `index` - A slice of member addresses to initialize the index.
    /// * `pos` - The starting position in the index.
    fn new(index: &[&str], pos: usize) -> Self {
        let len = index.len();
        let index = index.iter().map(|x| x.to_string()).collect::<Vec<_>>();

        Self {
            index: Arc::new(RwLock::new(index)),
            pos: Arc::new(AtomicUsize::new(pos)),
            len: Arc::new(AtomicUsize::new(len)),
        }
    }

    /// Returns the length of the index.
    fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    /// Returns the current position in the index.
    fn pos(&self) -> usize {
        self.pos.load(Ordering::SeqCst)
    }

    /// Advances the current position in the index and shuffles the index if needed.
    ///
    /// Returns the new position.
    async fn advance(&self) -> usize {
        let next_pos = (self.pos() + 1) % self.len();

        if next_pos == 0 {
            self.shuffle().await;
        }

        self.pos.store(next_pos, Ordering::SeqCst);
        next_pos
    }

    /// Inserts a list of member addresses at random positions in the index.
    async fn insert_list_at_random_pos(&self, addrs: &[String]) {
        for addr in addrs.iter() {
            self.insert_at_random_pos(addr).await
        }
    }

    /// Inserts a member address at a random position in the index.
    async fn insert_at_random_pos(&self, addr: impl Into<String>) {
        let pos = thread_rng().gen_range(0..=self.len());
        let mut index = self.index.write().await;
        index.insert(pos, addr.into());
        self.len.store(index.len(), Ordering::SeqCst);
    }

    /// Removes a member address from the index.
    async fn remove(&self, addr: impl AsRef<str>) {
        let mut index = self.index.write().await;

        if let Some(pos) = index.iter().position(|x| x == addr.as_ref()) {
            index.remove(pos);
            self.len.store(index.len(), Ordering::SeqCst);

            if self.pos() == self.len() {
                self.pos.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    /// Returns the current member address at the current position in the index.
    async fn current(&self) -> Option<String> {
        let pos = self.pos();
        let index = self.index.read().await;
        index.get(pos).cloned()
    }

    /// Shuffles the index to randomize the order of member addresses.
    async fn shuffle(&self) {
        let mut index = self.index.write().await;
        index.shuffle(&mut thread_rng());
    }
}

/// The `MembershipList` maintains the list of members in the SWIM cluster.
///
/// It keeps track of member states, provides methods to add, remove, and update members,
/// and supports selecting random subsets of members for gossip and failure detection.
#[derive(Clone, Debug)]
pub struct MembershipList {
    /// The address of this node.
    addr: String,
    /// A map of member addresses to their `Member` struct.
    members: DashMap<String, Member>,
    /// An index to manage member selection and shuffling.
    index: MembershipListIndex,
    /// A notification mechanism to signal when new members are added.
    notify: Arc<Notify>,
}

impl MembershipList {
    /// Creates a new [`MembershipList`] for the node
    /// with the given address and incarnation number.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of this node.
    /// * `incarnation` - The initial incarnation number of this node.
    ///
    /// # Returns
    ///
    /// A new `MembershipList` instance.
    pub fn new(addr: impl Into<String>, incarnation: u64) -> Self {
        let addr = addr.into();
        let members = DashMap::from_iter([(
            addr.clone(),
            Member::new(&addr, NodeState::Alive, incarnation),
        )]);
        let index = MembershipListIndex::new(&[&addr], 0);
        let notify = Arc::new(Notify::new());

        Self {
            addr,
            members,
            index,
            notify,
        }
    }

    /// Checks if the membership list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of members in the membership list.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Returns a reference to the members map.
    pub fn members(&self) -> &DashMap<String, Member> {
        &self.members
    }

    /// Retrieves the state of a member.
    pub fn member_state(&self, addr: impl AsRef<str>) -> Option<Result<NodeState>> {
        let addr = addr.as_ref();
        self.members.get(addr).map(|entry| {
            NodeState::try_from(entry.value().state).map_err(|e| Error::ProstUnknownEnumValue {
                message: e.to_string(),
                location: location!(),
            })
        })
    }

    /// Retrieves the incarnation number of a member.
    pub fn member_incarnation(&self, addr: impl AsRef<str>) -> Option<u64> {
        let addr = addr.as_ref();
        self.members()
            .get(addr)
            .map(|entry| entry.value().incarnation)
    }

    /// Adds a new member to the membership list.
    /// The member is added only if it does not already exist.
    ///
    /// # Returns
    ///
    /// `true` if the member was added, `false` if it already existed.
    pub async fn add_member(&self, addr: impl Into<String>, incarnation: u64) -> bool {
        let addr = addr.into();

        if self.members.contains_key(&addr) {
            return false;
        }

        self.index.insert_at_random_pos(&addr).await;
        let member = Member::new(&addr, NodeState::Alive, incarnation);
        self.members.insert(addr, member);

        self.notify_waiters();
        true
    }

    /// Updates an existing member in the membership list.
    pub fn update_member(&self, member: Member) {
        if self.members.contains_key(&member.addr) {
            self.members.insert(member.addr.clone(), member);
        }
        self.notify_waiters();
    }

    /// Updates the membership list from an iterator of `Member` instances.
    ///
    /// New members are added, and existing members are updated.
    pub async fn update_from_iter<I>(&self, iter: I)
    where
        I: IntoIterator<Item = Member>,
    {
        let new_members = iter
            .into_iter()
            .filter_map(|m| {
                let key = m.addr.clone();
                match self.members.insert(key.clone(), m) {
                    Some(_) => None,
                    None => Some(key),
                }
            })
            .collect::<Vec<_>>();

        self.index.insert_list_at_random_pos(&new_members).await;

        self.notify_waiters();
    }

    /// Removes a member from the membership list.
    pub(crate) async fn remove_member(&self, addr: impl AsRef<str>) -> bool {
        let addr = addr.as_ref();
        self.index.remove(addr).await;
        self.members.remove(addr).is_some()
    }

    /// Retrieves a list of members, excluding certain addresses.
    pub(crate) async fn get_member_list(
        &self,
        amount: usize,
        exclude: Option<&str>,
    ) -> Vec<Member> {
        let max_amount = match exclude.is_some() {
            true => amount.min(self.index.len().saturating_sub(2)),
            false => amount.min(self.index.len().saturating_sub(1)),
        };

        let mut selected_members = Vec::with_capacity(max_amount);

        while selected_members.len() < max_amount {
            match self.index.current().await {
                Some(member_str) => {
                    let should_skip =
                        member_str == self.addr || exclude.map_or(false, |addr| addr == member_str);

                    if should_skip {
                        self.index.advance().await;
                        continue;
                    }

                    if let Some(member) = self.members().get(&member_str) {
                        selected_members.push(member.clone());
                    }

                    self.index.advance().await;
                }
                None => {
                    break;
                }
            }
        }

        selected_members
    }

    /// Waits until at least one other member is present in the membership list.
    pub(crate) async fn wait_for_members(&self) {
        while self.len() <= 1 {
            tracing::debug!("[{}] waiting for members", self.addr);
            self.notify.notified().await;
        }
    }

    /// Notifies any tasks waiting for new members.
    fn notify_waiters(&self) {
        if self.len() > 1 {
            self.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pb::{Member, NodeState};

    use super::{MembershipList, MembershipListIndex};

    #[tokio::test]
    async fn test_membershiplist_index_shuffle_on_complete_iteration() {
        let members = vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
        let index = MembershipListIndex::new(&members, members.len() - 1);

        let result = index.current().await;
        assert_eq!(result, Some("j".to_string()));

        index.advance().await;
        assert_eq!(index.pos(), 0);
        assert_ne!(members, index.index.read().await.as_slice());
    }

    #[tokio::test]
    async fn test_membershiplist_index_current_advance() {
        let members = vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
        let index = MembershipListIndex::new(&members, 0);

        let result = index.current().await;
        assert_eq!(result, Some("a".to_string()));

        index.advance().await;
        let result = index.current().await;
        assert_eq!(result, Some("b".to_string()));
    }

    #[tokio::test]
    async fn test_membershiplist_remove_member() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0).await;
        membership_list.add_member("NODE_C", 0).await;
        membership_list.add_member("NODE_D", 0).await;

        membership_list.remove_member("NODE_C").await;

        assert_eq!(membership_list.len(), 3);
        assert_eq!(membership_list.index.index.read().await.len(), 3);
    }

    #[tokio::test]
    async fn test_membershiplist_get_member_list() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0).await;
        membership_list.add_member("NODE_C", 0).await;
        membership_list.add_member("NODE_D", 0).await;

        let result = membership_list.get_member_list(2, Some("NODE_C")).await;
        assert_eq!(result.len(), 2);

        let result = membership_list.get_member_list(5, Some("NODE_C")).await;
        assert_eq!(result.len(), 2);

        let result = membership_list.get_member_list(4, None).await;
        assert_eq!(result.len(), 3);

        let result = membership_list.get_member_list(99, None).await;
        assert_eq!(result.len(), 3);

        let result = membership_list.get_member_list(1, None).await;
        assert_eq!(result.len(), 1);

        let result = membership_list.get_member_list(1, Some("NODE_A")).await;
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_membershiplist_update_member() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0).await;
        let member = Member::new("NODE_B", NodeState::Pending, 0);
        membership_list.update_member(member);

        assert_eq!(membership_list.len(), 2);
        assert_eq!(
            membership_list.member_state("NODE_B").unwrap().unwrap(),
            NodeState::Pending
        );
    }

    #[tokio::test]
    async fn test_membershiplist_add_member() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0).await;
        membership_list.add_member("NODE_C", 0).await;

        assert_eq!(membership_list.len(), 3);
        assert!(membership_list.members().contains_key("NODE_B"));
        assert!(membership_list.members().contains_key("NODE_C"));
    }

    #[tokio::test]
    async fn test_membershiplist_update_from_iter() {
        let iter = [
            Member::new("NODE_A", NodeState::Suspected, 0),
            Member::new("NODE_B", NodeState::Alive, 0),
            Member::new("NODE_C", NodeState::Alive, 0),
        ];
        let addr = "NODE_A";
        let membership_list = MembershipList::new(addr, 0);
        membership_list.update_from_iter(iter).await;

        let members = membership_list.members();

        assert_eq!(members.len(), 3);
        assert!(members.contains_key("NODE_A"));
        assert_eq!(
            members.get("NODE_A").unwrap().value().state,
            NodeState::Suspected as i32
        );
    }
}
