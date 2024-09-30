use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use rand::Rng;
use rand::{seq::IteratorRandom, thread_rng};
use snafu::location;
use tokio::sync::{Notify, RwLock};

use crate::error::{Error, Result};
use crate::pb::{Member, NodeState};

impl Member {
    pub(crate) fn new(addr: impl Into<String>, state: NodeState, incarnation: u64) -> Self {
        Self {
            addr: addr.into(),
            state: state as i32,
            incarnation,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MembershipList {
    addr: String,
    members: DashMap<String, Member>,
    sorted_members: Arc<RwLock<Vec<String>>>,
    current_index: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl MembershipList {
    pub fn new(addr: impl Into<String>, incarnation: u64) -> Self {
        let addr = addr.into();
        let members = DashMap::from_iter([(
            addr.clone(),
            Member::new(&addr, NodeState::Alive, incarnation),
        )]);
        let sorted_members = Arc::new(RwLock::new(vec![addr.clone()]));
        let current_index = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());

        Self {
            addr,
            members,
            sorted_members,
            current_index,
            notify,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn current_index(&self) -> usize {
        self.current_index.load(Ordering::SeqCst)
    }

    pub fn members(&self) -> &DashMap<String, Member> {
        &self.members
    }

    pub fn members_hashmap(&self) -> HashMap<String, Member> {
        self.members
            .iter()
            .map(|x| (x.key().clone(), x.value().clone()))
            .collect()
    }

    pub fn member_state(&self, addr: impl AsRef<str>) -> Option<Result<NodeState>> {
        let addr = addr.as_ref();
        self.members.get(addr).map(|entry| {
            NodeState::try_from(entry.value().state).map_err(|e| Error::ProstUnknownEnumValue {
                message: e.to_string(),
                location: location!(),
            })
        })
    }

    pub fn member_incarnation(&self, addr: impl AsRef<str>) -> Option<u64> {
        let addr = addr.as_ref();
        self.members()
            .get(addr)
            .map(|entry| entry.value().incarnation)
    }

    pub async fn add_member(&self, addr: impl Into<String>, incarnation: u64) {
        let addr = addr.into();

        self.add_sorted_member(&addr).await;

        let member = Member::new(&addr, NodeState::Alive, incarnation);
        self.members.insert(addr, member);

        self.notify_waiters();
    }

    pub fn update_member(&self, member: Member) {
        self.members.insert(member.addr.clone(), member);
        self.notify_waiters();
    }

    pub fn update_from_iter<I>(&self, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = Member>,
    {
        for member in iter {
            let key = member.addr.clone();
            self.members.insert(key, member);
        }

        self.notify_waiters();

        Ok(())
    }

    // TODO: refactor, use sorted cache, use bool when sort is needed
    pub(crate) async fn get_member_list(
        &self,
        amount: usize,
        exclude: Option<&str>,
    ) -> Vec<Member> {
        let mut sorted_members = self
            .members()
            .iter()
            .map(|x| x.key().clone())
            .collect::<Vec<_>>();
        sorted_members.sort();

        let num_excluded = if exclude.is_some() { 1 } else { 0 };
        let amount = amount.min(
            sorted_members
                .len()
                .saturating_sub(num_excluded)
                .saturating_sub(1),
        );

        let mut selected_members = Vec::with_capacity(amount);
        let mut selected_count = 0;

        while selected_count < amount {
            let current_index = self.current_index.load(Ordering::SeqCst);

            if let Some(member_str) = sorted_members.get(current_index) {
                if member_str == &self.addr {
                    let next_index = (current_index + 1) % sorted_members.len();
                    self.current_index.store(next_index, Ordering::SeqCst);
                    continue;
                }
                if let Some(exclude) = exclude {
                    if exclude == member_str {
                        let next_index = (current_index + 1) % sorted_members.len();
                        self.current_index.store(next_index, Ordering::SeqCst);
                        continue;
                    }
                }

                if let Some(member) = self.members().get(member_str) {
                    selected_members.push(member.clone());
                }
                selected_count += 1;
            }

            let next_index = (current_index + 1) % sorted_members.len();
            self.current_index.store(next_index, Ordering::SeqCst);
        }

        selected_members
    }

    pub(crate) fn get_random_member_list(
        &self,
        amount: usize,
        exclude: Option<&str>,
    ) -> Vec<Member> {
        let mut rng = thread_rng();

        self.members
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                if key == &self.addr {
                    return None;
                }

                if let Some(exclude) = exclude {
                    if key == exclude {
                        return None;
                    }
                }

                Some(entry.value().clone())
            })
            .choose_multiple(&mut rng, amount)
    }

    pub(crate) async fn wait_for_members(&self) {
        while self.len() <= 1 {
            tracing::debug!("[{}] waiting for members", self.addr);
            self.notify.notified().await;
        }
    }

    async fn add_sorted_member(&self, addr: impl Into<String>) {
        let mut sorted_members = self.sorted_members.write().await;
        let index = thread_rng().gen_range(0..=sorted_members.len());
        sorted_members.insert(index, addr.into());
    }

    fn notify_waiters(&self) {
        if self.len() > 1 {
            self.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::pb::{Member, NodeState};

    use super::MembershipList;

    #[tokio::test]
    async fn test_membershiplist_get_member_list() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0).await;
        membership_list.add_member("NODE_C", 0).await;
        membership_list.add_member("NODE_D", 0).await;

        let result = membership_list.get_member_list(2, Some("NODE_C")).await;
        assert_eq!(
            result,
            vec![
                Member::new("NODE_B", NodeState::Alive, 0),
                Member::new("NODE_D", NodeState::Alive, 0)
            ]
        );

        membership_list.current_index.store(0, Ordering::SeqCst);
        let result = membership_list.get_member_list(5, Some("NODE_C")).await;
        assert_eq!(
            result,
            vec![
                Member::new("NODE_B", NodeState::Alive, 0),
                Member::new("NODE_D", NodeState::Alive, 0),
            ]
        );

        membership_list.current_index.store(0, Ordering::SeqCst);
        let result = membership_list.get_member_list(4, None).await;
        assert_eq!(
            result,
            vec![
                Member::new("NODE_B", NodeState::Alive, 0),
                Member::new("NODE_C", NodeState::Alive, 0),
                Member::new("NODE_D", NodeState::Alive, 0),
            ]
        );

        membership_list.current_index.store(0, Ordering::SeqCst);
        let result = membership_list.get_member_list(99, None).await;
        assert_eq!(
            result,
            vec![
                Member::new("NODE_B", NodeState::Alive, 0),
                Member::new("NODE_C", NodeState::Alive, 0),
                Member::new("NODE_D", NodeState::Alive, 0),
            ]
        );

        membership_list.current_index.store(0, Ordering::SeqCst);
        let result = membership_list.get_member_list(1, None).await;
        assert_eq!(result, vec![Member::new("NODE_B", NodeState::Alive, 0),]);

        membership_list.current_index.store(0, Ordering::SeqCst);
        let result = membership_list.get_member_list(1, Some("NODE_A")).await;
        assert_eq!(result, vec![Member::new("NODE_B", NodeState::Alive, 0),]);
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
    async fn test_membershiplist_get_random_member() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0).await;

        let random_members = membership_list.get_random_member_list(1, None);
        assert_eq!(random_members.len(), 1);

        let random_members = membership_list.get_random_member_list(2, None);
        assert_eq!(random_members.len(), 1);

        let random_members = membership_list.get_random_member_list(200, None);
        assert_eq!(random_members.len(), 1);
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
        membership_list.update_from_iter(iter).unwrap();

        let members = membership_list.members();

        assert_eq!(members.len(), 3);
        assert!(members.contains_key("NODE_A"));
        assert_eq!(
            members.get("NODE_A").unwrap().value().state,
            NodeState::Suspected as i32
        );
    }
}
