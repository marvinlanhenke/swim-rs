use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use rand::{seq::IteratorRandom, thread_rng};
use snafu::location;
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::pb::{Member, NodeState};

#[derive(Clone, Debug)]
pub struct MembershipList {
    addr: String,
    members: DashMap<String, Member>,
    notify: Arc<Notify>,
}

impl MembershipList {
    pub fn new(addr: impl Into<String>, incarnation: u64) -> Self {
        let addr = addr.into();
        let member = Member {
            addr: addr.clone(),
            state: NodeState::Alive as i32,
            incarnation,
        };
        let members = DashMap::from_iter([(addr.clone(), member)]);
        let notify = Arc::new(Notify::new());

        Self {
            addr,
            members,
            notify,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.members.len()
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
        self.members.get(addr).map(|s| {
            NodeState::try_from(s.value().state).map_err(|e| Error::ProstUnknownEnumValue {
                message: e.to_string(),
                location: location!(),
            })
        })
    }

    pub fn add_member(&self, addr: impl Into<String>, incarnation: u64) {
        let addr = addr.into();
        let member = Member {
            addr: addr.clone(),
            state: NodeState::Alive as i32,
            incarnation,
        };
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

    fn notify_waiters(&self) {
        if self.len() > 1 {
            self.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pb::{Member, NodeState};

    use super::MembershipList;

    #[test]
    fn test_membershiplist_update_member() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0);
        let member = Member {
            addr: "NODE_B".to_string(),
            state: NodeState::Pending as i32,
            incarnation: 0,
        };
        membership_list.update_member(member);

        assert_eq!(membership_list.len(), 2);
        assert_eq!(
            membership_list.member_state("NODE_B").unwrap().unwrap(),
            NodeState::Pending
        );
    }

    #[test]
    fn test_membershiplist_add_member() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0);
        membership_list.add_member("NODE_C", 0);

        assert_eq!(membership_list.len(), 3);
        assert!(membership_list.members().contains_key("NODE_B"));
        assert!(membership_list.members().contains_key("NODE_C"));
    }

    #[test]
    fn test_membershiplist_get_random_member() {
        let membership_list = MembershipList::new("NODE_A", 0);
        membership_list.add_member("NODE_B", 0);

        let random_members = membership_list.get_random_member_list(1, None);
        assert_eq!(random_members.len(), 1);

        let random_members = membership_list.get_random_member_list(2, None);
        assert_eq!(random_members.len(), 1);

        let random_members = membership_list.get_random_member_list(200, None);
        assert_eq!(random_members.len(), 1);
    }

    #[test]
    fn test_membershiplist_update_from_iter() {
        let iter = [
            Member {
                addr: "NODE_A".to_string(),
                state: NodeState::Suspected as i32,
                incarnation: 0,
            },
            Member {
                addr: "NODE_B".to_string(),
                state: NodeState::Alive as i32,
                incarnation: 0,
            },
            Member {
                addr: "NODE_C".to_string(),
                state: NodeState::Alive as i32,
                incarnation: 0,
            },
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
