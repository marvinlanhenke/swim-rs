use std::time::Duration;

use swim_rs::{api::config::SwimConfig, pb::gossip::Event, MembershipList, SwimNode};
use tokio::{net::UdpSocket, sync::broadcast};

#[tokio::test]
async fn test_swim_node_declare_node_as_dead() {
    let addr = "127.0.0.1:8080";
    let socket = UdpSocket::bind(addr).await.unwrap();
    let duration = Duration::from_millis(10);
    let config = SwimConfig::builder()
        .with_ping_interval(duration)
        .with_ping_timeout(duration)
        .with_ping_req_timeout(duration)
        .with_suspect_timeout(duration)
        .build();
    let membership_list = MembershipList::new(addr);
    membership_list.add_member("127.0.0.1:8081");

    let (tx, _) = broadcast::channel::<Event>(32);

    let node = SwimNode::try_new_with_membership_list(socket, config, membership_list, tx).unwrap();
    let (dispatch_handle, detection_handle) = node.run().await;

    let mut rx = node.subscribe();

    loop {
        if let Ok(Event::NodeDeceased(msg)) = rx.recv().await {
            assert_eq!(msg.from, addr);
            assert_eq!(msg.deceased, "127.0.0.1:8081");
            assert_eq!(node.membership_list().len(), 1);
            break;
        }
    }

    dispatch_handle.abort();
    detection_handle.abort();
}
