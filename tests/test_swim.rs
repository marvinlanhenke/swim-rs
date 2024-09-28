use std::time::Duration;

use swim_rs::{api::config::SwimConfig, pb::gossip::Event, MembershipList, SwimNode};
use tokio::{net::UdpSocket, sync::broadcast};

macro_rules! assert_event {
    ($event:pat, $rx:expr, $ms:expr) => {
        let result = tokio::time::timeout(Duration::from_millis($ms), async {
            loop {
                match $rx.recv().await {
                    Ok($event) => break,
                    Ok(_) => continue,
                    Err(_) => panic!(),
                }
            }
        })
        .await;

        if result.is_err() {
            panic!()
        }
    };
}

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

    assert_event!(Event::NodeDeceased(_), rx, 3000);

    dispatch_handle.abort();
    detection_handle.abort();
}
