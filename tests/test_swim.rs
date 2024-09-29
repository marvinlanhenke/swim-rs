use std::time::Duration;

use swim_rs::{
    api::{config::SwimConfig, swim::SwimCluster},
    Event,
};

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

fn create_config_with_duration(duration: Duration) -> SwimConfig {
    SwimConfig::builder()
        .with_ping_interval(duration)
        .with_ping_timeout(duration)
        .with_ping_req_timeout(duration)
        .with_suspect_timeout(duration)
        .build()
}

#[tokio::test]
async fn test_swim_node_recovered_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node1 = SwimCluster::try_new("127.0.0.1:0", config).await.unwrap();
    let node2 = SwimCluster::try_new("127.0.0.1:0", SwimConfig::new())
        .await
        .unwrap();
    node1.membership_list().add_member(node2.addr(), 0);

    node1.run().await;

    let mut rx = node1.subscribe();

    loop {
        match rx.recv().await {
            Ok(Event::NodeSuspected(_)) => {
                node2.run().await;
                break;
            }
            Ok(_) => continue,
            Err(_) => panic!(),
        }
    }

    assert_event!(Event::NodeRecovered(_), rx, 3000);
}

#[tokio::test]
async fn test_swim_node_joined_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node1 = SwimCluster::try_new("127.0.0.1:0", config).await.unwrap();
    let node2 = SwimCluster::try_new(
        "127.0.0.1:0",
        SwimConfig::builder()
            .with_known_peers(&[node1.addr()])
            .build(),
    )
    .await
    .unwrap();

    node1.run().await;
    node2.run().await;

    let mut rx = node1.subscribe();

    assert_event!(Event::NodeJoined(_), rx, 3000);
}

#[tokio::test]
async fn test_swim_node_deceased_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node = SwimCluster::try_new("127.0.0.1:0", config).await.unwrap();
    node.membership_list().add_member("127.0.0.1:8081", 0);

    node.run().await;

    let mut rx = node.subscribe();

    assert_event!(Event::NodeDeceased(_), rx, 3000);
}

#[tokio::test]
async fn test_swim_node_suspect_event() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node = SwimCluster::try_new("127.0.0.1:0", config).await.unwrap();
    node.membership_list().add_member("127.0.0.1:8081", 0);

    node.run().await;

    let mut rx = node.subscribe();

    assert_event!(Event::NodeSuspected(_), rx, 3000);
}
