use std::time::Duration;

use swim_rs::{
    api::{config::SwimConfig, swim::SwimCluster},
    pb::gossip::Event,
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
async fn test_swim_node_declare_node_as_dead() {
    let config = create_config_with_duration(Duration::from_millis(10));
    let node = SwimCluster::try_new("127.0.0.1:8080", config)
        .await
        .unwrap();
    node.membership_list().add_member("127.0.0.1:8081");

    node.run().await;

    let mut rx = node.subscribe();

    assert_event!(Event::NodeDeceased(_), rx, 3000);
}
