use tokio::net::UdpSocket;

use crate::error::Result;
use crate::pb::swim_message::Action;

pub async fn send_action(
    socket: &UdpSocket,
    action: &Action,
    target: impl AsRef<str>,
) -> Result<()> {
    let mut buf = vec![];
    action.encode(&mut buf);

    socket.send_to(&buf, target.as_ref()).await?;

    Ok(())
}
