use tokio::net::UdpSocket;

use crate::error::Result;
use crate::pb::swim_message::Action;

pub(crate) async fn send_action(
    socket: &UdpSocket,
    action: &Action,
    target: impl AsRef<str>,
) -> Result<usize> {
    let mut buf = vec![];
    action.encode(&mut buf);

    Ok(socket.send_to(&buf, target.as_ref()).await?)
}
