//! One connection attempt: `GET /v1/client/events` with the query of its
//! kind, and its stream read until it ends. The task posts each event's data
//! and the stream's end to the Connection owner, which bounds the attempt's
//! time and judges its first valid `connected`.

use std::sync::Arc;
use std::time::Duration;

use protocol::bootstrap::BootstrapKey;
use protocol::connection::{Downstream, DownstreamPayload, Resume};
use protocol::decode;
use protocol::id::Id;
use tokio::sync::mpsc;

use crate::http::Transport;
use crate::owner::Message;
use crate::stream::{Chunks, Events};

/// The time from an attempt's start to its first valid `connected`.
pub const ATTEMPT_TOTAL: Duration = Duration::from_secs(10);

/// What an attempt asks Engine for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptKind {
    /// A new connection; `previous` is the last connection held, none on the
    /// first attempt after App Server start.
    Fresh { previous: Option<Id> },
    /// The held connection on a new stream.
    Resume { connection: Id },
}

impl AttemptKind {
    pub fn query(&self, instance_id: &Id, credential_id: &Id) -> Vec<(&'static str, String)> {
        let mut query = vec![
            ("protocol_version", "1".to_owned()),
            ("instance_id", instance_id.as_str().to_owned()),
            ("credential_id", credential_id.as_str().to_owned()),
        ];
        let (previous, resume) = match self {
            AttemptKind::Fresh { previous: None } => return query,
            AttemptKind::Fresh {
                previous: Some(previous),
            } => (previous, Resume::False),
            AttemptKind::Resume { connection } => (connection, Resume::True),
        };
        query.push(("previous_connection_id", previous.as_str().to_owned()));
        query.push((
            "resume",
            match resume {
                Resume::True => "true",
                Resume::False => "false",
            }
            .to_owned(),
        ));
        query
    }

    /// The connection and whether it resumed, when `data` is a valid
    /// `connected` for this attempt: `resumed` is `true` only for a resume
    /// of the same connection.
    pub fn connected(&self, data: &str) -> Option<(Id, bool)> {
        let downstream: Downstream = decode(data.as_bytes()).ok()?;
        let DownstreamPayload::Connected(connected) = downstream.payload else {
            return None;
        };
        let resumes = match self {
            AttemptKind::Resume { connection } => *connection == downstream.connection_id,
            AttemptKind::Fresh { .. } => false,
        };
        (!connected.resumed || resumes).then_some((downstream.connection_id, connected.resumed))
    }
}

/// Runs one attempt's request and stream as the task tagged `stream`.
pub(crate) async fn run(
    transport: Arc<Transport>,
    key: BootstrapKey,
    query: Vec<(&'static str, String)>,
    max_event_bytes: usize,
    stream: u64,
    mailbox: mpsc::UnboundedSender<Message>,
) {
    let pairs: Vec<(&str, &str)> = query
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    let response = match transport.events(&pairs, &key).await {
        Ok(response) => response,
        Err(answer) => {
            let _ = mailbox.send(Message::Refused { stream, answer });
            return;
        }
    };
    let mut events = Events::new(Chunks::new(response), max_event_bytes);
    loop {
        match events.next().await {
            Ok(data) => {
                if mailbox.send(Message::Event { stream, data }).is_err() {
                    return;
                }
            }
            Err(_) => {
                let _ = mailbox.send(Message::StreamEnded { stream });
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(text: &str) -> Id {
        Id::new(text).unwrap()
    }

    fn connected(connection: &str, resumed: bool) -> String {
        serde_json::json!({
            "protocol_version": 1,
            "connection_id": connection,
            "message_id": "msg_1",
            "type": "connected",
            "payload": {"resumed": resumed},
        })
        .to_string()
    }

    #[test]
    fn a_resumed_connected_is_valid_only_for_a_resume_of_the_same_connection() {
        let fresh = AttemptKind::Fresh { previous: None };
        let resume = AttemptKind::Resume {
            connection: id("conn_a"),
        };
        assert_eq!(
            fresh.connected(&connected("conn_a", false)),
            Some((id("conn_a"), false))
        );
        assert_eq!(fresh.connected(&connected("conn_a", true)), None);
        assert_eq!(
            resume.connected(&connected("conn_a", true)),
            Some((id("conn_a"), true))
        );
        assert_eq!(resume.connected(&connected("conn_b", true)), None);
        assert_eq!(
            resume.connected(&connected("conn_b", false)),
            Some((id("conn_b"), false))
        );
        assert_eq!(fresh.connected(r#"{"type":"connected"}"#), None);
    }
}
