//! The first steps of handling a downstream event once the stream's first
//! valid `connected` came: whether it can be parsed, and for a Session
//! instruction its identities and content fingerprint.

use protocol::connection::Envelope;
use protocol::id::Id;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::protocol_error::ProtocolErrorKind;

/// A downstream event as its first steps read it.
#[derive(Debug)]
pub enum Read {
    /// It cannot be parsed. `message_id` is its `message_id` when one can be
    /// read as an `Id`; no other identity is guessed.
    Unparseable {
        kind: ProtocolErrorKind,
        message_id: Option<Id>,
    },
    Session(SessionMessage),
    /// A message that is not a Session instruction, and its data.
    Other(Envelope, String),
}

/// A Session instruction whose envelope and identities can be read.
#[derive(Debug, Clone)]
pub struct SessionMessage {
    pub envelope: Envelope,
    /// The event's data as it arrived.
    pub data: String,
    /// See [`fingerprint`].
    pub fingerprint: String,
    pub session_id: String,
    pub binding_id: String,
    /// Present for tool and local MCP instructions.
    pub tool_use_event_id: Option<String>,
    pub task_id: Option<String>,
}

/// Reads `data`: strictly as one JSON object passing the envelope's schema,
/// then, for a Session instruction, its nonempty `session_id` and
/// `binding_id` and, for a tool or local MCP instruction, its nonempty
/// `tool_use_event_id` and `task_id`.
pub fn read(data: String) -> Read {
    let Ok(envelope) = protocol::decode::<Envelope>(data.as_bytes()) else {
        return Read::Unparseable {
            kind: ProtocolErrorKind::Unparseable,
            message_id: readable_message_id(&data),
        };
    };
    if !envelope.kind.is_session_instruction() {
        return Read::Other(envelope, data);
    }
    let member = |name: &str| {
        envelope
            .payload
            .get(name)
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
    };
    let (session_id, binding_id) = (member("session_id"), member("binding_id"));
    let (tool_use_event_id, task_id) = if envelope.kind.is_tool_instruction() {
        (member("tool_use_event_id"), member("task_id"))
    } else {
        (None, None)
    };
    let complete = session_id.is_some()
        && binding_id.is_some()
        && (!envelope.kind.is_tool_instruction()
            || (tool_use_event_id.is_some() && task_id.is_some()));
    let (Some(session_id), Some(binding_id), true) = (session_id, binding_id, complete) else {
        return Read::Unparseable {
            kind: ProtocolErrorKind::MissingIdentity,
            message_id: Some(envelope.message_id),
        };
    };
    let value: Value = serde_json::from_str(&data).expect("an envelope that decoded is JSON");
    Read::Session(SessionMessage {
        fingerprint: fingerprint(&value),
        envelope,
        data,
        session_id,
        binding_id,
        tool_use_event_id,
        task_id,
    })
}

/// The `message_id` of a message that cannot be parsed, when it is one JSON
/// object whose `message_id` is an `Id`.
fn readable_message_id(data: &str) -> Option<Id> {
    let value: Value = serde_json::from_str(data).ok()?;
    serde_json::from_value(value.get("message_id")?.clone()).ok()
}

/// The lowercase hexadecimal SHA-256 of `value` re-encoded with object
/// members sorted by name and no whitespace, so that member order and
/// whitespace are no change while an omitted member or another value is.
/// The sorting is done here, not left to the JSON library, whose maps keep
/// insertion order when a dependency turns that on.
pub fn fingerprint(value: &Value) -> String {
    let mut encoded = Vec::new();
    canonical(value, &mut encoded);
    Sha256::digest(&encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(members) => {
            let mut names: Vec<&String> = members.keys().collect();
            names.sort();
            out.push(b'{');
            for (index, name) in names.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, name).expect("a string encodes");
                out.push(b':');
                canonical(&members[name], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                canonical(item, out);
            }
            out.push(b']');
        }
        scalar => serde_json::to_writer(&mut *out, scalar).expect("a scalar encodes"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(data: &str) -> SessionMessage {
        match read(data.to_owned()) {
            Read::Session(message) => message,
            other => panic!("{data} read as {other:?}"),
        }
    }

    fn unparseable(data: &str) -> (ProtocolErrorKind, Option<String>) {
        match read(data.to_owned()) {
            Read::Unparseable { kind, message_id } => {
                (kind, message_id.map(|id| id.as_str().to_owned()))
            }
            other => panic!("{data} read as {other:?}"),
        }
    }

    #[test]
    fn member_order_and_whitespace_keep_the_fingerprint_and_any_other_change_does_not() {
        let close = session(
            r#"{"protocol_version":1,"connection_id":"conn_a","message_id":"msg_a","type":"session.close","payload":{"session_id":"sess_a","binding_id":"bind_a"}}"#,
        );
        let reordered = session(
            "{ \"payload\" : { \"binding_id\":\"bind_a\", \"session_id\":\"sess_a\" },\n \"type\":\"session.close\", \"message_id\":\"msg_a\", \"connection_id\":\"conn_a\", \"protocol_version\":1 }",
        );
        assert_eq!(close.fingerprint, reordered.fingerprint);
        assert_eq!(close.fingerprint.len(), 64);
        for changed in [
            r#"{"protocol_version":1,"connection_id":"conn_a","message_id":"msg_a","type":"session.close","payload":{"session_id":"sess_a","binding_id":"bind_b"}}"#,
            r#"{"protocol_version":1,"connection_id":"conn_a","message_id":"msg_a","type":"session.close","payload":{"session_id":"sess_a","binding_id":"bind_a","extra":1}}"#,
            r#"{"protocol_version":1,"connection_id":"conn_a","message_id":"msg_a","type":"session.close","payload":{"session_id":"sess_a","binding_id":"bind_a"},"traceparent":"x"}"#,
        ] {
            assert_ne!(session(changed).fingerprint, close.fingerprint, "{changed}");
        }
    }

    #[test]
    fn a_message_without_its_envelope_or_identities_cannot_be_parsed() {
        assert_eq!(unparseable("[1]"), (ProtocolErrorKind::Unparseable, None));
        assert_eq!(
            unparseable(r#"{"message_id":"msg_a","type":"session.close"}"#),
            (ProtocolErrorKind::Unparseable, Some("msg_a".to_owned()))
        );
        assert_eq!(
            unparseable(
                r#"{"protocol_version":1,"connection_id":"conn_a","message_id":"msg_a","type":"session.close","payload":{"session_id":"","binding_id":"bind_a"}}"#
            ),
            (ProtocolErrorKind::MissingIdentity, Some("msg_a".to_owned()))
        );
        assert_eq!(
            unparseable(
                r#"{"protocol_version":1,"connection_id":"conn_a","message_id":"msg_a","type":"tool.cancel","payload":{"session_id":"sess_a","binding_id":"bind_a","tool_use_event_id":"tue_a"}}"#
            ),
            (ProtocolErrorKind::MissingIdentity, Some("msg_a".to_owned()))
        );
        let execute = session(
            r#"{"protocol_version":1,"connection_id":"conn_a","message_id":"msg_a","type":"tool.cancel","payload":{"session_id":"sess_a","binding_id":"bind_a","tool_use_event_id":"tue_a","task_id":"task_a"}}"#,
        );
        assert_eq!(execute.task_id.as_deref(), Some("task_a"));
    }
}
