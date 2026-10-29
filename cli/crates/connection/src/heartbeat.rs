//! Which `heartbeat` events start a send and which heartbeat ACKs may set
//! the connection deadline. Sequences only grow: Engine never sends a
//! heartbeat under a new `message_id` whose sequence is not above the last.

use protocol::id::Id;

#[derive(Debug, Default)]
pub struct Heartbeats {
    /// The `message_id` and sequence of the latest `heartbeat` event.
    latest: Option<(Id, i64)>,
    /// The sequence of the last ACK that set the deadline; 0 before any.
    acknowledged: i64,
}

impl Heartbeats {
    /// Whether the `heartbeat` event `message_id` with `seq` starts a send:
    /// a repeated event starts nothing, and neither does one whose sequence
    /// is not above the latest.
    pub fn admit(&mut self, message_id: &Id, seq: i64) -> bool {
        if let Some((latest, latest_seq)) = &self.latest
            && (latest == message_id || seq <= *latest_seq)
        {
            return false;
        }
        self.latest = Some((message_id.clone(), seq));
        true
    }

    /// Whether `seq` is the latest event's, the only heartbeat retried.
    pub fn is_latest(&self, seq: i64) -> bool {
        self.latest
            .as_ref()
            .is_some_and(|(_, latest_seq)| *latest_seq == seq)
    }

    /// Whether a valid ACK of `seq` sets the deadline: only one above the
    /// sequence of the last ACK that set it.
    pub fn acknowledge(&mut self, seq: i64) -> bool {
        if seq > self.acknowledged {
            self.acknowledged = seq;
            true
        } else {
            false
        }
    }
}
