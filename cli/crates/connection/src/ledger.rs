//! The replay ledger: one per connection, the Session instructions that the
//! connection delivered, by `message_id`, so that an instruction Engine sends
//! again starts nothing twice.
//!
//! The ledger keeps [`RESERVE_ENTRIES`] entries and [`RESERVE_BYTES`] bytes
//! free for the work it already admitted: a new operation is recorded only
//! while that reserve stays free after recording it, and everything else,
//! the controls, closes and report identities of admitted work, may use the
//! reserve. Full, it refuses; it never makes room by removing an entry. An
//! entry goes only when its instruction's final report is acknowledged and
//! Engine cannot deliver the instruction again ([`Ledger::final_acked`],
//! [`Ledger::resume_started`]).

use std::collections::HashMap;

use protocol::connection::DownstreamType;
use protocol::id::Id;
use serde::Serialize;

/// The entries a ledger holds at most.
pub const ENTRIES_MAX: usize = 4_096;

/// The bytes a ledger's entries take at most, each counted as its compact
/// JSON encoding.
pub const BYTES_MAX: usize = 16_777_216;

/// The entries a new operation leaves free.
pub const RESERVE_ENTRIES: usize = 64;

/// The bytes a new operation leaves free.
pub const RESERVE_BYTES: usize = 1_048_576;

/// The ledger's bounds, each a constant but for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub entries_max: usize,
    pub bytes_max: usize,
    pub reserve_entries: usize,
    pub reserve_bytes: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            entries_max: ENTRIES_MAX,
            bytes_max: BYTES_MAX,
            reserve_entries: RESERVE_ENTRIES,
            reserve_bytes: RESERVE_BYTES,
        }
    }
}

/// How an instruction may use the ledger's room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// A new operation: a `session.prepare` for a binding no Session record
    /// holds, or a `tool.execute` or `mcp.call` for a task not admitted on
    /// the connection.
    NewOperation,
    /// Anything else, which may use the reserve.
    Reserve,
}

/// One recorded Session instruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entry {
    pub message_id: Id,
    #[serde(rename = "type")]
    pub kind: DownstreamType,
    /// The lowercase hexadecimal SHA-256 of its envelope with object members
    /// sorted by name and no whitespace.
    pub fingerprint: String,
    pub session_id: String,
    pub binding_id: String,
    pub tool_use_event_id: Option<String>,
    pub task_id: Option<String>,
    /// The number of the stream that delivered it last.
    pub stream: u64,
    pub state: EntryState,
    /// The `message_id`s of its reports.
    pub reports: Vec<Id>,
    /// The internal path of its stored body.
    pub body: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryState {
    Open,
    /// Its final report is acknowledged, but a stream before the current
    /// one delivered it.
    Acknowledged,
}

/// What the ledger knows of a `message_id` and fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seen {
    New,
    /// The same instruction again: nothing new starts.
    Duplicate,
    /// The `message_id` of another instruction.
    Conflict,
}

/// The ledger had no room for the entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exhausted;

#[derive(Debug)]
pub struct Ledger {
    limits: Limits,
    entries: HashMap<Id, (Entry, usize)>,
    bytes: usize,
}

impl Ledger {
    pub fn new(limits: Limits) -> Ledger {
        Ledger {
            limits,
            entries: HashMap::new(),
            bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Looks `message_id` up before anything else is done with it; a
    /// duplicate is noted as delivered by `stream`.
    pub fn seen(&mut self, message_id: &Id, fingerprint: &str, stream: u64) -> Seen {
        match self.entries.get_mut(message_id) {
            None => Seen::New,
            Some((entry, _)) if entry.fingerprint == fingerprint => {
                entry.stream = stream;
                Seen::Duplicate
            }
            Some(_) => Seen::Conflict,
        }
    }

    /// Records `entry` when the ledger has room for it as `need` allows,
    /// counting the entry and its size after recording it.
    pub fn record(&mut self, entry: Entry, need: Need) -> Result<(), Exhausted> {
        let size = encoded_size(&entry);
        let (entries_max, bytes_max) = match need {
            Need::NewOperation => (
                self.limits.entries_max - self.limits.reserve_entries,
                self.limits.bytes_max - self.limits.reserve_bytes,
            ),
            Need::Reserve => (self.limits.entries_max, self.limits.bytes_max),
        };
        if self.entries.len() + 1 > entries_max || self.bytes + size > bytes_max {
            return Err(Exhausted);
        }
        self.bytes += size;
        self.entries.insert(entry.message_id.clone(), (entry, size));
        Ok(())
    }

    /// The final report of `message_id` is acknowledged. When the current
    /// stream delivered the instruction, Engine writes it on no later stream
    /// of the connection and the entry goes; otherwise it waits for the next
    /// resume.
    pub fn final_acked(&mut self, message_id: &Id, current_stream: Option<u64>) {
        let Some((entry, _)) = self.entries.get_mut(message_id) else {
            return;
        };
        if Some(entry.stream) == current_stream {
            self.remove(message_id);
        } else {
            entry.state = EntryState::Acknowledged;
        }
    }

    /// A resume attempt starts: Engine resends only the instructions whose
    /// final report it has not acknowledged, so the acknowledged ones go.
    pub fn resume_started(&mut self) {
        let acknowledged: Vec<Id> = self
            .entries
            .values()
            .filter(|(entry, _)| entry.state == EntryState::Acknowledged)
            .map(|(entry, _)| entry.message_id.clone())
            .collect();
        for message_id in acknowledged {
            self.remove(&message_id);
        }
    }

    fn remove(&mut self, message_id: &Id) {
        if let Some((_, size)) = self.entries.remove(message_id) {
            self.bytes -= size;
        }
    }
}

/// An entry's size: the byte length of its compact JSON encoding.
pub fn encoded_size(entry: &Entry) -> usize {
    serde_json::to_vec(entry)
        .expect("an entry always serializes")
        .len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: usize, body: &str) -> Entry {
        Entry {
            message_id: Id::new(format!("msg_{index:05}")).unwrap(),
            kind: DownstreamType::SessionClose,
            fingerprint: "0".repeat(64),
            session_id: "sess_a".to_owned(),
            binding_id: "bind_a".to_owned(),
            tool_use_event_id: None,
            task_id: None,
            stream: 1,
            state: EntryState::Open,
            reports: Vec::new(),
            body: Some(body.to_owned()),
        }
    }

    /// Entry `index` of exactly `size` bytes.
    fn sized(index: usize, size: usize) -> Entry {
        let base = encoded_size(&entry(index, ""));
        let sized = entry(index, &"x".repeat(size - base));
        assert_eq!(encoded_size(&sized), size);
        sized
    }

    /// A ledger holding `count` small entries.
    fn holding(count: usize) -> Ledger {
        let mut ledger = Ledger::new(Limits::default());
        for index in 0..count {
            ledger.record(entry(index, ""), Need::Reserve).unwrap();
        }
        ledger
    }

    /// A ledger whose entries take `bytes`, with entry numbers from 10,000.
    fn taking(bytes: usize) -> Ledger {
        let mut ledger = Ledger::new(Limits::default());
        let piece = 1_000_000;
        let mut left = bytes;
        let mut index = 10_000;
        while left > 0 {
            let size = if left >= 2 * piece { piece } else { left };
            ledger.record(sized(index, size), Need::Reserve).unwrap();
            left -= size;
            index += 1;
        }
        assert_eq!(ledger.bytes(), bytes);
        ledger
    }

    #[test]
    fn a_new_operation_leaves_the_reserve_free_after_recording_and_the_rest_may_use_it() {
        let new = sized(1, 1_000);
        let rows: [(&str, Ledger, Entry, Need, bool); 8] = [
            (
                "4,031 entries, a new operation",
                holding(4_031),
                new.clone(),
                Need::NewOperation,
                true,
            ),
            (
                "4,032 entries, a new operation",
                holding(4_032),
                new.clone(),
                Need::NewOperation,
                false,
            ),
            (
                "15,728,640 bytes after recording a new operation",
                taking(15_728_640 - 1_000),
                new.clone(),
                Need::NewOperation,
                true,
            ),
            (
                "one byte more after recording a new operation",
                taking(15_728_640 - 999),
                new.clone(),
                Need::NewOperation,
                false,
            ),
            (
                "4,095 entries, a control",
                holding(4_095),
                new.clone(),
                Need::Reserve,
                true,
            ),
            (
                "4,096 entries, a control",
                holding(4_096),
                new.clone(),
                Need::Reserve,
                false,
            ),
            (
                "16,777,216 bytes after recording a control",
                taking(16_777_216 - 1_000),
                new.clone(),
                Need::Reserve,
                true,
            ),
            (
                "one byte more after recording a control",
                taking(16_777_216 - 999),
                new.clone(),
                Need::Reserve,
                false,
            ),
        ];
        for (name, mut ledger, entry, need, recorded) in rows {
            let before = (ledger.len(), ledger.bytes());
            let result = ledger.record(entry, need);
            assert_eq!(result.is_ok(), recorded, "{name}");
            if !recorded {
                assert_eq!((ledger.len(), ledger.bytes()), before, "{name}");
            }
        }

        let mut full = holding(4_032);
        let fingerprint = "0".repeat(64);
        assert!(full.record(sized(1, 1_000), Need::NewOperation).is_err());
        assert_eq!(
            full.seen(&entry(7, "").message_id, &fingerprint, 2),
            Seen::Duplicate
        );
        assert_eq!(
            full.seen(&entry(7, "").message_id, &"1".repeat(64), 2),
            Seen::Conflict
        );
        assert_eq!(
            full.seen(&entry(9_999, "").message_id, &fingerprint, 2),
            Seen::New
        );
    }

    #[test]
    fn an_entry_goes_once_its_final_ack_came_and_no_stream_can_deliver_it_again() {
        let id = |index: usize| entry(index, "").message_id;
        let mut ledger = Ledger::new(Limits::default());
        for index in 0..4 {
            let mut each = entry(index, "");
            each.stream = 5;
            ledger.record(each, Need::Reserve).unwrap();
        }
        let mut earlier = entry(4, "");
        earlier.stream = 4;
        ledger.record(earlier, Need::Reserve).unwrap();

        ledger.final_acked(&id(0), Some(5));
        assert_eq!(
            ledger.len(),
            4,
            "acknowledged on the stream that delivered it"
        );
        ledger.final_acked(&id(4), Some(5));
        assert_eq!(ledger.len(), 4, "delivered only on an earlier stream");

        // Stream 5 is lost; entry 1 is acknowledged before the resume
        // attempt starts, entry 2 after it.
        ledger.final_acked(&id(1), None);
        assert_eq!(ledger.len(), 4);
        ledger.resume_started();
        assert_eq!(ledger.len(), 2, "entries 1 and 4 went at the resume");
        ledger.final_acked(&id(2), None);
        assert_eq!(ledger.len(), 2, "acknowledged after the resume started");
        assert_eq!(ledger.seen(&id(3), &"0".repeat(64), 6), Seen::Duplicate);
        ledger.resume_started();
        assert_eq!(ledger.len(), 1, "entry 2 went at the next resume");
        assert_eq!(ledger.seen(&id(2), &"0".repeat(64), 6), Seen::New);
        assert_eq!(ledger.seen(&id(3), &"0".repeat(64), 6), Seen::Duplicate);
    }
}
