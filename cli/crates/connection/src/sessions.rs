//! The Session binding owners as the Connection owner reaches them: one per
//! `session_id`, created when the first `session.prepare` of its Session is
//! routed, which also creates its Session record; the record counts until
//! its owner reports it gone. At most `RECORDS_MAX` records exist. A
//! `session.close` of a Session without an owner creates one that holds no
//! record; it ends once its reports have their outcomes and it has taken
//! every instruction routed to it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use protocol::connection::LostChange;
use protocol::id::{Id, SafeId};
use protocol::local::SessionEntry;
use protocol::ports::{
    Close, ConnectionPort, Drives, KeptReply, Prepare, ProbeRequest, ProbeSlot, ReportSink,
    SessionFactory, SessionHandle, TicketAnswer, TicketRequest, ToolCall,
};
use tokio::sync::mpsc;

use crate::owner::Message;
use crate::probe_slots::Requester;

/// The Session records that may exist at once.
pub const RECORDS_MAX: usize = 128;

/// A call of a Session binding owner on the Connection owner.
#[derive(Debug)]
pub(crate) enum PortMessage {
    Slot(Requester),
    Withdraw(Requester),
    Free,
    GitTicket {
        session_id: SafeId,
        request: TicketRequest,
        connection_id: Id,
        binding_id: Id,
    },
    RecordGone(SafeId),
    TasksGone(SafeId),
    TaskStaged(Duration),
    TaskUnstaged(Duration),
    TaskStarted(Option<Duration>),
    TaskEnded,
    TaskRefused,
    KeepLostChanges(Vec<LostChange>, Kept),
    OwnerIdle(SafeId, u64),
}

/// The reply of a `KeepLostChanges`.
pub(crate) struct Kept(pub KeptReply);

impl std::fmt::Debug for Kept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("kept reply")
    }
}

/// The Connection owner's side of `ConnectionPort`: each call is a message
/// in its mailbox.
pub(crate) struct Port {
    pub mailbox: mpsc::UnboundedSender<Message>,
}

impl Port {
    fn send(&self, message: PortMessage) {
        let _ = self.mailbox.send(Message::Port(message));
    }
}

impl ConnectionPort for Port {
    fn probe_slot(&self, session_id: &SafeId, request: ProbeRequest) {
        self.send(PortMessage::Slot((session_id.clone(), request)));
    }

    fn probe_slot_withdraw(&self, session_id: &SafeId, request: ProbeRequest) {
        self.send(PortMessage::Withdraw((session_id.clone(), request)));
    }

    fn probe_slot_free(&self) {
        self.send(PortMessage::Free);
    }

    fn fetch_git_ticket(
        &self,
        session_id: &SafeId,
        request: TicketRequest,
        connection_id: &Id,
        binding_id: &Id,
    ) {
        self.send(PortMessage::GitTicket {
            session_id: session_id.clone(),
            request,
            connection_id: connection_id.clone(),
            binding_id: binding_id.clone(),
        });
    }

    fn record_gone(&self, session_id: &SafeId) {
        self.send(PortMessage::RecordGone(session_id.clone()));
    }

    fn tasks_gone(&self, session_id: &SafeId) {
        self.send(PortMessage::TasksGone(session_id.clone()));
    }

    fn task_staged(&self, staged: Duration) {
        self.send(PortMessage::TaskStaged(staged));
    }

    fn task_unstaged(&self, staged: Duration) {
        self.send(PortMessage::TaskUnstaged(staged));
    }

    fn task_started(&self, staged: Option<Duration>) {
        self.send(PortMessage::TaskStarted(staged));
    }

    fn task_ended(&self) {
        self.send(PortMessage::TaskEnded);
    }

    fn task_refused(&self) {
        self.send(PortMessage::TaskRefused);
    }

    fn keep_lost_changes(&self, entries: Vec<LostChange>, kept: KeptReply) {
        self.send(PortMessage::KeepLostChanges(entries, Kept(kept)));
    }

    fn owner_idle(&self, session_id: &SafeId, received: u64) {
        self.send(PortMessage::OwnerIdle(session_id.clone(), received));
    }
}

/// What `GET /v1/sessions` is read from: the owners, which its handler asks
/// itself, and the preparations still staged.
pub(crate) struct SessionsView {
    pub owners: Vec<Arc<dyn SessionHandle>>,
    pub staged: Vec<SessionEntry>,
}

struct Owned {
    handle: Arc<dyn SessionHandle>,
    record: bool,
    /// The instructions routed to it: each `session.prepare`, `session.close`,
    /// `session.drives` and tool instruction.
    routed: u64,
}

pub(crate) struct Owners {
    factory: Arc<dyn SessionFactory>,
    port: Arc<dyn ConnectionPort>,
    reports: Arc<dyn ReportSink>,
    records_max: usize,
    owners: BTreeMap<SafeId, Owned>,
}

impl Owners {
    pub fn new(
        factory: Arc<dyn SessionFactory>,
        port: Arc<dyn ConnectionPort>,
        reports: Arc<dyn ReportSink>,
        records_max: usize,
    ) -> Owners {
        Owners {
            factory,
            port,
            reports,
            records_max,
            owners: BTreeMap::new(),
        }
    }

    /// Whether `session_id` has no record while `records_max` exist.
    pub fn full_for(&self, session_id: &SafeId) -> bool {
        let records = self.owners.values().filter(|owned| owned.record).count();
        !self
            .owners
            .get(session_id)
            .is_some_and(|owned| owned.record)
            && records >= self.records_max
    }

    /// Hands `prepare` to its Session's owner, created when the Session has
    /// none; the owner creates the record as it handles it.
    pub fn prepare(&mut self, session_id: SafeId, prepare: Prepare) {
        let owned = self
            .owners
            .entry(session_id.clone())
            .or_insert_with(|| Owned {
                handle: self
                    .factory
                    .create(session_id, self.port.clone(), self.reports.clone()),
                record: false,
                routed: 0,
            });
        owned.record = true;
        owned.routed += 1;
        owned.handle.prepare(prepare);
    }

    /// Hands `close` to its Session's owner. A Session without one gets an
    /// owner that holds no record and counts against no limit, which runs
    /// the close as a late close.
    pub fn close(&mut self, session_id: &SafeId, close: Close) {
        let owned = self
            .owners
            .entry(session_id.clone())
            .or_insert_with(|| Owned {
                handle: self.factory.create(
                    session_id.clone(),
                    self.port.clone(),
                    self.reports.clone(),
                ),
                record: false,
                routed: 0,
            });
        owned.routed += 1;
        owned.handle.close(close);
    }

    /// Hands `drives` to its Session's owner, when the Session has one.
    pub fn drives(&mut self, session_id: &SafeId, drives: Drives) {
        if let Some(owned) = self.owners.get_mut(session_id) {
            owned.routed += 1;
            owned.handle.drives(drives);
        }
    }

    /// Ends the owner of `session_id` when it holds no record and has taken
    /// every instruction routed to it (`received`); otherwise it handles
    /// the later ones first.
    pub fn idle(&mut self, session_id: &SafeId, received: u64) {
        let ends = self
            .owners
            .get(session_id)
            .is_some_and(|owned| !owned.record && owned.routed == received);
        if ends && let Some(owned) = self.owners.remove(session_id) {
            owned.handle.end();
        }
    }

    /// Whether the Session `session_id` has an owner here, which then
    /// admits its tool instructions.
    pub fn owns(&self, session_id: &str) -> bool {
        SafeId::new(session_id)
            .ok()
            .is_some_and(|session_id| self.owners.contains_key(&session_id))
    }

    /// Hands `call` to its Session's owner, or, for a Session without one,
    /// to the factory, which rejects it; it never creates an owner or a
    /// record.
    pub fn tool(&mut self, call: ToolCall) {
        let owned = SafeId::new(call.session_id.as_str())
            .ok()
            .and_then(|session_id| self.owners.get_mut(&session_id));
        match owned {
            Some(owned) => {
                owned.routed += 1;
                owned.handle.tool(call);
            }
            None => self.factory.unbound_tool(call, self.reports.clone()),
        }
    }

    pub fn answer(&self, (session_id, request): &Requester, answer: ProbeSlot) {
        if let Some(owned) = self.owners.get(session_id) {
            owned.handle.probe_slot(*request, answer);
        }
    }

    pub fn git_ticket(&self, session_id: &SafeId, request: TicketRequest, answer: TicketAnswer) {
        if let Some(owned) = self.owners.get(session_id) {
            owned.handle.git_ticket(request, answer);
        }
    }

    pub fn connection_ended(&self, connection_id: &Id) {
        for owned in self.owners.values() {
            owned.handle.connection_ended(connection_id.clone());
        }
    }

    /// Has every owner end its tasks, and gives the Sessions whose tasks
    /// are not yet confirmed gone.
    pub fn shutdown(&self) -> BTreeSet<SafeId> {
        for owned in self.owners.values() {
            owned.handle.shutdown();
        }
        self.owners.keys().cloned().collect()
    }

    pub fn record_gone(&mut self, session_id: &SafeId) {
        if let Some(owned) = self.owners.get_mut(session_id) {
            owned.record = false;
        }
    }

    pub fn handles(&self) -> Vec<Arc<dyn SessionHandle>> {
        self.owners
            .values()
            .map(|owned| owned.handle.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use protocol::ports::{EntriesReply, ReportId, Submission, TicketAnswer};

    use super::*;

    /// Records each call its owners take, by name.
    #[derive(Default)]
    struct Calls(Mutex<Vec<&'static str>>);

    struct Recording(Arc<Calls>);

    impl SessionHandle for Recording {
        fn prepare(&self, _: Prepare) {
            self.0.0.lock().unwrap().push("prepare");
        }
        fn close(&self, _: Close) {
            self.0.0.lock().unwrap().push("close");
        }
        fn drives(&self, _: Drives) {}
        fn tool(&self, _: ToolCall) {}
        fn probe_slot(&self, _: ProbeRequest, _: ProbeSlot) {}
        fn git_ticket(&self, _: TicketRequest, _: TicketAnswer) {}
        fn connection_ended(&self, _: Id) {}
        fn shutdown(&self) {}
        fn entries(&self, reply: EntriesReply) {
            reply(Vec::new());
        }
        fn end(&self) {
            self.0.0.lock().unwrap().push("end");
        }
    }

    struct Factory(Arc<Calls>);

    impl SessionFactory for Factory {
        fn create(
            &self,
            _: SafeId,
            _: Arc<dyn ConnectionPort>,
            _: Arc<dyn ReportSink>,
        ) -> Arc<dyn SessionHandle> {
            Arc::new(Recording(self.0.clone()))
        }
        fn unbound_tool(&self, _: ToolCall, _: Arc<dyn ReportSink>) {}
    }

    struct NoReports;

    impl ReportSink for NoReports {
        fn submit(&self, _: Submission) -> ReportId {
            ReportId(0)
        }
        fn cancel(&self, _: ReportId) {}
        fn add_final(&self, _: ReportId, _: Id) {}
    }

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }

    fn close() -> Close {
        Close {
            connection_id: id("conn_1"),
            message_id: id("msg_close"),
            binding_id: id("bind_1"),
        }
    }

    #[test]
    fn an_owner_without_a_record_ends_only_once_it_has_taken_every_routed_instruction() {
        let calls = Arc::new(Calls::default());
        let (mailbox, _messages) = mpsc::unbounded_channel();
        let mut owners = Owners::new(
            Arc::new(Factory(calls.clone())),
            Arc::new(Port { mailbox }),
            Arc::new(NoReports),
            RECORDS_MAX,
        );
        let session = SafeId::new("sess_1").unwrap();
        owners.close(&session, close());
        owners.close(&session, close());
        // It asked after taking the first close only.
        owners.idle(&session, 1);
        assert!(owners.owns("sess_1"));
        owners.idle(&session, 2);
        assert!(!owners.owns("sess_1"));
        assert_eq!(*calls.0.lock().unwrap(), ["close", "close", "end"]);

        // A preparation routed after its close makes it count as a record.
        let other = SafeId::new("sess_2").unwrap();
        owners.close(&other, close());
        let payload = serde_json::json!({
            "session_id": "sess_2", "binding_id": "bind_1", "cwd": "/project",
            "drives": [], "drive_exclude": [], "repositories": [],
        });
        owners.prepare(
            other.clone(),
            Prepare {
                connection_id: id("conn_1"),
                message_id: id("msg_prepare"),
                addressed_connection_id: id("conn_1"),
                payload: serde_json::from_value(payload).unwrap(),
                fingerprint: "a".repeat(64),
                traceparent: None,
            },
        );
        owners.idle(&other, 2);
        assert!(owners.owns("sess_2"));
    }
}
