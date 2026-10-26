//! The Session binding owners as the Connection owner reaches them: one per
//! `session_id`, created when the first `session.prepare` of its Session is
//! routed, which also creates its Session record; the record counts until
//! its owner reports it gone. At most `RECORDS_MAX` records exist.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use protocol::id::{Id, SafeId};
use protocol::local::SessionEntry;
use protocol::ports::{
    Close, ConnectionPort, Prepare, ProbeRequest, ProbeSlot, ReportSink, SessionFactory,
    SessionHandle, ToolCall,
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
    RecordGone(SafeId),
    TasksGone(SafeId),
    TaskStaged(Duration),
    TaskUnstaged(Duration),
    TaskStarted(Option<Duration>),
    TaskEnded,
    TaskRefused,
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
            });
        owned.record = true;
        owned.handle.prepare(prepare);
    }

    /// Hands `close` to its Session's owner, when the Session has one.
    pub fn close(&self, session_id: &SafeId, close: Close) {
        if let Some(owned) = self.owners.get(session_id) {
            owned.handle.close(close);
        }
    }

    /// Hands `call` to its Session's owner, or, for a Session without one,
    /// to the factory, which rejects it; it never creates an owner or a
    /// record.
    /// Whether the Session `session_id` has an owner here, which then
    /// admits its tool instructions.
    pub fn owns(&self, session_id: &str) -> bool {
        SafeId::new(session_id)
            .ok()
            .is_some_and(|session_id| self.owners.contains_key(&session_id))
    }

    pub fn tool(&self, call: ToolCall) {
        let owned = SafeId::new(call.session_id.as_str())
            .ok()
            .and_then(|session_id| self.owners.get(&session_id));
        match owned {
            Some(owned) => owned.handle.tool(call),
            None => self.factory.unbound_tool(call, self.reports.clone()),
        }
    }

    pub fn answer(&self, (session_id, request): &Requester, answer: ProbeSlot) {
        if let Some(owned) = self.owners.get(session_id) {
            owned.handle.probe_slot(*request, answer);
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
