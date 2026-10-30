//! The steps of a `session.prepare` in its Session binding owner: the
//! connection it names, an earlier execution of the Session still being
//! cleaned up, the cases against the Session's binding, the state
//! root checked lexically without touching the target, a probe slot, the
//! probe, the repository paths' checks, the mounts of the bound drives
//! (`drives`), the git ticket and the clones (`repositories`), the commit
//! of its result while the connection, binding and operation are all still
//! current, the report and, on its ACK, `prepared`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use observe::{Identity, TraceContext};
use protocol::connection::{
    MESSAGE_MAX_BYTES, PreparationError, PreparationErrorCode, RepositoryFailure, SessionOutcome,
    SessionStatus,
};
use protocol::field::{AbsolutePath, List};
use protocol::id::Id;
use protocol::local::SessionState;
use protocol::ports::{
    Prepare, ProbeRequest, ProbeSlot, ReportId, ReportOutcome, Submission, TicketAnswer,
    TicketRequest,
};

use crate::cases::{self, Case};
use crate::owner::{ReportEvents, Running, SessionMessage, SessionOwner, TaskStarter};
use crate::probe;
use crate::probe_child::{Input, Output, RepositoryCheck, lexical};
use crate::record::{Binding, Record, Requester};
use crate::repositories::{self, Job};

/// The whole message of a probe that ends without a result line: no
/// operating system error caused it, and the fixed text of
/// `cwd_inaccessible` would report a crash as denied access.
pub(crate) const PROBE_ENDED_WITHOUT_RESULT: &str = "the directory probe ended without a result";

impl<S: TaskStarter> SessionOwner<S> {
    pub(crate) fn prepare(&mut self, prepare: Prepare) {
        let Some(preparation) = &self.preparation else {
            return;
        };
        let trace = prepare.traceparent.as_deref().and_then(TraceContext::parse);
        let session_id = preparation.session_id.to_id();
        let _span = observe::correlation_span(
            &[
                (Identity::Session, session_id.as_str()),
                (Identity::Binding, prepare.payload.binding_id.as_str()),
                (Identity::Connection, prepare.connection_id.as_str()),
                (Identity::Message, prepare.message_id.as_str()),
            ],
            trace.as_ref(),
        )
        .entered();
        let cleanup_pending = self.cleanup_pending();
        let record = self.record.get_or_insert_with(Record::default);
        let requester = Requester {
            message_id: prepare.message_id.clone(),
            traceparent: trace.map(|trace| trace.header_value().to_owned()),
        };
        if prepare.addressed_connection_id != prepare.connection_id {
            self.report_failure(&prepare, &requester, PreparationErrorCode::StaleConnection);
            return;
        }
        if cleanup_pending {
            self.report_failure(&prepare, &requester, PreparationErrorCode::CleanupPending);
            return;
        }
        let binding_id = prepare.payload.binding_id.as_str();
        match cases::decide(record.current.as_ref(), binding_id, &prepare.fingerprint) {
            Case::Conflict => {
                self.report_failure(&prepare, &requester, PreparationErrorCode::BindingConflict);
            }
            Case::Repeat => self.repeat(&prepare, requester),
            Case::New | Case::Replace => self.start(&prepare, requester),
        }
    }

    /// The same binding and payload again: it joins the preparation that
    /// still runs, or gets the saved result in a report of its own.
    fn repeat(&mut self, prepare: &Prepare, requester: Requester) {
        let Some(binding) = self.current_binding() else {
            return;
        };
        if binding.state == SessionState::Preparing {
            binding.waiting.push(requester);
            return;
        }
        if let Some(outcome) = saved_outcome(binding) {
            self.submit(prepare.connection_id.clone(), &requester, outcome);
        }
    }

    /// A new binding, which replaces a failed one: the state root checked
    /// lexically, then a probe slot asked for.
    fn start(&mut self, prepare: &Prepare, requester: Requester) {
        self.last_operation += 1;
        let operation = self.last_operation;
        let binding = Binding {
            binding_id: prepare.payload.binding_id.clone(),
            connection_id: prepare.connection_id.clone(),
            requested_cwd: prepare.payload.cwd.clone(),
            create_cwd: prepare.payload.create_cwd == Some(true),
            cwd: None,
            state: SessionState::Preparing,
            error: None,
            fingerprint: prepare.fingerprint.clone(),
            operation,
            waiting: vec![requester],
            reports: Vec::new(),
            repositories: prepare.payload.repositories.clone(),
            repository_failures: Vec::new(),
            drives: prepare.payload.drives.clone(),
            drive_exclude: prepare.payload.drive_exclude.clone(),
            drive_mount_failures: Vec::new(),
            tasks: Default::default(),
            applying: None,
        };
        let record = self.record.get_or_insert_with(Record::default);
        record.current = Some(binding);
        record.unavailable = None;
        let preparation = self.preparation.as_ref().expect("a preparing owner");
        let cwd = Path::new(prepare.payload.cwd.as_str());
        if inside_state_root(cwd, &preparation.shared.paths.given)
            || inside_state_root(cwd, &preparation.shared.paths.canonical)
        {
            self.fail(
                operation,
                PreparationError::new(PreparationErrorCode::CwdForbidden, None),
            );
            return;
        }
        self.last_request += 1;
        let request = ProbeRequest(self.last_request);
        self.requests.insert(request, operation);
        preparation
            .connection
            .probe_slot(&preparation.session_id, request);
    }

    pub(crate) fn probe_slot(&mut self, request: ProbeRequest, answer: ProbeSlot) {
        let Some(preparation) = &self.preparation else {
            return;
        };
        let operation = self
            .requests
            .remove(&request)
            .filter(|operation| self.is_preparing(*operation));
        let Some(operation) = operation else {
            if answer == ProbeSlot::Granted {
                preparation.connection.probe_slot_free();
            }
            return;
        };
        match answer {
            ProbeSlot::Granted => self.spawn_probe(operation),
            ProbeSlot::Exhausted => self.fail(
                operation,
                PreparationError::new(PreparationErrorCode::ResourceExhausted, None),
            ),
            // The connection's end or the shutdown, which end the binding
            // themselves.
            ProbeSlot::Cancelled => {}
        }
    }

    fn spawn_probe(&mut self, operation: u64) {
        let Some(binding) = self
            .record
            .as_ref()
            .and_then(|record| record.current.as_ref())
        else {
            return;
        };
        let preparation = self.preparation.as_ref().expect("a preparing owner");
        let input = Input {
            cwd: binding.requested_cwd.as_str().into(),
            create_cwd: binding.create_cwd,
            state_root: preparation.shared.paths.canonical_utf8.clone(),
            repository_paths: binding
                .repositories
                .iter()
                .map(|repository| repository.path.as_str().to_owned())
                .collect(),
        };
        let mailbox = self.mailbox.clone();
        let done = move |output| {
            let _ = mailbox.send(SessionMessage::ProbeDone { operation, output });
        };
        let program = &preparation.shared.paths.probe_program;
        match probe::spawn(program, &input, probe::OUTPUT_MAX_BYTES, done) {
            Ok(probe) => {
                let deadline = state::seams::now() + preparation.shared.times.probe_limit;
                self.probes.insert(
                    operation,
                    Running {
                        probe,
                        deadline,
                        killed: false,
                    },
                );
            }
            Err(error) => {
                preparation.connection.probe_slot_free();
                self.fail(
                    operation,
                    PreparationError::new(
                        PreparationErrorCode::ResourceExhausted,
                        Some(&error.to_string()),
                    ),
                );
            }
        }
    }

    /// The probe of `operation` is reaped: its slot goes back, and its
    /// result counts only while its preparation still waits for it.
    pub(crate) fn probe_done(&mut self, operation: u64, output: Option<Output>) {
        let Some(running) = self.probes.remove(&operation) else {
            return;
        };
        if let Some(preparation) = &self.preparation {
            preparation.connection.probe_slot_free();
        }
        if running.killed || !self.is_preparing(operation) {
            return;
        }
        match output {
            Some(Output::Entered { cwd, repositories }) => match AbsolutePath::new(cwd) {
                Ok(cwd) => self.probed(operation, cwd, &repositories),
                Err(error) => self.fail(
                    operation,
                    PreparationError::new(
                        PreparationErrorCode::CwdInaccessible,
                        Some(&error.to_string()),
                    ),
                ),
            },
            Some(Output::Failed { error }) => self.fail(operation, error),
            None => self.fail(
                operation,
                PreparationError {
                    code: PreparationErrorCode::CwdInaccessible,
                    message: PROBE_ENDED_WITHOUT_RESULT.to_owned(),
                },
            ),
        }
    }

    pub(crate) fn probe_timed_out(&mut self, operation: u64) {
        if self.is_preparing(operation) {
            self.fail(
                operation,
                PreparationError::new(PreparationErrorCode::CwdCheckTimeout, None),
            );
        }
    }

    /// The probe of `operation` found its canonical `cwd`. A repository path
    /// in the state root, lexically or resolved, then one where an entry
    /// that this Session did not clone exists, fails the preparation;
    /// otherwise its bound drives are mounted (`drives`), then its
    /// repositories are cloned once a git ticket comes.
    fn probed(&mut self, operation: u64, cwd: AbsolutePath, checks: &[RepositoryCheck]) {
        let (Some(preparation), Some(binding)) = (
            &self.preparation,
            self.record
                .as_ref()
                .and_then(|record| record.current.as_ref())
                .filter(|binding| binding.operation == operation),
        ) else {
            return;
        };
        let paths = &preparation.shared.paths;
        let pairs = || binding.repositories.iter().zip(checks);
        let failure = if checks.len() != binding.repositories.len() {
            Some(PreparationError {
                code: PreparationErrorCode::CwdInaccessible,
                message: PROBE_ENDED_WITHOUT_RESULT.to_owned(),
            })
        } else if pairs().any(|(repository, check)| {
            let path = Path::new(repository.path.as_str());
            check.inside_state_root
                || inside_state_root(path, &paths.given)
                || inside_state_root(path, &paths.canonical)
        }) {
            Some(PreparationError::new(
                PreparationErrorCode::RepositoryPathForbidden,
                None,
            ))
        } else if checks.iter().any(|check| check.exists) {
            let listed = repositories::listed(&state::paths::session_directory(
                &paths.canonical,
                preparation.session_id.as_str(),
            ));
            pairs()
                .any(|(repository, check)| {
                    check.exists && !listed.iter().any(|path| path == repository.path.as_str())
                })
                .then(|| PreparationError::new(PreparationErrorCode::RepositoryPathExists, None))
        } else {
            None
        };
        if let Some(error) = failure {
            return self.fail(operation, error);
        }
        self.mount_drives(operation, cwd);
    }

    /// After the mounts of `operation`: its repositories are cloned once a
    /// git ticket comes.
    pub(crate) fn clone_repositories(&mut self, operation: u64, cwd: AbsolutePath) {
        let (Some(preparation), Some(binding)) = (
            &self.preparation,
            self.record
                .as_ref()
                .and_then(|record| record.current.as_ref())
                .filter(|binding| binding.operation == operation),
        ) else {
            return;
        };
        if binding.repositories.is_empty() {
            return self.prepared(operation, cwd, Vec::new());
        }
        self.last_request += 1;
        let request = TicketRequest(self.last_request);
        self.tickets.insert(request, operation);
        preparation.connection.fetch_git_ticket(
            &preparation.session_id,
            request,
            &binding.connection_id,
            &binding.binding_id,
        );
        if let Some(binding) = self.current_binding() {
            binding.cwd = Some(cwd);
        }
    }

    /// The git ticket for `request`: the repositories are cloned with it, or
    /// all fail with cause `clone` and the fetch's message.
    pub(crate) fn git_ticket(&mut self, request: TicketRequest, answer: TicketAnswer) {
        let Some(operation) = self
            .tickets
            .remove(&request)
            .filter(|operation| self.is_preparing(*operation))
        else {
            return;
        };
        let Some(preparation) = &self.preparation else {
            return;
        };
        let session_directory = state::paths::session_directory(
            &preparation.shared.paths.canonical,
            preparation.session_id.as_str(),
        );
        let Some(binding) = self.current_binding() else {
            return;
        };
        let repositories = binding.repositories.clone();
        let cwd = binding.cwd.clone().expect("a probed binding has its cwd");
        let ticket = match answer {
            Ok(ticket) => ticket,
            Err(message) => {
                let failures = repositories::ticket_failures(&repositories, &message);
                return self.prepared(operation, cwd, failures);
            }
        };
        let current = Arc::new(AtomicBool::new(true));
        let (kill, killed) = tokio::sync::watch::channel(false);
        self.clones.insert(
            operation,
            repositories::Running {
                current: current.clone(),
                kill,
            },
        );
        let mailbox = self.mailbox.clone();
        tokio::spawn(async move {
            let failures = repositories::run(Job {
                session_directory,
                repositories,
                ticket,
                current,
                killed,
            })
            .await;
            let _ = mailbox.send(SessionMessage::RepositoriesDone {
                operation,
                failures,
            });
        });
    }

    /// The clones of `operation` ended with `failures`; they count only
    /// while its preparation is still current.
    pub(crate) fn repositories_done(&mut self, operation: u64, failures: Vec<RepositoryFailure>) {
        self.clones.remove(&operation);
        if !self.is_preparing(operation) {
            return;
        }
        let Some(cwd) = self
            .current_binding()
            .and_then(|binding| binding.cwd.clone())
        else {
            return;
        };
        self.prepared(operation, cwd, failures);
    }

    /// The preparation of `operation` found its canonical `cwd`, which becomes
    /// the binding's default directory: each waiting instruction gets its
    /// report, and the binding waits for an ACK.
    fn prepared(&mut self, operation: u64, cwd: AbsolutePath, failures: Vec<RepositoryFailure>) {
        let Some(binding) = self.current_binding().filter(|b| b.operation == operation) else {
            return;
        };
        binding.cwd = Some(cwd);
        binding.repository_failures = failures;
        binding.state = SessionState::PreparedPendingAck;
        let mount_failures = binding.drive_mount_failures.clone();
        self.mount_failures_reported(&mount_failures);
        self.report_waiting();
    }

    fn fail(&mut self, operation: u64, error: PreparationError) {
        let Some(binding) = self.current_binding().filter(|b| b.operation == operation) else {
            return;
        };
        binding.state = SessionState::Failed;
        binding.error = Some(error);
        self.report_waiting();
    }

    /// Reports the binding's result to every instruction that waits for it.
    fn report_waiting(&mut self) {
        let Some(binding) = self.current_binding() else {
            return;
        };
        let waiting = std::mem::take(&mut binding.waiting);
        let Some(outcome) = saved_outcome(binding) else {
            return;
        };
        let connection = binding.connection_id.clone();
        for requester in waiting {
            self.submit(connection.clone(), &requester, outcome.clone());
        }
    }

    /// The ACK of a report of the binding's result enters `prepared`.
    pub(crate) fn report_outcome(&mut self, report: ReportId, outcome: ReportOutcome) {
        let Some(binding) = self.current_binding() else {
            return;
        };
        let Some(index) = binding.reports.iter().position(|each| *each == report) else {
            return;
        };
        binding.reports.remove(index);
        if outcome == ReportOutcome::Acked && binding.state == SessionState::PreparedPendingAck {
            binding.state = SessionState::Prepared;
            self.drive_binding_current();
            self.start_staged();
        }
    }

    /// A failed report for `prepare` that changes no binding.
    fn report_failure(
        &mut self,
        prepare: &Prepare,
        requester: &Requester,
        code: PreparationErrorCode,
    ) {
        let outcome = SessionOutcome::Failed {
            error: PreparationError::new(code, None),
        };
        let report = self.build(
            prepare.connection_id.clone(),
            prepare.payload.binding_id.clone(),
            requester,
            outcome,
        );
        if let Some(preparation) = &self.preparation {
            preparation.reports.submit(report);
        }
    }

    /// Submits a report of the current binding's result, whose ACK may enter
    /// `prepared`.
    fn submit(&mut self, connection: Id, requester: &Requester, outcome: SessionOutcome) {
        let Some(binding_id) = self
            .current_binding()
            .map(|binding| binding.binding_id.clone())
        else {
            return;
        };
        let report = self.build(connection, binding_id, requester, outcome);
        let Some(preparation) = &self.preparation else {
            return;
        };
        let id = preparation.reports.submit(report);
        if let Some(binding) = self.current_binding() {
            binding.reports.push(id);
        }
    }

    fn build(
        &self,
        connection: Id,
        binding_id: Id,
        requester: &Requester,
        outcome: SessionOutcome,
    ) -> Submission {
        let preparation = self.preparation.as_ref().expect("a preparing owner");
        let session_id = preparation.session_id.to_id();
        let request_message_id = requester.message_id.clone();
        let mut submission = Submission::new(connection, move |connection_id, message_id| {
            fitted(SessionStatus {
                connection_id,
                message_id,
                request_message_id,
                session_id,
                binding_id,
                outcome,
            })
        });
        submission.final_for = vec![requester.message_id.clone()];
        submission.traceparent = requester.traceparent.clone();
        submission.listener = Some(Arc::new(ReportEvents {
            mailbox: self.mailbox.clone(),
        }));
        submission
    }

    pub(crate) fn current_binding(&mut self) -> Option<&mut Binding> {
        self.record.as_mut()?.current.as_mut()
    }

    pub(crate) fn is_preparing(&self, operation: u64) -> bool {
        self.record
            .as_ref()
            .and_then(|record| record.current.as_ref())
            .is_some_and(|binding| {
                binding.operation == operation && binding.state == SessionState::Preparing
            })
    }
}

/// `status` with its repository failures' messages shortened, from the
/// last failure backwards, until the report fits its limit.
fn fitted(mut status: SessionStatus) -> SessionStatus {
    if protocol::encode(&status).len() <= MESSAGE_MAX_BYTES {
        return status;
    }
    let SessionOutcome::Prepared {
        repository_failures: Some(List(failures)),
        ..
    } = &mut status.outcome
    else {
        return status;
    };
    let mut fitting = std::mem::take(failures);
    let mut probe = status.clone();
    repositories::fit(&mut fitting, MESSAGE_MAX_BYTES, |failures| {
        if let SessionOutcome::Prepared {
            repository_failures: Some(List(each)),
            ..
        } = &mut probe.outcome
        {
            failures.clone_into(each);
        }
        protocol::encode(&probe).len()
    });
    if let SessionOutcome::Prepared {
        repository_failures: Some(List(failures)),
        ..
    } = &mut status.outcome
    {
        *failures = fitting;
    }
    status
}

/// The result saved for a binding whose preparation ended, as a report says
/// it again.
fn saved_outcome(binding: &Binding) -> Option<SessionOutcome> {
    match (binding.state, &binding.cwd, &binding.error) {
        (SessionState::PreparedPendingAck | SessionState::Prepared, Some(cwd), _) => {
            Some(SessionOutcome::Prepared {
                cwd: cwd.clone(),
                drive_mount_failures: (!binding.drive_mount_failures.is_empty()).then(|| {
                    List(crate::drives::failures(
                        binding.drive_mount_failures.clone(),
                    ))
                }),
                repository_failures: (!binding.repository_failures.is_empty())
                    .then(|| List(binding.repository_failures.clone())),
            })
        }
        (SessionState::Failed, _, Some(error)) => Some(SessionOutcome::Failed {
            error: error.clone(),
        }),
        _ => None,
    }
}

/// Whether `cwd` is `root` or below it, comparing whole components after
/// the lexical removal of `.` and `..` from both, without reading the
/// filesystem.
pub fn inside_state_root(cwd: &Path, root: &Path) -> bool {
    lexical(cwd).starts_with(lexical(root))
}
