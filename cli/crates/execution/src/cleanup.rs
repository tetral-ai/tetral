//! The records of processes that could outlive their App Server. A task's
//! supervisor writes `R/execution/<task name>.json` when the task's child
//! reports its start on descriptor 3, or, without that report, while the
//! task is being ended, and deletes it once the task's processes are
//! confirmed gone; a repository preparation does the same for each `git`
//! ([`Recorded`]). The next App Server reads the records an
//! earlier one left before its first connection ([`read`]) and checks them
//! every [`CHECK_INTERVAL`] until each is confirmed ([`Check::run`]); until
//! then the record's Session is `cleanup_pending`. The check is the only
//! writer of which Sessions those are; Session binding owners and the local
//! API read what it last published ([`Leftovers`]).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use state::files::{self, IoFailure};
use tokio::sync::watch;

use crate::process;
use crate::terminate::{Steps, Termination};

/// The directory of the records under the canonical state root.
pub const DIRECTORY: &str = "execution";

/// How often unconfirmed processes are checked again.
pub const CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// A record is a few short fields; a larger file is no record.
pub const RECORD_MAX_BYTES: usize = 4_096;

/// The interval and size of the records' check, each a constant but for
/// tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub check_interval: Duration,
    pub record_max_bytes: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            check_interval: CHECK_INTERVAL,
            record_max_bytes: RECORD_MAX_BYTES,
        }
    }
}

#[cfg(target_os = "linux")]
const PLATFORM: &str = "linux";
#[cfg(not(target_os = "linux"))]
const PLATFORM: &str = std::env::consts::OS;

/// One task's processes as its record names them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub session_id: String,
    pub platform: String,
    pub boot_id: String,
    /// The host process ID of the task's first process inside its sandbox.
    pub pid: u32,
    pub start_time: u64,
    /// The task's PID namespace on Linux; its process group on macOS
    /// ([`process::Leader`]).
    pub pid_namespace: u64,
}

impl Record {
    /// The record of the task of `session_id` whose first process is
    /// `leader`, on this boot.
    pub fn new(session_id: &str, boot_id: &str, leader: process::Leader) -> Record {
        Record {
            session_id: session_id.to_owned(),
            platform: PLATFORM.to_owned(),
            boot_id: boot_id.to_owned(),
            pid: leader.pid,
            start_time: leader.start_time,
            pid_namespace: leader.namespace,
        }
    }

    /// Writes the record to `path`, mode `0600`, replacing any there.
    pub fn write(&self, path: &Path) -> Result<(), IoFailure> {
        let bytes = serde_json::to_vec(self).expect("a record always encodes");
        files::replace(path, &bytes)
    }
}

/// The record path of the task named `task_name` under the canonical state
/// root `state_root`.
pub fn record_path(state_root: &Path, task_name: &str) -> PathBuf {
    state_root.join(DIRECTORY).join(format!("{task_name}.json"))
}

/// Deletes the record at `path`, where it exists.
pub fn remove(path: &Path) {
    let _ = std::fs::remove_file(path);
}

/// The record of a launched process that no task owns, a repository
/// preparation's `git`, in the same form as a task's: written at its entry's
/// ready report, before the start pipe's byte, and deleted once its
/// processes are confirmed gone, so that a start after an abnormal exit ends
/// them as it ends a task's. One that cannot be written is logged as a
/// task's is.
#[derive(Debug)]
pub struct Recorded {
    path: PathBuf,
    namespace: u64,
    /// Held open until confirmation, as a task's is
    /// ([`process::hold_namespace`]).
    _namespace: Option<std::fs::File>,
}

impl Recorded {
    /// Writes, under the canonical state root `state_root` and with a new
    /// task name, the record of the Session `session_id`'s process launched
    /// as `launched`; nothing, logged as `task.record_failed`, when the boot
    /// ID cannot be read (with its error), that launch's first process is not
    /// found (`error.os_code` 0) or the write fails.
    pub fn write(state_root: &Path, session_id: &str, launched: u32) -> Option<Recorded> {
        let leader = process::boot_id()
            .map_err(|error| process::boot_id_unread(&error))
            .and_then(|boot_id| {
                let not_found = || (0, "the launch's first process was not found".to_owned());
                Ok((boot_id, process::leader(launched).ok_or_else(not_found)?))
            });
        let written = leader.and_then(|(boot_id, leader)| {
            let held = process::hold_namespace(leader.pid, leader.namespace);
            let path = record_path(state_root, &crate::store::task_name());
            Record::new(session_id, &boot_id, leader)
                .write(&path)
                .map_err(|failure| {
                    (
                        failure.source.raw_os_error().unwrap_or(0),
                        failure.to_string(),
                    )
                })?;
            Ok(Recorded {
                path,
                namespace: leader.namespace,
                _namespace: held,
            })
        });
        if let Err((os_code, cause)) = &written {
            observe::warn!(
                "task.record_failed",
                error.os_code = *os_code,
                error.code = observe::Code("task_record_failed"),
                error.message = observe::Cause::system(cause)
            );
        }
        written.ok()
    }

    /// After the exit of the recorded launch's process `launched`: ends the
    /// rest of its processes as a task's exit does, on macOS with the
    /// termination procedure on its process group, while on Linux the
    /// kernel ended its PID namespace with its first process; then waits
    /// until none is left, checking at once, then every [`CHECK_INTERVAL`]
    /// and each time its wait reads the clock, and deletes the record. The
    /// reading at each clock reading matters because the processes can end
    /// a moment after `launched` is reaped: on macOS a member ends only once
    /// the termination's signal reaches it, and on Linux, after a kill, the
    /// kernel ends the namespace's processes after the launched one; under
    /// the test harness the clock moves only when a test advances it.
    pub async fn confirm(self, launched: u32) {
        let mut ending: Option<Termination> = None;
        while !process::namespace_empty(self.namespace) {
            if process::MEMBERS_OUTLIVE_THE_LEADER && ending.is_none() {
                ending = Some(Termination::begin(launched, Steps::default()));
            }
            let grace = ending.filter(|termination| !termination.killed());
            let check = state::seams::now() + CHECK_INTERVAL;
            if self
                .emptied_before(grace.map_or(check, |termination| check.min(termination.due())))
                .await
            {
                break;
            }
            if let Some(termination) =
                grace.filter(|termination| state::seams::now() >= termination.due())
            {
                ending = termination.step();
            }
        }
        remove(&self.path);
    }

    /// Waits until `deadline` on the CLI's clock as
    /// [`state::seams::sleep_until`] does, reading the recorded processes
    /// again each time it reads the clock; gives whether none was left
    /// before the deadline.
    async fn emptied_before(&self, deadline: Duration) -> bool {
        loop {
            let remaining = deadline.saturating_sub(state::seams::now());
            if remaining.is_zero() {
                return false;
            }
            tokio::time::sleep(remaining.min(state::seams::recheck_interval())).await;
            if process::namespace_empty(self.namespace) {
                return true;
            }
        }
    }
}

/// Which Sessions still have a record an earlier App Server left that is
/// not confirmed, as [`Check`], its only writer, last published it.
#[derive(Debug, Clone)]
pub struct Leftovers(watch::Receiver<Arc<BTreeSet<String>>>);

impl Leftovers {
    /// No unconfirmed record.
    pub fn none() -> Leftovers {
        Leftovers(watch::channel(Arc::default()).1)
    }

    /// Whether a record of `session_id` is unconfirmed.
    pub fn pending(&self, session_id: &str) -> bool {
        self.0.borrow().contains(session_id)
    }

    /// Whether any record is unconfirmed.
    pub fn any(&self) -> bool {
        !self.0.borrow().is_empty()
    }
}

/// The unconfirmed records an earlier App Server left, by Session, owned
/// by the task that checks them.
#[derive(Debug)]
pub struct Check {
    state_root: PathBuf,
    check_interval: Duration,
    records: BTreeMap<String, Vec<Leftover>>,
    published: watch::Sender<Arc<BTreeSet<String>>>,
}

#[derive(Debug)]
struct Leftover {
    path: PathBuf,
    record: Record,
    signalled: bool,
}

/// Reads every record in `R/execution/`, creating the directory where it
/// is missing. A record of another boot is confirmed at once and its
/// processes are never signalled: the restart ended every process it
/// names, whose IDs may now belong to others; when it was its Session's
/// last record, the Session's task directories go with it, as
/// [`Check::run`] removes them. A file that is no record names no process
/// and is deleted.
pub fn read(state_root: &Path) -> Result<(Check, Leftovers), IoFailure> {
    read_with(state_root, Limits::default())
}

/// [`read`] with `limits`.
pub fn read_with(state_root: &Path, limits: Limits) -> Result<(Check, Leftovers), IoFailure> {
    let directory = state_root.join(DIRECTORY);
    files::create_private_dirs(&directory)?;
    files::remove_interrupted_replacements(&directory)?;
    let boot_id = process::boot_id().ok();
    let mut records: BTreeMap<String, Vec<Leftover>> = BTreeMap::new();
    let mut confirmed = BTreeSet::new();
    let entries =
        std::fs::read_dir(&directory).map_err(|error| IoFailure::new(&directory, error))?;
    for entry in entries.flatten() {
        let path = entry.path();
        let record = files::read_bounded(&path, limits.record_max_bytes)
            .ok()
            .flatten()
            .filter(|bytes| bytes.len() <= limits.record_max_bytes)
            .and_then(|bytes| serde_json::from_slice::<Record>(&bytes).ok());
        match record {
            Some(record) if Some(&record.boot_id) == boot_id.as_ref() => {
                records
                    .entry(record.session_id.clone())
                    .or_default()
                    .push(Leftover {
                        path,
                        record,
                        signalled: false,
                    });
            }
            Some(record) => {
                remove(&path);
                confirmed.insert(record.session_id);
            }
            None => remove(&path),
        }
    }
    for session_id in confirmed
        .iter()
        .filter(|session_id| !records.contains_key(*session_id))
    {
        crate::store::clear_session_tasks(state_root, session_id);
    }
    let (published, leftovers) = watch::channel(Arc::new(records.keys().cloned().collect()));
    let check = Check {
        state_root: state_root.to_path_buf(),
        check_interval: limits.check_interval,
        records,
        published,
    };
    Ok((check, Leftovers(leftovers)))
}

impl Check {
    /// Checks the records at once and then every check interval of the
    /// seam clock until each is confirmed.
    pub async fn run(mut self) {
        loop {
            self.check().await;
            if self.records.is_empty() {
                return;
            }
            state::seams::sleep_until_counted(
                state::seams::now() + self.check_interval,
                "cleanup.checked",
            )
            .await;
        }
    }

    /// One check of every record. On macOS a record whose processes cannot
    /// be the task's ([`process::not_the_tasks`]) is confirmed without a
    /// signal. Otherwise the recorded process, the same ID with the same
    /// start time, gets SIGKILL once, and on macOS so does a recorded process
    /// group that still has members ([`process::kill_recorded`]); the record
    /// is confirmed when that process is gone and, on macOS, no process is
    /// in the recorded group. On Linux the recorded namespace is not read:
    /// nothing has held it open since its App Server ended, so its inode
    /// may name a later namespace, and its first process, the recorded one,
    /// is gone only once the kernel has ended every other process of it.
    /// Confirming a Session's last record removes the task directories its
    /// earlier tasks left, before the Session leaves `cleanup_pending`.
    async fn check(&mut self) {
        let sessions: Vec<String> = self.records.keys().cloned().collect();
        for session_id in sessions {
            let mut leftovers = self.records.remove(&session_id).unwrap_or_default();
            let mut kept = Vec::new();
            for mut leftover in leftovers.drain(..) {
                let record = &leftover.record;
                if process::not_the_tasks(record.pid, record.start_time, record.pid_namespace) {
                    state::seams::barrier("cleanup.confirmed").await;
                    remove(&leftover.path);
                    continue;
                }
                let alive = process::start_time(record.pid) == Some(record.start_time);
                if alive
                    || (process::MEMBERS_OUTLIVE_THE_LEADER
                        && !process::namespace_empty(record.pid_namespace))
                {
                    if !leftover.signalled {
                        process::kill_recorded(record.pid, record.pid_namespace);
                        leftover.signalled = true;
                    }
                    kept.push(leftover);
                    continue;
                }
                state::seams::barrier("cleanup.confirmed").await;
                remove(&leftover.path);
            }
            if kept.is_empty() {
                crate::store::clear_session_tasks(&self.state_root, &session_id);
                self.published
                    .send_replace(Arc::new(self.records.keys().cloned().collect()));
            } else {
                self.records.insert(session_id, kept);
            }
        }
    }
}
