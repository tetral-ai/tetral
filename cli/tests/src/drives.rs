//! Drives of a Session on the real App Server, with the reference control
//! server as Engine: its controls stand for another writer and for what the
//! protocol leaves to Engine, and its request log and drive state show what
//! the CLI sent and what the drive holds.

use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::record::Case;
use crate::reference::ReferenceServer;
use crate::tasks::TaskMachine;

pub const DRIVE: &str = "drive_a";
pub const READ_WRITE: &str = "read_write";
pub const READ_ONLY: &str = "read_only";

/// A change of another writer's commit that writes `text` at `path`.
pub fn write(path: &str, text: &str) -> Value {
    json!({ "path": path, "text": text })
}

/// [`write`] of an executable file.
pub fn write_executable(path: &str, text: &str) -> Value {
    json!({ "path": path, "text": text, "executable": true })
}

/// A change of another writer's commit that deletes `path`.
pub fn delete(path: &str) -> Value {
    json!({ "path": path, "delete": true })
}

/// Commits `changes` to `drive` as another writer, in one commit, and gives
/// its `seq`. The server refuses a change the drive's rules reject.
pub fn commit(engine: &ReferenceServer, drive: &str, changes: &[Value]) -> u64 {
    let committed = engine.control(
        "drive_commit",
        json!({ "drive_id": drive, "changes": changes }),
    );
    committed["seq"]
        .as_u64()
        .expect("the commit made a version")
}

/// The drive's `seq`, current `files`, kept `versions`, change `records`
/// and held `contents`.
pub fn state(engine: &ReferenceServer, drive: &str) -> Value {
    engine.control("drive_state", json!({ "drive_id": drive }))
}

/// The drive's current files as `(path, content_sha256)`, in path order.
pub fn files(engine: &ReferenceServer, drive: &str) -> Vec<(String, String)> {
    state(engine, drive)["files"]
        .as_array()
        .expect("files")
        .iter()
        .map(|file| {
            (
                file["path"].as_str().unwrap().to_owned(),
                file["content_sha256"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

/// How many requests and answers the server holds for a `hold_request`, a
/// `withhold` or a `delay` of the answer control.
pub fn held(engine: &ReferenceServer) -> usize {
    engine.state()["held"].as_array().map_or(0, Vec::len)
}

/// The seam counter of the final uploads an App Server's drive syncs have
/// begun, each with its end time set: a test that moves the clock through a
/// final upload waits for it first.
pub const FINAL_UPLOAD: &str = "drive.final_upload";

/// The request log's entries of `endpoint` that name `drive`.
pub fn requests(engine: &ReferenceServer, endpoint: &str, drive: &str) -> Vec<Value> {
    engine
        .requests(endpoint)
        .into_iter()
        .filter(|request| request["drive_id"] == drive)
        .collect()
}

/// The `changes` of each commit the server accepted for `drive`, in arrival
/// order.
pub fn commits(engine: &ReferenceServer, drive: &str) -> Vec<Value> {
    engine
        .received("commit")
        .into_iter()
        .filter(|commit| commit["drive_id"] == drive)
        .map(|commit| commit["body"]["changes"].clone())
        .collect()
}

/// `path` of `drive` as a command of the Session names it.
pub fn in_drive(drive: &str, path: &str) -> String {
    format!("\"$TETRAL_DRIVES/{drive}/{path}\"")
}

/// The lowercase hexadecimal SHA-256 of `bytes`.
pub fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A [`TaskMachine`] whose prepared binding binds drives, in a directory of
/// its own. Dropping it stops the App Server, detaches what it left mounted
/// and removes the directory.
pub struct DriveMachine {
    tasks: TaskMachine,
    _unmount: Unmount,
    pub directory: tempfile::TempDir,
    _socket_directory: crate::socket_directory::IsolatedSocketDir,
    commands: AtomicU64,
}

impl std::ops::Deref for DriveMachine {
    type Target = TaskMachine;

    fn deref(&self) -> &TaskMachine {
        &self.tasks
    }
}

impl std::ops::DerefMut for DriveMachine {
    fn deref_mut(&mut self) -> &mut TaskMachine {
        &mut self.tasks
    }
}

impl DriveMachine {
    /// Starts the machine and prepares its binding with `drives`, each a
    /// drive ID and its access.
    pub fn start(case: &mut Case, drives: &[(&str, &str)]) -> DriveMachine {
        DriveMachine::start_with(case, drives, &[], |_| {})
    }

    /// [`DriveMachine::start`] with `seed` run on the reference control
    /// server before the App Server starts, so that the first mount finds
    /// what it committed.
    pub fn start_seeded(
        case: &mut Case,
        drives: &[(&str, &str)],
        seed: impl FnOnce(&ReferenceServer),
    ) -> DriveMachine {
        DriveMachine::start_with(case, drives, &[], seed)
    }

    /// [`DriveMachine::start_seeded`] with the machine's directory in the
    /// host's memory where Linux has `/dev/shm`, for a test of thousands of
    /// file operations: the App Server stores each before it returns, which
    /// on a disk takes some eight milliseconds.
    pub fn start_seeded_in_memory(
        case: &mut Case,
        drives: &[(&str, &str)],
        seed: impl FnOnce(&ReferenceServer),
    ) -> DriveMachine {
        let memory = Path::new("/dev/shm");
        let directory = if cfg!(target_os = "linux") && memory.is_dir() {
            tempfile::tempdir_in(memory)
        } else {
            tempfile::tempdir()
        };
        DriveMachine::start_in(
            directory.expect("a temporary directory"),
            case,
            drives,
            &[],
            seed,
        )
    }

    /// [`DriveMachine::start_seeded`] with `exclude`, each a drive ID and
    /// the paths its binding leaves out.
    pub fn start_with(
        case: &mut Case,
        drives: &[(&str, &str)],
        exclude: &[(&str, &[&str])],
        seed: impl FnOnce(&ReferenceServer),
    ) -> DriveMachine {
        let directory = tempfile::tempdir().expect("a temporary directory");
        DriveMachine::start_in(directory, case, drives, exclude, seed)
    }

    fn start_in(
        directory: tempfile::TempDir,
        case: &mut Case,
        drives: &[(&str, &str)],
        exclude: &[(&str, &[&str])],
        seed: impl FnOnce(&ReferenceServer),
    ) -> DriveMachine {
        let socket_directory = crate::socket_directory::isolated();
        let unmount = Unmount::under(directory.path());
        let tasks = TaskMachine::start_with_drives(case, directory.path(), drives, exclude, seed);
        DriveMachine {
            tasks,
            _unmount: unmount,
            directory,
            _socket_directory: socket_directory,
            commands: AtomicU64::new(0),
        }
    }

    /// Where the prepared binding's `drive` is mounted.
    pub fn mount(&self, drive: &str) -> PathBuf {
        self.tasks
            .root
            .join("sessions")
            .join(crate::tasks::SESSION)
            .join("drives")
            .join(drive)
    }

    /// Issues `cmd` in the prepared binding with an initial wait of
    /// `wait_ms` of the App Server's clock, at whose end the command is
    /// killed, and gives its `task_id`.
    pub fn start_command_waiting(&self, cmd: &str, wait_ms: u64) -> String {
        self.issue(json!({ "cmd": cmd, "wait_ms": wait_ms }))
    }

    /// [`DriveMachine::start_command_waiting`] for a command that goes on
    /// in the background at the wait's end, where its running report comes.
    pub fn start_background(&self, cmd: &str, wait_ms: u64) -> String {
        self.issue(json!({ "cmd": cmd, "wait_ms": wait_ms, "on_wait_expiry": "detach" }))
    }

    fn issue(&self, input: Value) -> String {
        let task = format!("task_{}", self.commands.fetch_add(1, Ordering::SeqCst) + 1);
        self.tasks.execute(
            &task,
            input,
            crate::tasks::policy(&self.tasks.project, "disabled"),
        );
        task
    }

    /// Issues `cmd` in the prepared binding and gives its `task_id`. The
    /// command is killed once the App Server's clock moves a quarter second.
    pub fn start_command(&self, cmd: &str) -> String {
        self.start_command_waiting(cmd, 0)
    }

    /// Runs `cmd` in the prepared binding and gives its terminal report.
    pub fn command(&self, cmd: &str) -> Value {
        self.command_within(crate::seams::WAIT_LIMIT, cmd)
    }

    /// [`DriveMachine::command`] for a command whose own work takes longer
    /// than the harness's wait: its terminal report may take `limit`.
    pub fn command_within(&self, limit: Duration, cmd: &str) -> Value {
        let task = self.start_command(cmd);
        self.tasks.report_within(limit, &task, "terminal")
    }

    /// The `applied` report of the `session.drives` issued as `message_id`.
    pub fn applied(&self, message_id: &str) -> Value {
        let report = self.tasks.session_report(message_id);
        assert_eq!(report["status"], "applied", "{report}");
        report
    }

    /// Runs `cmd`, which must exit 0, and gives its stdout without the
    /// final line end.
    pub fn output(&self, cmd: &str) -> String {
        let terminal = self.command(cmd);
        let (exit, stdout, _) = outcome(&terminal);
        assert_eq!(exit, 0, "{cmd}: {terminal}");
        stdout.trim_end_matches('\n').to_owned()
    }
}

impl TaskMachine {
    /// Runs `cmd` as `task_id` of `session`'s binding `binding` under the
    /// policy of [`crate::tasks::policy`] without a network, whose write
    /// rule also lets the command write its `read_write` drives, and gives
    /// its terminal report once the server has accepted it.
    pub fn run_in(&self, session: &str, binding: &str, task_id: &str, cmd: &str) -> Value {
        self.execute_with(
            session,
            binding,
            task_id,
            json!({ "cmd": cmd, "wait_ms": 0 }),
            crate::tasks::policy(&self.project, "disabled"),
            None,
        );
        self.terminal(task_id)
    }

    /// [`TaskMachine::run_in`] for the prepared binding.
    pub fn run(&self, task_id: &str, cmd: &str) -> Value {
        self.run_in(crate::tasks::SESSION, crate::tasks::BINDING, task_id, cmd)
    }
}

/// The exit code, the stdout text and the `drive_sync` (null when the
/// result has none) of a terminal report.
pub fn outcome(terminal: &Value) -> (i64, String, Value) {
    let envelope = &terminal["result"];
    (
        envelope["result"]["exit_code"]
            .as_i64()
            .unwrap_or_else(|| panic!("no exit code in {terminal}")),
        envelope["result"]["stdout"]["text"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        envelope["drive_sync"].clone(),
    )
}

/// Detaches what an App Server left mounted under a directory, so that the
/// directory can be removed after the App Server was killed.
pub struct Unmount(pub PathBuf);

impl Unmount {
    pub fn under(directory: &Path) -> Unmount {
        Unmount(directory.canonicalize().expect("the directory exists"))
    }
}

impl Drop for Unmount {
    #[cfg(target_os = "linux")]
    fn drop(&mut self) {
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        for point in mountinfo.lines().filter_map(|line| line.split(' ').nth(4)) {
            if Path::new(point).starts_with(&self.0) {
                let _ = std::process::Command::new("fusermount3")
                    .args(["-u", "-z", point])
                    .status();
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn drop(&mut self) {
        let mounted = std::process::Command::new("/sbin/mount")
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
            .unwrap_or_default();
        for point in mounted
            .lines()
            .filter_map(|line| line.split(" on ").nth(1))
            .filter_map(|rest| rest.rsplit_once(" (").map(|(point, _)| point))
        {
            if Path::new(point).starts_with(&self.0) {
                let _ = std::process::Command::new("/sbin/umount")
                    .args(["-f", point])
                    .status();
            }
        }
    }
}
