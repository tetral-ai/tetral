//! The end of tasks between `tetral server run`, whose ready advertises
//! `tool_execution`, and the reference control server as separate
//! processes, each on its own controlled clock, with real restricted
//! processes: the termination procedure's grace, a connection's end with
//! SIGKILL at once, a resume that keeps a task running, processes that stay
//! after SIGKILL and the `cleanup_pending` of their Session, an old
//! connection's late events, and the records an App Server killed with
//! SIGKILL leaves for the next start. The test seams these tests use are
//! those `tetral_tests::tasks` and `tetral_tests::machine` name.

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, Generation, INodeNo, LockOwner, OpenFlags,
    ReplyAttr, ReplyData, ReplyEntry, Request,
};
use serde_json::{Value, json};
use tetral_tests::app_server::{request, wait_until};
use tetral_tests::machine::{INSTANCE, answer_fault, seconds, send, wait_for_requests};
use tetral_tests::socket_directory;
use tetral_tests::tasks::{
    GONE_CHECKED, REPLY_WAITING, SESSION, TaskMachine, WAIT_CHECKED, millis, policy,
    process_running,
};

/// The test seam barrier before a record an earlier App Server left is
/// deleted as confirmed.
const CONFIRMED: &str = "cleanup.confirmed";

/// Starts `cmd` as `task_id`, detached after 250 ms, under `direct`, and
/// gives its `running` report.
fn detached(tasks: &TaskMachine, task_id: &str, cmd: &str) -> Value {
    let checked = tasks.machine().seams.count(WAIT_CHECKED);
    tasks.execute(
        task_id,
        json!({"cmd": cmd, "wait_ms": 250, "on_wait_expiry": "detach"}),
        policy(&tasks.project, "direct"),
    );
    tasks.wait_for_start(checked);
    tasks.machine().seams.advance(millis(250));
    tasks.report(task_id, "running")
}

/// Ends the current connection with a connection-ending `disconnect` and
/// waits until the App Server has taken its end.
fn end_connection(tasks: &TaskMachine) {
    let machine = tasks.machine();
    let connection = machine.status()["connection_id"].clone();
    send(
        &tasks.engine,
        json!({
            "protocol_version": 1,
            "connection_id": connection,
            "message_id": format!("msg_end_{}", connection.as_str().unwrap()),
            "type": "disconnect",
            "payload": { "scope": "connection", "retryable": true, "code": "stale_connection" },
        }),
    );
    machine.wait_for("disconnected");
}

/// Waits for the next connection to be online once the ended one's lease
/// has passed and the CLI's clock has moved by `backoff`, and gives its
/// `connection_id`.
fn reconnect(tasks: &TaskMachine, backoff: Duration) -> Value {
    tasks.engine.advance(seconds(46.0));
    tasks.machine().seams.advance(backoff);
    tasks.machine().wait_for("online")["connection_id"].clone()
}

/// Issues `session.prepare` of `session`'s `binding` at the project and
/// gives the report that answers it: its `status` and error code.
fn prepared(tasks: &TaskMachine, session: &str, binding: &str) -> Value {
    let issued = tasks.prepare(session, binding, &tasks.project, None);
    let mut outcome = Value::Null;
    wait_until(&format!("the report answering {issued}"), || {
        match tasks
            .engine
            .received("session-status")
            .into_iter()
            .find(|report| report["body"]["request_message_id"] == issued.as_str())
        {
            Some(report) => {
                outcome = json!([report["body"]["status"], report["body"]["error"]["code"]]);
                true
            }
            None => false,
        }
    });
    outcome
}

/// Waits until the App Server has handled every instruction routed to
/// `session`'s owner before: the reply to a read of a task it never had.
fn handled(tasks: &TaskMachine, session: &str, binding: &str) {
    let issued = tasks.engine.control(
        "stdin",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": session, "binding_id": binding,
            "tool_use_event_id": "call_absent", "task_id": "task_absent", "chars": "",
        }}),
    );
    let reply = tasks.reply(issued["message_id"].as_str().unwrap());
    assert_eq!(reply["error"]["code"], "task_not_found");
}

/// Moves the App Server's clock by `step` and waits for the next check of
/// an ended task's processes.
fn next_check(tasks: &TaskMachine, step: Duration) {
    let seams = &tasks.machine().seams;
    let checked = seams.count(GONE_CHECKED);
    seams.advance(step);
    seams.wait_for_count(GONE_CHECKED, checked + 1);
}

/// Prepares `session`'s `binding` again, after each `cleanup_pending` once
/// the next check of the earlier task's processes has run, until the
/// Session prepares; gives the outcome.
fn prepared_once_gone(tasks: &TaskMachine, session: &str, binding: &str) -> Value {
    let pending = json!(["failed", "cleanup_pending"]);
    let mut outcome = prepared(tasks, session, binding);
    for _ in 0..10 {
        if outcome != pending {
            break;
        }
        next_check(tasks, seconds(1.0));
        outcome = prepared(tasks, session, binding);
    }
    outcome
}

#[test]
fn a_cancel_gives_the_command_its_grace_to_run_its_term_trap_and_exit() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("trap 'echo t >> m' TERM; sleep 100; echo after, then a cancel");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    detached(
        &tasks,
        "task_trap",
        "trap 'echo t >> m' TERM; sleep 100; echo after",
    );
    let cancel = tasks.cancel("task_trap", json!({}));
    assert_eq!(tasks.reply(&cancel)["result"]["result"]["cancelled"], true);
    // The clock never moves: the exit comes within the grace.
    let terminal = tasks.terminal("task_trap");
    let result = &terminal["result"]["result"];
    assert_eq!(
        json!([
            result["exit_code"],
            result["signal"],
            result["cancelled"],
            result["stdout"]["text"]
        ]),
        json!([0, null, true, "after\n"]),
        "{terminal:#}"
    );
    // The sandbox's first process passes SIGTERM on to the command's process
    // group and to the shell itself, so the trap runs once or twice.
    let marks = fs::read_to_string(tasks.project.join("m")).unwrap();
    assert!(
        !marks.is_empty() && marks.lines().all(|line| line == "t"),
        "{marks:?}"
    );
    case.observe("within the grace m held t, stdout after, and the terminal report exit_code 0 with cancelled");
}

#[test]
fn a_connections_end_discards_its_staged_tasks_and_kills_its_running_ones_at_once() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a task staged behind a withheld prepared ACK, and a running task with a TERM trap, a sleep child and a child that called setsid: a connection-ending disconnect",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    detached(
        &tasks,
        "task_running",
        "trap 'echo t >> m' TERM; sleep 1201 & setsid sleep 1202 & wait",
    );
    wait_until("both children running", || {
        process_running(&["sleep", "1201"]) && process_running(&["sleep", "1202"])
    });
    answer_fault(
        &tasks.engine,
        json!({"endpoint": "session-status", "action": "withhold"}),
    );
    let issued = tasks.prepare("sess_staged", "bind_staged", &tasks.project, None);
    tasks.wait_prepared(&issued);
    tasks.execute_with(
        "sess_staged",
        "bind_staged",
        "task_staged",
        json!({"cmd": "touch staged"}),
        policy(&tasks.project, "disabled"),
        None,
    );
    handled(&tasks, "sess_staged", "bind_staged");

    let ended = Instant::now();
    end_connection(&tasks);
    wait_until("every process of the task gone", || {
        !process_running(&["sleep", "1201"]) && !process_running(&["sleep", "1202"])
    });
    let took = ended.elapsed();
    assert!(took < seconds(2.0), "{took:?}");
    answer_fault(&tasks.engine, json!({"action": "release"}));
    reconnect(&tasks, seconds(1.2));
    assert!(!tasks.project.join("m").exists());
    assert!(!tasks.project.join("staged").exists());
    case.observe(format!(
        "both children gone {took:?} after the disconnect was sent, with no SIGTERM (no m); the staged task never ran (no staged)"
    ));
}

#[test]
fn a_resume_during_a_task_keeps_it_running_also_one_40_seconds_after_the_last_grant() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a task waiting for a file: a stream break and its resume, then a resume that succeeds 40 seconds after the last grant, and the clock past the earlier deadline",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let machine = tasks.machine();
    let seams = &machine.seams;
    detached(
        &tasks,
        "task_long",
        "while [ ! -e go ]; do sleep 0.05; done; echo done",
    );
    let connection = machine.status()["connection_id"].clone();

    tasks
        .engine
        .control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    seams.advance(seconds(1.2));
    let status = machine.wait_for("online");
    assert_eq!(status["connection_id"], connection);

    // The last grant comes at the start of the advance; 30 seconds after
    // it a break, and a resume whose connected event comes 40 seconds after
    // it, 5 seconds before the deadline it set.
    tasks.advance(seconds(15.0));
    let grants = seams.count("connection.granted");
    seams.advance(seconds(15.0));
    let streams = tasks.engine.requests("events").len();
    let attempts = machine.attempts();
    tasks.engine.control(
        "events",
        json!({ "mode": "delay_connected", "delay_ms": 9_000 }),
    );
    tasks
        .engine
        .control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    seams.advance(seconds(1.2));
    seams.wait_for_count("connection.attempt", attempts + 1);
    seams.advance(seconds(8.8));
    tasks.engine.advance(seconds(9.0));
    let status = machine.wait_for("online");
    assert_eq!(status["connection_id"], connection);
    assert_eq!(
        tasks.engine.requests("events")[streams]["query"]["resume"],
        "true"
    );
    seams.wait_for_count("connection.granted", grants + 1);
    machine.advance(seconds(10.0));
    assert_eq!(machine.state(), "online");

    fs::write(tasks.project.join("go"), "").unwrap();
    let terminal = tasks.terminal("task_long");
    let result = &terminal["result"]["result"];
    assert_eq!(
        json!([result["exit_code"], result["stdout"]["text"]]),
        json!([0, "done\n"])
    );
    case.observe("the task ran across both resumes and past the deadline of the grant before the late one, and its result arrived");
}

#[test]
fn a_running_task_ends_at_the_connections_deadline_after_a_withheld_resume_or_a_step_past_it() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case(
        "a running task with a TERM trap; the stream broken and its resume withheld past the connection's deadline",
    );
    let tasks = TaskMachine::start(&mut case, &directory.path().join("withheld"));
    let machine = tasks.machine();
    detached(&tasks, "task_held", "trap 'echo t >> m' TERM; sleep 1211");
    let connection = machine.status()["connection_id"].clone();
    let streams = tasks.engine.requests("events").len();
    tasks
        .engine
        .control("events", json!({ "mode": "hold_headers", "count": 1 }));
    tasks
        .engine
        .control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(1.2));
    let events = wait_for_requests(&tasks.engine, "events", streams + 1);
    assert_eq!(events[streams]["query"]["resume"], "true");
    assert!(process_running(&["sleep", "1211"]));
    tasks.engine.advance(seconds(44.5));
    machine.advance(seconds(43.81));
    assert_eq!(machine.state(), "disconnected");
    wait_until("the task's process gone", || {
        !process_running(&["sleep", "1211"])
    });
    // The withheld resume's timeout and the connection's end each doubled
    // the backoff.
    let fresh = reconnect(&tasks, seconds(4.8));
    assert_ne!(fresh, connection);
    assert!(!tasks.project.join("m").exists());
    case.observe("the connection ended at its deadline with the resume withheld, which killed the task without SIGTERM (no m); the next connection was a fresh one");
    drop(tasks);

    let mut case = record.case(
        "a running task; the clock moved past the connection's deadline in one step with the stream open",
    );
    let tasks = TaskMachine::start(&mut case, &directory.path().join("stepped"));
    let machine = tasks.machine();
    detached(&tasks, "task_stepped", "sleep 1212");
    let stepped = Instant::now();
    machine.seams.advance(seconds(46.0));
    wait_until("the task's process gone", || {
        !process_running(&["sleep", "1212"])
    });
    let took = stepped.elapsed();
    assert!(took < seconds(1.0), "{took:?}");
    case.observe(format!("the task's process gone {took:?} after the step"));
}

#[test]
fn an_old_connections_waiter_and_late_output_change_nothing_of_the_new_one() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a task printing all the time with a write's reply waiting, its connection ended; on the next connection the Session prepared again and a task of the same task_id",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let machine = tasks.machine();
    detached(&tasks, "task_old", "while :; do printf x; sleep 0.01; done");
    // A write's reply waits its whole wait while output keeps coming.
    let waiting = machine.seams.count(REPLY_WAITING);
    tasks.stdin(
        "task_old",
        json!({"chars": "x", "write_seq": 1, "wait_ms": 30_000}),
    );
    machine.seams.wait_for_count(REPLY_WAITING, waiting + 1);
    let old = machine.status()["connection_id"].clone();
    let checked = machine.seams.count(GONE_CHECKED);
    end_connection(&tasks);
    let new = reconnect(&tasks, seconds(1.2));
    machine.seams.wait_for_count(GONE_CHECKED, checked + 1);
    assert_eq!(
        prepared_once_gone(&tasks, SESSION, "bind_new"),
        json!(["prepared", null])
    );
    tasks.execute_with(
        SESSION,
        "bind_new",
        "task_old",
        json!({"cmd": "printf new"}),
        policy(&tasks.project, "disabled"),
        None,
    );
    let on_new: Vec<Value> = tasks
        .engine
        .received("tool-status")
        .into_iter()
        .map(|received| received["body"].clone())
        .filter(|body| body["connection_id"] == new)
        .collect();
    wait_until("the new task's terminal report", || {
        tasks
            .reports("task_old")
            .iter()
            .any(|report| report["binding_id"] == "bind_new")
    });
    let new_reports: Vec<Value> = tasks
        .reports("task_old")
        .into_iter()
        .filter(|report| report["connection_id"] == new)
        .map(|report| {
            json!([
                report["binding_id"],
                report["phase"],
                report["report_seq"],
                report["result"]["result"]["stdout"]["text"]
            ])
        })
        .collect();
    assert_eq!(new_reports, [json!(["bind_new", "terminal", 1, "new"])]);
    assert!(
        on_new.iter().all(|body| body["kind"] != "control"),
        "{on_new:?}"
    );
    let sessions = request(&machine.app_server.socket, "GET", "/v1/sessions")
        .unwrap()
        .json();
    let bindings: Vec<Value> = sessions["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["session_id"] == SESSION)
        .map(|entry| json!([entry["binding_id"], entry["connection_id"], entry["state"]]))
        .collect();
    assert_eq!(bindings, [json!(["bind_new", new, "prepared"])]);
    assert_ne!(old, new);
    case.observe("no reply or report of the old task reached the new connection; the new task of the same task_id reported terminal 1 with its own output; the Session's binding is the new prepared one");
}

#[test]
fn processes_left_after_sigkill_keep_their_session_cleanup_pending_while_others_prepare() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a task ignoring SIGTERM whose child is blocked in a read on a FUSE mount that never answers; its connection ended",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let mount = tasks.project.join("mnt");
    fs::create_dir(&mount).unwrap();
    let held = Arc::new((Mutex::new(false), Condvar::new()));
    let reads = Arc::new(AtomicUsize::new(0));
    let session = fuser::spawn_mount(
        Unanswered {
            released: held.clone(),
            reads: reads.clone(),
        },
        &mount,
        &fuser::Config::default(),
    )
    .unwrap();
    detached(
        &tasks,
        "task_stuck",
        "trap '' TERM; sleep 1221 & cat mnt/f > out; wait",
    );
    wait_until("the read reaching the mount", || {
        reads.load(Ordering::SeqCst) > 0
    });
    let checked = tasks.machine().seams.count(GONE_CHECKED);
    end_connection(&tasks);
    reconnect(&tasks, seconds(1.2));
    // The kill's wait passes with the blocked child still in the task's
    // namespace.
    tasks.machine().seams.advance(seconds(1.0));
    tasks
        .machine()
        .seams
        .wait_for_count(GONE_CHECKED, checked + 1);
    assert_eq!(
        prepared(&tasks, SESSION, "bind_again"),
        json!(["failed", "cleanup_pending"])
    );
    assert_eq!(
        prepared(&tasks, "sess_other", "bind_other"),
        json!(["prepared", null])
    );
    next_check(&tasks, seconds(1.0));
    assert_eq!(
        prepared(&tasks, SESSION, "bind_again_2"),
        json!(["failed", "cleanup_pending"])
    );

    release(&held);
    next_check(&tasks, seconds(1.0));
    assert_eq!(
        prepared_once_gone(&tasks, SESSION, "bind_released"),
        json!(["prepared", null])
    );
    let claimed: Vec<Value> = tasks
        .reports("task_stuck")
        .into_iter()
        .filter(|report| report["phase"] == "terminal")
        .collect();
    assert!(claimed.is_empty(), "{claimed:?}");
    drop(session);
    case.observe("while the read was unanswered the Session's preparations failed cleanup_pending and another Session prepared; once the mount answered the next preparation succeeded; no terminal report claimed an exit");
}

#[test]
fn records_an_app_server_killed_with_sigkill_leaves_are_confirmed_at_the_next_start() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case(
        "a command appending to a marker every 100 ms, the App Server killed with SIGKILL, the record's confirmation held at the next start",
    );
    let mut tasks = TaskMachine::start(&mut case, &directory.path().join("killed"));
    detached(
        &tasks,
        "task_marker",
        "while :; do echo x >> marker; sleep 0.1; done",
    );
    let records = tasks.root.join("execution");
    wait_until("the task's record", || listing(&records).len() == 1);
    let written: Value = serde_json::from_slice(&fs::read(&listing(&records)[0]).unwrap()).unwrap();
    assert_eq!(written["session_id"], SESSION);
    tasks.machine().seams.hold(CONFIRMED);
    tasks.kill_and_restart(|_| {});
    let marker = tasks.project.join("marker");
    let size = fs::metadata(&marker).unwrap().len();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(fs::metadata(&marker).unwrap().len(), size);
    let status = tasks.machine().status();
    assert_eq!(
        status["last_error"]["code"], "cleanup_pending",
        "{status:#}"
    );
    assert_eq!(
        prepared(&tasks, SESSION, "bind_restarted"),
        json!(["failed", "cleanup_pending"])
    );
    tasks.machine().seams.release(CONFIRMED);
    // The recorded process may still be dying at the first check, which
    // then signals it and checks again a second later.
    wait_until("last_error cleared", || {
        let cleared = tasks.machine().status()["last_error"].is_null();
        if !cleared {
            tasks.machine().seams.advance(millis(100));
        }
        cleared
    });
    assert!(listing(&records).is_empty());
    assert_eq!(
        prepared(&tasks, SESSION, "bind_confirmed"),
        json!(["prepared", null])
    );
    case.observe("the marker stopped growing; last_error was cleanup_pending and the Session's preparation failed so until the record was confirmed, then null and the preparation succeeded");
    drop(tasks);

    let mut case = record.case(
        "a record of another boot naming a running process started outside any task, written before the App Server starts",
    );
    let mut tasks = TaskMachine::start(&mut case, &directory.path().join("other_boot"));
    let mut outside = std::process::Command::new("sleep")
        .arg("1231")
        .spawn()
        .unwrap();
    let pid = outside.id();
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let start_time: u64 = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap();
    let link = fs::read_link(format!("/proc/{pid}/ns/pid")).unwrap();
    let namespace: u64 = link
        .to_str()
        .unwrap()
        .trim_start_matches("pid:[")
        .trim_end_matches(']')
        .parse()
        .unwrap();
    let path = tasks
        .root
        .join("execution")
        .join("00000000000000000000000000000000.json");
    tasks.kill_and_restart(|_| {
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "session_id": SESSION, "platform": "linux",
                "boot_id": "00000000-0000-0000-0000-000000000000",
                "pid": pid, "start_time": start_time, "pid_namespace": namespace,
            }))
            .unwrap(),
        )
        .unwrap();
    });
    assert!(tasks.machine().status()["last_error"].is_null());
    assert!(!path.exists());
    assert_eq!(
        prepared(&tasks, SESSION, "bind_other_boot"),
        json!(["prepared", null])
    );
    assert!(outside.try_wait().unwrap().is_none());
    outside.kill().unwrap();
    outside.wait().unwrap();
    case.observe("the record was confirmed at the start: last_error null, the record gone, the Session prepared, and the named process still running");
}

fn listing(directory: &Path) -> Vec<std::path::PathBuf> {
    fs::read_dir(directory)
        .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
        .unwrap_or_default()
}

/// A FUSE file system with one file `f` whose reads are answered only once
/// [`release`] is called; meanwhile the reading process waits in the kernel
/// and no signal ends it.
struct Unanswered {
    released: Arc<(Mutex<bool>, Condvar)>,
    reads: Arc<AtomicUsize>,
}

const FILE: INodeNo = INodeNo(2);
const CONTENT: &[u8] = b"answered\n";

fn attributes(ino: INodeNo) -> FileAttr {
    let file = ino == FILE;
    FileAttr {
        ino,
        size: if file { CONTENT.len() as u64 } else { 0 },
        blocks: 1,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: if file {
            FileType::RegularFile
        } else {
            FileType::Directory
        },
        perm: if file { 0o644 } else { 0o755 },
        nlink: if file { 1 } else { 2 },
        uid: rustix::process::getuid().as_raw(),
        gid: rustix::process::getgid().as_raw(),
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

impl Filesystem for Unanswered {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        if parent == INodeNo::ROOT && name == "f" {
            reply.entry(&Duration::ZERO, &attributes(FILE), Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        if ino == INodeNo::ROOT || ino == FILE {
            reply.attr(&Duration::ZERO, &attributes(ino));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        _size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let (released, changed) = &*self.released;
        let mut released = released.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        reply.data(CONTENT.get(offset as usize..).unwrap_or_default());
    }
}

fn release(held: &Arc<(Mutex<bool>, Condvar)>) {
    let (released, changed) = &**held;
    *released.lock().unwrap() = true;
    changed.notify_all();
}
