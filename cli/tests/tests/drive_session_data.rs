//! Layer 2. What the real App Server keeps of a Session's drives across
//! idle time, its own restarts and ended connections, against the reference
//! control server and through real mounts: the Session's local data until
//! its close, a change not yet committed until its commit, a rejection not
//! yet acknowledged until its acknowledgement, and the bytes of a download
//! its connection's end cut short until the next read.

use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, READ_WRITE, commit, in_drive, outcome, sha256, write,
};
use tetral_tests::machine::{INSTANCE, answer_fault};
use tetral_tests::tasks::{BINDING, SESSION};

/// How long the pauses are in which the App Server could do what a test
/// then finds it has not done.
const PAUSE: Duration = Duration::from_millis(300);

const SECOND: Duration = Duration::from_secs(1);

/// Prepares the Session again as `binding` with the drive, as the server
/// does on a new connection.
fn prepared_again(machine: &DriveMachine, binding: &str) {
    let issued = machine.prepare_with(
        SESSION,
        binding,
        &machine.project,
        &[(DRIVE, READ_WRITE)],
        &[],
    );
    let report = machine.session_report(&issued);
    assert_eq!(report["status"], "prepared", "{report}");
    machine.answers_settled();
}

/// The requests of `endpoint` the server has answered for the drive.
fn sent(machine: &DriveMachine, endpoint: &str) -> Vec<Value> {
    drives::requests(&machine.engine, endpoint, DRIVE)
}

#[test]
fn a_sessions_data_stays_through_idle_time_a_restart_and_a_connections_end_until_its_close() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a Session that read a file, left idle for a minute, across a kill of the App Server and across the end of its connection",
    );
    let mut machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(engine, DRIVE, &[write("f.txt", "first")]);
    });
    let read = format!("cat {}", in_drive(DRIVE, "f.txt"));
    assert_eq!(machine.output(&read), "first");
    let data = machine.root.join("sessions").join(SESSION);
    // The data that stays includes the file's content: no read after the
    // first downloads it again.
    let stays = |machine: &DriveMachine, binding: &str, task: &str| {
        assert!(data.exists(), "the Session's data is gone");
        let terminal = machine.run_in(SESSION, binding, task, &read);
        assert_eq!(outcome(&terminal).1, "first", "{terminal}");
        assert_eq!(sent(machine, "download_content").len(), 1);
    };
    machine.advance(Duration::from_secs(60));
    std::thread::sleep(PAUSE);
    stays(&machine, BINDING, "after_idle");
    machine.kill_and_restart(|_| {});
    prepared_again(&machine, "bind_2");
    stays(&machine, "bind_2", "after_restart");
    machine.end_connection();
    machine.reconnect(Duration::from_millis(1_200));
    prepared_again(&machine, "bind_3");
    stays(&machine, "bind_3", "after_reconnect");
    case.observe("after a minute idle, after a kill and a new start, and after its connection's end, the Session's data was there and each new binding's command read f.txt with no second download");
    case.pass();

    let mut case = record.case(
        "the same Session closed with a new file and a rewrite of a file another writer changed meanwhile, both written outside any command",
    );
    let mount = machine.mount(DRIVE);
    std::fs::write(mount.join("f.txt"), "agent").unwrap();
    std::fs::write(mount.join("new.txt"), "new").unwrap();
    commit(&machine.engine, DRIVE, &[write("f.txt", "developer")]);
    // No task runs: the changes wait for a sync point, also past the two
    // seconds after which a running task's write is uploaded.
    for _ in 0..3 {
        machine.step(SECOND);
    }
    std::thread::sleep(PAUSE);
    assert_eq!(
        (
            sent(&machine, "upload_content").len(),
            sent(&machine, "commit").len()
        ),
        (0, 0)
    );
    let issued = machine.engine.control(
        "close",
        json!({
            "instance_id": INSTANCE,
            "payload": { "session_id": SESSION, "binding_id": "bind_3" },
        }),
    );
    let report = machine.session_report(issued["message_id"].as_str().unwrap());
    assert_eq!(
        (&report["status"], report.get("lost_changes")),
        (&json!("closed"), None),
        "{report}"
    );
    wait_until("the Session's data deleted", || !data.exists());
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [
            ("f.txt".to_owned(), sha256(b"developer")),
            ("new.txt".to_owned(), sha256(b"new")),
        ]
    );
    case.observe("three seconds on, nothing was uploaded; the close committed new.txt, the drive kept the other writer's f.txt, the closed report listed no lost change and the Session's data was deleted");
    case.pass();
}

/// Runs a command that writes `path` while every upload is answered `503`,
/// and gives its result, which comes with the 30th second after its exit
/// and lists the file as uploading.
fn written_while_uploads_fail(machine: &DriveMachine, binding: &str, path: &str) {
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "upload_content", "action": "error",
            "code": "unavailable", "status": 503, "count": 10_000,
        }),
    );
    let task = format!("write_{path}");
    let before = sent(machine, "upload_content").len();
    machine.execute_with(
        SESSION,
        binding,
        &task,
        json!({"cmd": format!("printf {path} > {}", in_drive(DRIVE, path)), "wait_ms": 50_000}),
        tetral_tests::tasks::policy(&machine.project, "disabled"),
        None,
    );
    wait_until("the first refused upload", || {
        sent(machine, "upload_content").len() > before
    });
    for _ in 0..30 {
        machine.step(SECOND);
    }
    let terminal = machine.terminal(&task);
    let listed = outcome(&terminal).2["not_synced"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        listed.len() == 1
            && listed[0]["reason"] == "uploading"
            && listed[0]["path"]
                .as_str()
                .is_some_and(|listed| listed.ends_with(&format!("/drives/{DRIVE}/{path}"))),
        "{terminal}"
    );
    // A change of no command: no task's report covers it.
    std::fs::write(machine.mount(DRIVE).join(outside(path)), outside(path)).unwrap();
}

/// The file [`written_while_uploads_fail`] writes outside any command
/// beside `path`.
fn outside(path: &str) -> String {
    format!("outside-{path}")
}

/// With uploads still failing, a command of `binding` ends with the clock
/// standing and without the pending files; once uploads pass, `path` and
/// the file written outside any command are committed with no tool call.
fn committed_after_the_loss(machine: &DriveMachine, binding: &str, path: &str) {
    let commits = drives::commits(&machine.engine, DRIVE).len();
    let next = machine.run_in(SESSION, binding, &format!("after_{path}"), "true");
    let sync = outcome(&next).2;
    assert!(
        sync.is_null() || sync["not_synced"] == json!([]) && sync["committed"] == 0,
        "{next}"
    );
    assert_eq!(drives::commits(&machine.engine, DRIVE).len(), commits);
    answer_fault(&machine.engine, json!({"action": "clear"}));
    let committed = || -> Vec<Value> {
        drives::commits(&machine.engine, DRIVE)[commits..]
            .iter()
            .flat_map(|commit| commit.as_array().cloned().unwrap_or_default())
            .collect()
    };
    machine.advance_until(
        &format!("the commits of {path} and {}", outside(path)),
        Duration::from_secs(5),
        Duration::from_secs(120),
        || committed().len() == 2,
    );
    let mut changes = committed();
    changes.sort_by_key(|change| change["path"].as_str().map(str::len));
    let write = |path: &str| {
        json!({
            "path": path, "change": "write", "base_sha256": null,
            "content_sha256": sha256(path.as_bytes()), "executable": false,
        })
    };
    assert_eq!(changes, [write(path), write(&outside(path))]);
}

#[test]
fn a_change_not_yet_committed_outlives_its_connection_a_kill_and_a_kill_before_its_commits_answer()
{
    let record = tetral_tests::record!();
    let mut machine = {
        let mut case = record.case("a Session with one read_write drive");
        let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
        case.pass();
        machine
    };

    let mut case = record.case("a file written while uploads fail, and then the connection's end");
    written_while_uploads_fail(&machine, BINDING, "a.txt");
    machine.end_connection();
    machine.reconnect(Duration::from_millis(1_200));
    prepared_again(&machine, "bind_2");
    committed_after_the_loss(&machine, "bind_2", "a.txt");
    case.observe("on the next connection a command ended with the clock standing and without a.txt or the file written outside any command while uploads still failed; once they passed both were committed with no tool call");
    case.pass();

    let mut case =
        record.case("a file written while uploads fail, and then a kill of the App Server");
    written_while_uploads_fail(&machine, "bind_2", "b.txt");
    machine.kill_and_restart(|_| {});
    prepared_again(&machine, "bind_3");
    committed_after_the_loss(&machine, "bind_3", "b.txt");
    case.observe("after the next start a command ended with the clock standing and without b.txt or the file written outside any command while uploads still failed; once they passed both were committed with no tool call");
    case.pass();

    let mut case = record.case(
        "a file rewritten after the server applied its commit and before the commit's answer arrived, and then a kill of the App Server",
    );
    answer_fault(
        &machine.engine,
        json!({"endpoint": "commit", "drive_id": DRIVE, "action": "drop", "count": 1}),
    );
    let commits = drives::commits(&machine.engine, DRIVE).len();
    machine.execute_with(
        SESSION,
        "bind_3",
        "write_c",
        json!({"cmd": format!("printf c.txt > {}", in_drive(DRIVE, "c.txt")), "wait_ms": 50_000}),
        tetral_tests::tasks::policy(&machine.project, "disabled"),
        None,
    );
    wait_until("the commit applied and its answer lost", || {
        drives::commits(&machine.engine, DRIVE).len() == commits + 1
    });
    std::fs::write(machine.mount(DRIVE).join("c.txt"), "again").unwrap();
    machine.kill_and_restart(|_| {});
    prepared_again(&machine, "bind_4");
    machine.advance_until(
        "the commit sent again and the commit of the rewrite",
        SECOND,
        Duration::from_secs(60),
        || drives::commits(&machine.engine, DRIVE).len() == commits + 3,
    );
    // Sent again unchanged, the first commit is the one the server applied;
    // the rewrite's commit then names the content it replaced. Sent as one
    // new commit, the rewrite would name no base and be rejected.
    let all = drives::commits(&machine.engine, DRIVE);
    assert_eq!(all[commits], all[commits + 1], "{all:?}");
    assert_eq!(
        all[commits + 2],
        json!([{
            "path": "c.txt", "change": "write", "base_sha256": sha256(b"c.txt"),
            "content_sha256": sha256(b"again"), "executable": false,
        }])
    );
    let statuses: Vec<Value> = sent(&machine, "commit")
        .iter()
        .skip(commits)
        .map(|commit| commit["status"].clone())
        .collect();
    assert_eq!(statuses, vec![json!(200); 3], "a commit was rejected");
    let files = drives::files(&machine.engine, DRIVE);
    assert!(
        files.contains(&("c.txt".to_owned(), sha256(b"again"))),
        "{files:?}"
    );
    case.observe("after the next start and the Session's new preparation the App Server sent the lost commit again with the same changes, then committed the rewrite on the content the first commit wrote; the drive holds the rewrite");
    case.pass();
}

/// The drive sync reports the server has received.
fn drive_sync_reports(machine: &DriveMachine) -> Vec<Value> {
    machine
        .engine
        .received("drive-sync")
        .into_iter()
        .map(|received| received["body"].clone())
        .collect()
}

#[test]
fn a_rejection_after_its_commands_result_is_reported_again_after_a_kill_until_acknowledged() {
    let record = tetral_tests::record!();
    let mut machine = {
        let mut case = record.case("a Session with one read_write drive");
        let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
        case.pass();
        machine
    };

    let mut case = record.case(
        "a file whose commit is rejected after its command's result, the report of it never acknowledged, and then a kill of the App Server",
    );
    written_while_uploads_fail(&machine, BINDING, "n.txt");
    answer_fault(&machine.engine, json!({"action": "clear"}));
    answer_fault(
        &machine.engine,
        json!({"endpoint": "drive-sync", "action": "drop", "count": 1000}),
    );
    // Another writer's file of the same name: the command's write names no
    // content it replaces.
    commit(&machine.engine, DRIVE, &[write("n.txt", "theirs")]);
    machine.advance_until(
        "the report of the rejection",
        Duration::from_secs(5),
        Duration::from_secs(120),
        || !drive_sync_reports(&machine).is_empty(),
    );
    let first = drive_sync_reports(&machine)[0].clone();
    let rejected = first["drive_sync"]["rejected"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        first["task_id"] == "write_n.txt"
            && first["binding_id"] == BINDING
            && rejected.len() == 1
            && rejected[0]["path"]
                .as_str()
                .is_some_and(|path| path.ends_with(&format!("/drives/{DRIVE}/n.txt"))),
        "{first}"
    );
    machine.kill_and_restart(|_| {});
    answer_fault(&machine.engine, json!({"action": "clear"}));
    prepared_again(&machine, "bind_2");
    let again = || -> Vec<Value> {
        drive_sync_reports(&machine)
            .into_iter()
            .filter(|report| report["binding_id"] == "bind_2")
            .collect()
    };
    wait_until("the report on the new binding", || !again().is_empty());
    let second = again()[0].clone();
    assert_eq!(
        (
            &second["task_id"],
            &second["session_id"],
            &second["drive_sync"]
        ),
        (
            &first["task_id"],
            &first["session_id"],
            &first["drive_sync"]
        ),
        "{second}"
    );
    assert_ne!(second["connection_id"], first["connection_id"], "{second}");
    machine.answers_settled();
    case.observe(format!(
        "the rejection of n.txt was reported for the task that wrote it; after the kill and the Session's new preparation the same drive_sync was reported on the new connection and binding: {}",
        second["drive_sync"]
    ));
    case.pass();

    let mut case = record.case("the acknowledged report and then the connection's end");
    machine.end_connection();
    machine.reconnect(Duration::from_millis(1_200));
    prepared_again(&machine, "bind_3");
    machine.run_in(SESSION, "bind_3", "after_the_report", "true");
    let later: Vec<Value> = drive_sync_reports(&machine)
        .into_iter()
        .filter(|report| report["binding_id"] == "bind_3")
        .collect();
    assert_eq!(again().len(), 1, "a report after its acknowledgement");
    assert!(later.is_empty(), "{later:?}");
    case.observe("on the next connection a command ran and no drive sync report was sent");
    case.pass();
}

/// A process outside the CLI, killed once the test is over, however it
/// ends. A process that waits in a read of the mount ends only with the
/// mount, so nothing waits for it here.
#[cfg(target_os = "linux")]
struct Outside(std::process::Child);

#[cfg(target_os = "linux")]
impl Outside {
    /// Runs `dd` with `arguments` outside the CLI, its stderr kept.
    fn dd(arguments: &[String]) -> Outside {
        Outside(
            std::process::Command::new("dd")
                .args(arguments)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("dd runs"),
        )
    }

    fn running(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }

    /// The exit code and stderr of the process once it has ended.
    fn ended(mut self) -> (Option<i32>, String) {
        use std::io::Read;
        let status = self.0.wait().unwrap();
        let mut stderr = String::new();
        self.0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        (status.code(), stderr)
    }
}

#[cfg(target_os = "linux")]
impl Drop for Outside {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

// `O_DIRECT` reads pass the kernel's page cache, so each asks the mount.
#[cfg(target_os = "linux")]
#[test]
fn a_connections_end_fails_the_reads_waiting_for_a_download_and_the_next_read_resumes_it() {
    /// The bytes of the download that arrive before it is held.
    const PARTIAL: u64 = (1 << 20) + 4097;
    const EIO: &str = "Input/output error";
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a download held after its first 1,052,673 bytes while one process reads the file with O_DIRECT and another overwrites a later block, and then the connection's end",
    );
    let content: String = (0..8_usize << 20)
        .map(|at| char::from(b'a' + u8::try_from((at * 7 + (at >> 13)) % 26).unwrap()))
        .collect();
    let sum = sha256(content.as_bytes());
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(
            engine,
            DRIVE,
            &[write("big.bin", &content), write("small.txt", "small")],
        );
    });
    assert_eq!(
        machine.output(&format!("cat {}", in_drive(DRIVE, "small.txt"))),
        "small"
    );
    let big = machine.mount(DRIVE).join("big.bin");
    let downloads = || -> Vec<Value> {
        sent(&machine, "download_content")
            .into_iter()
            .filter(|request| request["content_sha256"] == sum.as_str())
            .collect()
    };
    let passed = || machine.engine.state()["throttled"][0]["read"].as_u64();
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "download_content", "content_sha256": sum,
            "action": "throttle", "bytes_per_second": PARTIAL, "count": 1,
        }),
    );
    let mut read = Outside::dd(&[
        format!("if={}", big.display()),
        "of=/dev/null".to_owned(),
        "bs=1M".to_owned(),
        "iflag=direct".to_owned(),
    ]);
    wait_until("the download begun", || passed().is_some());
    // The server writes the content by its own clock: one second's worth.
    machine.engine.advance(SECOND);
    wait_until("the first bytes of the download", || {
        passed() == Some(PARTIAL)
    });
    let mut overwrite = Outside::dd(&[
        "if=/dev/zero".to_owned(),
        format!("of={}", big.display()),
        "bs=4096".to_owned(),
        "count=1".to_owned(),
        "seek=1024".to_owned(),
        "conv=notrunc".to_owned(),
    ]);
    std::thread::sleep(PAUSE);
    assert!(
        read.running() && overwrite.running(),
        "a process ended while the download was held"
    );

    machine.end_connection();
    for (name, mut process) in [("read", read), ("write", overwrite)] {
        wait_until(&format!("the waiting {name} ended"), || !process.running());
        let (code, stderr) = process.ended();
        assert!(
            code == Some(1) && stderr.contains(EIO),
            "the waiting {name} ended {code:?}: {stderr}"
        );
    }
    let unbound = std::fs::read(&big).expect_err("a read of big.bin with no connection");
    assert_eq!(unbound.raw_os_error(), Some(5), "{unbound}");
    assert_eq!(
        std::fs::read_to_string(machine.mount(DRIVE).join("small.txt")).unwrap(),
        "small"
    );
    case.observe("the waiting O_DIRECT read and the block write each ended 1 with an input/output error, a new read of big.bin failed with EIO and small.txt, read before, was read");
    case.pass();

    let mut case = record
        .case("the Session prepared again on the next connection, and then a read of the file");
    machine.reconnect(Duration::from_millis(1_200));
    prepared_again(&machine, "bind_2");
    let before = downloads().len();
    for _ in 0..3 {
        machine.step(SECOND);
    }
    std::thread::sleep(PAUSE);
    assert_eq!(downloads().len(), before, "a download before any read");
    assert_eq!(sha256(&std::fs::read(&big).unwrap()), sum);
    let resumed = downloads().split_off(before);
    assert_eq!(resumed.len(), 1, "{resumed:?}");
    let from: u64 = resumed[0]["range"]
        .as_str()
        .and_then(|range| {
            range
                .strip_prefix("bytes=")?
                .strip_suffix('-')?
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("no open range in {}", resumed[0]));
    assert!(0 < from && from <= PARTIAL, "{}", resumed[0]);
    assert!(
        drives::files(&machine.engine, DRIVE).contains(&("big.bin".to_owned(), sum.clone())),
        "the drive's big.bin changed"
    );
    case.observe(format!(
        "three seconds after the preparation no download had been sent; the read sent one with Range {} and read the content the drive holds, which the failed write left unchanged",
        resumed[0]["range"]
    ));
    case.pass();
}
