//! Layer 2. Uploads of the real App Server against the reference control
//! server at the server's limits, through real mounts: a file over the size
//! limit, a commit whose accepted copy is still verified when the commit's
//! time has passed, an upload slower than a result waits, an upload the
//! workspace's budget refuses for a time, and more changes than a commit
//! holds.

use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::{PAUSE, wait_until};
use tetral_tests::drives::{
    self, COMMIT_VERIFICATION, DRIVE, DriveMachine, READ_WRITE, commit, in_drive, outcome, sha256,
    write,
};
use tetral_tests::machine::answer_fault;

/// What the work on a file of hundreds of megabytes may take.
const LARGE_WORK: Duration = Duration::from_secs(300);

const SECOND: Duration = Duration::from_secs(1);

/// The uploads the server has answered for the drive.
fn uploads(machine: &DriveMachine) -> Vec<Value> {
    drives::requests(&machine.engine, "upload_content", DRIVE)
}

/// The `not_synced` entry of `path` in a result's `drive_sync`.
fn not_synced<'a>(terminal: &'a Value, path: &str) -> Option<&'a Value> {
    terminal["result"]["drive_sync"]["not_synced"]
        .as_array()?
        .iter()
        .find(|entry| {
            entry["path"]
                .as_str()
                .is_some_and(|listed| listed.ends_with(&format!("/drives/{DRIVE}/{path}")))
        })
}

/// The command that makes `to` of the drive a file of `bytes` zeros. It
/// sets the file's size and writes no byte: a write through a mount is
/// stored before it returns, and a file of this size written a megabyte at
/// a time takes some fourteen seconds.
fn zeros(bytes: u64, to: &str) -> String {
    format!(
        "dd if=/dev/zero of={} bs=1 count=0 seek={bytes} 2>/dev/null",
        in_drive(DRIVE, to)
    )
}

/// Whether a terminal report of `task` has come.
fn ended(machine: &DriveMachine, task: &str) -> bool {
    machine
        .reports(task)
        .iter()
        .any(|report| report["phase"] == "terminal")
}

/// The drive's files as `(path, size_bytes)`, in path order.
fn sizes(machine: &DriveMachine) -> Vec<(String, u64)> {
    drives::state(&machine.engine, DRIVE)["files"]
        .as_array()
        .expect("files")
        .iter()
        .map(|file| {
            (
                file["path"].as_str().unwrap().to_owned(),
                file["size_bytes"].as_u64().unwrap(),
            )
        })
        .collect()
}

#[test]
fn a_file_over_the_size_limit_is_rejected_by_the_servers_rule_and_one_at_the_limit_is_committed() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that writes a file of 500,000,001 bytes and one of 500,000,000, with ten seconds passing between the commit's answer and the verification of the accepted copy",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    let seams = &machine.machine().seams;
    seams.hold(COMMIT_VERIFICATION);
    let writing = machine.start_command(&format!(
        "{} && {}",
        zeros(500_000_001, "over.bin"),
        zeros(500_000_000, "max.bin")
    ));
    // The commit's task waits between the commit's answer and the
    // verification of the accepted copy: the ten seconds the commit had for
    // its answer pass there, with the answer given and nothing verified.
    tetral_tests::app_server::wait_until_within(LARGE_WORK, "the commit's answer", || {
        seams.waiting(COMMIT_VERIFICATION) == 1
    });
    assert_eq!(
        drives::requests(&machine.engine, "commit", DRIVE)
            .iter()
            .map(|commit| commit["status"].clone())
            .collect::<Vec<_>>(),
        [json!(200)]
    );
    machine.step(Duration::from_secs(10));
    std::thread::sleep(PAUSE);
    assert!(
        !ended(&machine, &writing),
        "a result before the accepted copy was verified"
    );
    seams.release(COMMIT_VERIFICATION);
    let terminal = machine.report_within(LARGE_WORK, &writing, "terminal");
    let (exit, _, sync) = outcome(&terminal);
    assert_eq!(exit, 0, "{terminal}");
    assert_eq!(
        (&sync["committed"], &sync["not_synced"]),
        (&json!(1), &json!([])),
        "{terminal}"
    );
    let rejected = sync["rejected"].as_array().cloned().unwrap_or_default();
    assert_eq!(rejected.len(), 1, "{terminal}");
    assert!(
        rejected[0]["path"]
            .as_str()
            .is_some_and(|path| path.ends_with(&format!("/drives/{DRIVE}/over.bin"))),
        "{terminal}"
    );
    assert_eq!(
        (
            &rejected[0]["change"],
            &rejected[0]["reason"],
            &rejected[0]["rule"]
        ),
        (
            &json!("write"),
            &json!("limit"),
            &json!("a file holds at most 500,000,000 bytes")
        ),
        "{terminal}"
    );
    let answered: Vec<(Value, Value)> = uploads(&machine)
        .into_iter()
        .map(|upload| (upload["status"].clone(), upload["body_bytes"].clone()))
        .collect();
    assert!(
        answered.contains(&(json!(413), Value::Null))
            && answered.contains(&(json!(200), json!(500_000_000)))
            && answered.len() == 2,
        "{answered:?}"
    );
    assert_eq!(sizes(&machine), [("max.bin".to_owned(), 500_000_000)]);
    case.observe(format!(
        "the upload of over.bin was answered 413 with no byte of it read, and the result listed {}; max.bin was uploaded and committed with 500,000,000 bytes, and the result counted it with the App Server's clock ten seconds past the commit's answer",
        rejected[0]
    ));
    case.pass();
}

#[test]
fn an_upload_slower_than_a_result_waits_is_listed_uploading_and_committed_later_on_its_own() {
    const RATE: u64 = 4_000_000;
    const SIZE: u64 = 200_000_000;
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that writes a small file and one of 200,000,000 bytes, whose upload the server reads at 4,000,000 bytes a second",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "upload_content", "drive_id": DRIVE, "action": "throttle",
            "bytes_per_second": RATE, "count": 2,
        }),
    );
    let writing = machine.start_command(&format!(
        "printf small > {} && {}",
        in_drive(DRIVE, "small.txt"),
        zeros(SIZE, "large.bin")
    ));
    // The bytes the server has read of the large upload. It reads at the
    // pace of its own clock, and the App Server ends an upload that sends no
    // byte for ten seconds of its clock: both clocks move a second at a
    // time, the App Server's once the server has read that second's bytes.
    let read = || -> u64 {
        machine.engine.state()["throttled"]
            .as_array()
            .map_or(0, |reads| {
                reads
                    .iter()
                    .filter_map(|one| one["read"].as_u64())
                    .max()
                    .unwrap_or(0)
            })
    };
    let throttled = || {
        machine.engine.state()["throttled"]
            .as_array()
            .map_or(0, Vec::len)
    };
    tetral_tests::app_server::wait_until_within(LARGE_WORK, "both uploads' requests", || {
        throttled() == 2
    });
    let seconds = std::cell::Cell::new(0);
    let second = || {
        seconds.set(seconds.get() + 1);
        machine.step_together(SECOND);
        let due = SIZE.min(RATE * seconds.get());
        wait_until("the second's bytes of the large upload", || read() >= due);
    };
    for _ in 0..29 {
        second();
    }
    std::thread::sleep(PAUSE);
    assert!(
        !ended(&machine, &writing),
        "a result before the 30th second after the command's exit"
    );
    second();
    let terminal = machine.terminal(&writing);
    let (exit, _, sync) = outcome(&terminal);
    assert_eq!((exit, &sync["committed"]), (0, &json!(1)), "{terminal}");
    let listed = not_synced(&terminal, "large.bin").unwrap_or(&Value::Null);
    assert_eq!(listed["reason"], "uploading", "{terminal}");
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("small.txt".to_owned(), sha256(b"small"))]
    );
    case.observe(format!(
        "the result came with the 30th second after the command's exit, with small.txt committed and {listed}; the server had read {} bytes of large.bin",
        read()
    ));

    // The clocks stand: a command that waited for the upload would not end.
    let next = machine.command("true");
    assert_eq!(outcome(&next).0, 0, "{next}");
    assert_eq!(drives::commits(&machine.engine, DRIVE).len(), 1);
    case.observe(format!(
        "the next command's result came with both clocks standing, with drive_sync {}",
        outcome(&next).2
    ));

    while drives::commits(&machine.engine, DRIVE).len() < 2 {
        assert!(
            seconds.get() < 90,
            "large.bin was not committed within 90 seconds of the clocks"
        );
        second();
        if read() == SIZE {
            std::thread::sleep(PAUSE);
        }
    }
    let commits = drives::commits(&machine.engine, DRIVE);
    assert_eq!(commits[1].as_array().map(Vec::len), Some(1), "{commits:?}");
    assert_eq!(commits[1][0]["path"], "large.bin", "{commits:?}");
    // The server refuses a commit that names content it has not read in
    // full, and the App Server would send it again: no commit was refused.
    let answered: Vec<Value> = drives::requests(&machine.engine, "commit", DRIVE)
        .iter()
        .map(|request| request["status"].clone())
        .collect();
    assert_eq!(
        answered,
        [200, 200],
        "a commit of large.bin before its upload ended"
    );
    assert_eq!(
        sizes(&machine),
        [("large.bin".to_owned(), SIZE), ("small.txt".to_owned(), 5)]
    );
    assert_eq!(
        machine
            .output(&format!("wc -c < {}", in_drive(DRIVE, "large.bin")))
            .trim(),
        SIZE.to_string()
    );
    case.observe(format!(
        "{} seconds of both clocks after the command's exit large.bin was committed in a commit of its own, with no tool call, and a command then read its 200,000,000 bytes",
        seconds.get()
    ));
    case.pass();
}

#[test]
fn an_upload_the_budget_refuses_is_listed_uploading_and_sent_again_after_its_retry_after() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that writes two files of one byte into a workspace whose upload budget holds one upload a minute",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    machine
        .engine
        .control("drive_budget", json!({"bytes_per_minute": 65_536}));
    let writing = machine.start_command(&format!(
        "printf a > {} && printf b > {}",
        in_drive(DRIVE, "a.txt"),
        in_drive(DRIVE, "b.txt")
    ));
    wait_until("both uploads' answers", || uploads(&machine).len() == 2);
    let first = uploads(&machine);
    let refused: Vec<&Value> = first
        .iter()
        .filter(|upload| upload["status"] == 429)
        .collect();
    assert!(
        refused.len() == 1
            && refused[0]["code"] == "rate_limited"
            && first.iter().any(|upload| upload["status"] == 200),
        "{first:?}"
    );
    let late = if refused[0]["content_sha256"] == sha256(b"a") {
        "a"
    } else {
        "b"
    };
    let sent = |content: &str| -> Vec<Value> {
        uploads(&machine)
            .into_iter()
            .filter(|upload| upload["content_sha256"] == sha256(content.as_bytes()))
            .collect()
    };

    // The budget refills by the server's clock and the App Server waits by
    // its own, so both move together. The refusal's Retry-After is the 60
    // seconds in which the budget holds an upload again.
    for _ in 0..29 {
        machine.step_together(SECOND);
    }
    std::thread::sleep(PAUSE);
    assert!(
        !ended(&machine, &writing),
        "a result before the 30th second after the command's exit"
    );
    machine.step_together(SECOND);
    let terminal = machine.terminal(&writing);
    let (exit, _, sync) = outcome(&terminal);
    assert_eq!((exit, &sync["committed"]), (0, &json!(1)), "{terminal}");
    let listed = not_synced(&terminal, &format!("{late}.txt")).unwrap_or(&Value::Null);
    assert_eq!(listed["reason"], "uploading", "{terminal}");
    case.observe(format!(
        "the upload of {late}.txt was refused 429 rate_limited; the result came with the 30th second after the command's exit, with the other file committed and {listed}"
    ));

    for _ in 30..59 {
        machine.step_together(SECOND);
    }
    std::thread::sleep(PAUSE);
    assert_eq!(
        sent(late).len(),
        1,
        "{late}.txt was sent again before its Retry-After had passed"
    );
    machine.step_together(SECOND);
    wait_until("the refused upload sent again", || sent(late).len() == 2);
    wait_until("the commit of the late file", || {
        drives::commits(&machine.engine, DRIVE).len() == 2
    });
    let again = sent(late);
    assert_eq!(again[1]["status"], 200, "{again:?}");
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [
            ("a.txt".to_owned(), sha256(b"a")),
            ("b.txt".to_owned(), sha256(b"b"))
        ]
    );
    case.observe(format!(
        "with the 60th second after its refusal and not a second sooner {late}.txt was sent again, accepted, and committed with no tool call"
    ));
    case.pass();
}

#[test]
fn more_changes_than_a_commit_holds_go_in_two_commits_with_the_deletions_first() {
    const DELETED: usize = 9_990;
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a sync point with 9,990 deleted files and 11 new ones, one change more than a commit holds",
    );
    // The new files' names sort before the deleted ones', so the order of
    // the paths alone would put the writes first.
    let name = |index: usize| format!("f{index:04}");
    let new = |index: usize| format!("a{index:02}");
    let mut seeded = 0;
    let machine =
        DriveMachine::start_seeded_in_memory(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
            let files: Vec<Value> = (0..DELETED).map(|index| write(&name(index), "x")).collect();
            seeded = commit(engine, DRIVE, &files);
        });
    // The changes are made outside any command, in an order that is not the
    // paths': the new files first and backwards, then the deletions
    // backwards.
    let mount = machine.mount(DRIVE);
    for index in (0..11).rev() {
        std::fs::write(mount.join(new(index)), new(index)).unwrap();
    }
    for index in (0..DELETED).rev() {
        std::fs::remove_file(mount.join(name(index))).unwrap();
    }

    let terminal = machine.command_within(LARGE_WORK, "true");
    let (exit, _, sync) = outcome(&terminal);
    assert_eq!(exit, 0, "{terminal}");
    assert_eq!(
        sync,
        json!({"committed": 11, "deleted": DELETED, "not_synced": [], "rejected": []}),
    );
    let commits = drives::commits(&machine.engine, DRIVE);
    let sizes: Vec<usize> = commits
        .iter()
        .map(|changes| changes.as_array().map_or(0, Vec::len))
        .collect();
    assert_eq!(sizes, [10_000, 1]);
    for (index, change) in commits[0].as_array().unwrap().iter().enumerate() {
        let want = if index < DELETED {
            (json!("delete"), json!(name(index)))
        } else {
            (json!("write"), json!(new(index - DELETED)))
        };
        assert_eq!(
            (&change["change"], &change["path"]),
            (&want.0, &want.1),
            "change {index} of the first commit"
        );
    }
    assert_eq!(
        (&commits[1][0]["change"], &commits[1][0]["path"]),
        (&json!("write"), &json!(new(10)))
    );
    assert_eq!(
        drives::state(&machine.engine, DRIVE)["seq"],
        json!(seeded + 2)
    );
    let left: Vec<(String, String)> = (0..11)
        .map(|index| (new(index), sha256(new(index).as_bytes())))
        .collect();
    assert_eq!(drives::files(&machine.engine, DRIVE), left);
    case.observe(
        "the result counted 11 committed and 9,990 deleted; the first commit held 10,000 changes, the 9,990 deletions in path order and then the writes of g00 to g09, and the next commit the write of g10",
    );
    case.pass();
}
