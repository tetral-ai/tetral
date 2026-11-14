//! Layer 2. Drive transfer of the real App Server against the reference control
//! server, through real mounts: what a command's writes become on the wire,
//! and what a command reads after another writer's commits.

use serde_json::json;
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, READ_WRITE, commit, delete, in_drive, outcome, sha256, write,
};
use tetral_tests::machine::{INSTANCE, seconds};
use tetral_tests::tasks::{BINDING, SESSION};

#[test]
fn one_command_writing_two_files_uploads_both_and_then_commits_them_together() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that writes a.txt and b.txt into a read_write drive, then another writer's commit, then a command that overwrites a.txt",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);

    let terminal = machine.command(&format!(
        "printf one > {} && printf two > {}",
        in_drive(DRIVE, "a.txt"),
        in_drive(DRIVE, "b.txt")
    ));
    let (exit, _, sync) = outcome(&terminal);
    assert_eq!(exit, 0, "{terminal}");
    assert_eq!(sync["committed"], 2, "{terminal}");
    assert_eq!(sync["not_synced"], json!([]), "{terminal}");
    assert_eq!(sync["rejected"], json!([]), "{terminal}");
    let uploads = drives::requests(&machine.engine, "upload_content", DRIVE);
    let uploaded: Vec<&str> = uploads
        .iter()
        .map(|upload| upload["content_sha256"].as_str().unwrap())
        .collect();
    assert_eq!(uploaded.len(), 2, "{uploads:?}");
    assert!(
        uploaded.contains(&sha256(b"one").as_str()) && uploaded.contains(&sha256(b"two").as_str())
    );
    assert!(uploads.iter().all(|upload| upload["status"] == 200));
    // A commit that named content the drive did not hold yet would have
    // been answered `409 content_missing`.
    let commits = drives::requests(&machine.engine, "commit", DRIVE);
    assert_eq!(commits.len(), 1, "{commits:?}");
    assert_eq!(commits[0]["status"], 200);
    assert_eq!(
        drives::commits(&machine.engine, DRIVE),
        [json!([
            {"change": "write", "path": "a.txt", "base_sha256": null,
             "content_sha256": sha256(b"one"), "executable": false},
            {"change": "write", "path": "b.txt", "base_sha256": null,
             "content_sha256": sha256(b"two"), "executable": false},
        ])]
    );
    case.observe("the result's drive_sync has committed 2; the server got 2 content uploads and then 1 commit holding a.txt and b.txt");

    let seq = commit(&machine.engine, DRIVE, &[write("c.txt", "three")]);
    assert_eq!(seq, 2);
    wait_until("a change listing after the machine's own commit", || {
        drives::requests(&machine.engine, "list_changes", DRIVE)
            .iter()
            .any(|listing| listing["query"]["after_seq"] == "1")
    });
    case.observe("after another writer's commit and its drive.changed, the CLI listed the changes after seq 1");

    let terminal = machine.command(&format!("printf four > {}", in_drive(DRIVE, "a.txt")));
    let (exit, _, sync) = outcome(&terminal);
    assert_eq!((exit, &sync["committed"]), (0, &json!(1)), "{terminal}");
    assert_eq!(
        drives::commits(&machine.engine, DRIVE).last().unwrap(),
        &json!([{"change": "write", "path": "a.txt", "base_sha256": sha256(b"one"),
                "content_sha256": sha256(b"four"), "executable": false}])
    );
    case.observe("a command that overwrote a.txt got committed 1, and its commit named the version it replaced as its base");
    case.pass();
}

#[test]
fn a_command_dispatched_after_a_commit_reads_it_also_after_a_resume() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "commands that read a file another writer commits before each of them, the last commit made while the machine has no stream",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(
            engine,
            DRIVE,
            &[
                write("f.txt", "v1"),
                write("skill/a.md", "a1"),
                write("skill/b.md", "b1"),
                write("skill/c.md", "c1"),
            ],
        );
    });
    let read = || machine.output(&format!("cat {}", in_drive(DRIVE, "f.txt")));
    assert_eq!(read(), "v1");
    // Removed and bound again, so that the commit's notice must reach the
    // drive's second mount.
    let removed = machine.change_drives(SESSION, BINDING, &[]);
    machine.applied(&removed);
    let added = machine.change_drives(SESSION, BINDING, &[(DRIVE, READ_WRITE)]);
    machine.applied(&added);
    // Read once more, so that v2 cannot arrive in the second mount's first
    // listing.
    assert_eq!(read(), "v1");
    commit(&machine.engine, DRIVE, &[write("f.txt", "v2")]);
    assert_eq!(read(), "v2");
    case.observe("after the drive was removed and bound again, the command dispatched after the commit of v2 read v2");

    // Listed and read first, so that a Mac's NFS client holds the folder
    // and the files that the commit then changes.
    let look = format!("cd {} && ls && cat a.md b.md", in_drive(DRIVE, "skill"));
    assert_eq!(machine.output(&look), "a.md\nb.md\nc.md\na1b1");
    commit(
        &machine.engine,
        DRIVE,
        &[
            write("skill/a.md", "a2"),
            write("skill/b.md", "b2"),
            delete("skill/c.md"),
            write("skill/d.md", "d1"),
        ],
    );
    assert_eq!(machine.output(&look), "a.md\nb.md\nd.md\na2b2");
    case.observe(
        "one commit that rewrote two files a command had read, deleted a third and added a fourth was whole before the next command, in the folder's listing too",
    );

    // The resumed stream connects only once the server's clock moves, so the
    // commit below finds no stream to send its notice on.
    let attempts = machine.machine().attempts();
    machine.engine.control(
        "events",
        json!({"mode": "delay_connected", "delay_ms": 1_000, "count": 1}),
    );
    machine
        .engine
        .control("break_stream", json!({"instance_id": INSTANCE}));
    machine.machine().wait_for("resuming");
    machine.machine().seams.advance(seconds(1.2));
    machine
        .machine()
        .seams
        .wait_for_count("connection.attempt", attempts + 1);
    let notices = notices(&machine);
    commit(&machine.engine, DRIVE, &[write("f.txt", "v3")]);
    assert_eq!(
        notices,
        self::notices(&machine),
        "no stream carried the commit's notice"
    );
    machine.engine.advance(seconds(1.0));
    machine.machine().wait_for("online");
    assert_eq!(read(), "v3");
    assert_eq!(self::notices(&machine), notices + 1);
    case.observe(
        "after a resume whose stream began with the drive's notice, the next command read v3",
    );
    case.pass();
}

/// The `drive.changed` events the server has written.
fn notices(machine: &DriveMachine) -> usize {
    machine.engine.state()["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["type"] == "drive.changed")
        .count()
}
