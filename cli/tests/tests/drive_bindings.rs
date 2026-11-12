//! Layer 2. Drive bindings of the real App Server against the reference control
//! server, through real mounts: what a binding's access, its excluded paths
//! and its changes allow a command, and what stays on the machine.

use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, FINAL_UPLOAD, READ_ONLY, READ_WRITE, commit, in_drive, outcome,
    sha256, write,
};
use tetral_tests::machine::answer_fault;
use tetral_tests::tasks::{BINDING, SESSION};

const OTHER: &str = "drive_b";

fn paths(machine: &DriveMachine, drive: &str) -> Vec<String> {
    drives::files(&machine.engine, drive)
        .into_iter()
        .map(|(path, _)| path)
        .collect()
}

fn no_requests(machine: &DriveMachine, endpoint: &str, drive: &str) {
    assert_eq!(
        drives::requests(&machine.engine, endpoint, drive),
        [] as [Value; 0]
    );
}

#[test]
fn a_write_into_a_read_only_drive_fails_as_a_read_only_file_system_and_sends_nothing() {
    let record = tetral_tests::record!();
    let mut case = record
        .case("a command that reads a file of a read_only drive and writes a new one into it");
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("kept.txt", "kept")]);
    });
    let terminal = machine.command(&format!(
        "cat {}; echo; printf x > {}; echo \"exit=$?\"",
        in_drive(DRIVE, "kept.txt"),
        in_drive(DRIVE, "new.txt")
    ));
    let (_, stdout, _) = outcome(&terminal);
    assert_eq!(stdout, "kept\nexit=1\n", "{terminal}");
    let stderr = terminal["result"]["result"]["stderr"]["text"]
        .as_str()
        .unwrap();
    assert!(
        stderr.contains("new.txt: Read-only file system"),
        "{terminal}"
    );
    no_requests(&machine, "upload_content", DRIVE);
    no_requests(&machine, "commit", DRIVE);
    assert_eq!(paths(&machine, DRIVE), ["kept.txt"]);
    case.observe(format!(
        "the command read kept.txt and its write failed: {stderr:?}; the CLI sent no upload and no commit"
    ));
    case.pass();
}

#[test]
fn a_drive_added_to_an_idle_session_is_applied_and_read_by_the_next_command() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a Session prepared without drives, a session.drives that adds one, and a command that reads it",
    );
    let machine = DriveMachine::start(&mut case, &[]);
    commit(&machine.engine, DRIVE, &[write("seed.txt", "seed")]);
    let issued = machine.change_drives(SESSION, BINDING, &[(DRIVE, READ_WRITE)]);
    let applied = machine.applied(&issued);
    assert_eq!(applied.get("lost_changes"), None, "{applied}");
    assert_eq!(
        machine.output(&format!("cat {}", in_drive(DRIVE, "seed.txt"))),
        "seed"
    );
    case.observe("the session.drives was reported applied and the next command read seed.txt from the added drive");
    case.pass();
}

/// A removed drive is no drive that failed to mount and leaves nothing
/// pending: no Session report names `removed` in `drive_mount_failures`,
/// and `terminal`, of a command after the change, lists no unmounted drive
/// and nothing not synced.
fn nothing_left_of(machine: &DriveMachine, removed: &str, terminal: &Value) {
    let sync = outcome(terminal).2;
    assert!(
        sync.is_null() || (sync["not_synced"] == json!([]) && sync.get("unmounted").is_none()),
        "{terminal}"
    );
    for report in machine.engine.received("session-status") {
        let failed = &report["body"]["drive_mount_failures"];
        assert!(
            failed
                .as_array()
                .is_none_or(|failed| failed.iter().all(|failure| failure["drive_id"] != removed)),
            "{report}"
        );
    }
}

#[test]
fn a_binding_change_commits_pending_changes_before_its_report_and_the_next_command_follows_it() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a session.drives that removes one read_write drive and makes another read_only, each holding a change written outside any command; then a command",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE), (OTHER, READ_WRITE)]);
    for drive in [DRIVE, OTHER] {
        std::fs::write(machine.mount(drive).join("a.txt"), "a").unwrap();
    }
    let issued = machine.change_drives(SESSION, BINDING, &[(OTHER, READ_ONLY)]);
    let applied = machine.applied(&issued);
    assert_eq!(applied.get("lost_changes"), None, "{applied}");
    // The server stores the report after it answered the commits: a commit
    // that came later was refused, since the report ends the old drive set.
    for drive in [DRIVE, OTHER] {
        let commits = drives::requests(&machine.engine, "commit", drive);
        assert_eq!(commits.len(), 1, "{commits:?}");
        assert_eq!(commits[0]["status"], 200);
        assert_eq!(paths(&machine, drive), ["a.txt"]);
    }
    let requests = machine.engine.state()["requests"]
        .as_array()
        .unwrap()
        .clone();
    let position = |found: &dyn Fn(&Value) -> bool| requests.iter().position(found).unwrap();
    let report = position(&|request| {
        request["endpoint"] == "session-status" && request["message_id"] == applied["message_id"]
    });
    for drive in [DRIVE, OTHER] {
        assert!(
            position(&|request| request["endpoint"] == "commit" && request["drive_id"] == drive)
                < report
        );
    }
    case.observe("both pending changes were committed before the applied report");

    let after = machine.command(&format!(
        "test -e {} && echo present || echo absent; (printf x > {}) 2>/dev/null && echo writable || echo read-only",
        in_drive(DRIVE, "a.txt"),
        in_drive(OTHER, "x")
    ));
    assert_eq!(outcome(&after).1, "absent\nread-only\n", "{after}");
    assert_eq!(
        drives::requests(&machine.engine, "upload_content", OTHER).len(),
        1
    );
    nothing_left_of(&machine, DRIVE, &after);
    case.observe("the next command ran without the removed drive, could not write the other and listed no unmounted drive and nothing not synced; no report names the removed drive as one that failed to mount");
    case.pass();
}

#[test]
fn a_binding_change_after_a_kill_and_a_failed_preparation_commits_or_lists_each_pending_change() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "an App Server killed with a change pending in each of two drives and started without the Session's working directory; with the directory back the Session is prepared again, and a session.drives removes the drive whose uploads fail and makes the other read_only",
    );
    let both = [(DRIVE, READ_WRITE), (OTHER, READ_WRITE)];
    let mut machine = DriveMachine::start(&mut case, &both);
    for drive in [DRIVE, OTHER] {
        std::fs::write(machine.mount(drive).join("c.txt"), "c").unwrap();
    }
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "upload_content", "drive_id": DRIVE, "action": "error",
            "code": "unavailable", "status": 503, "count": 10_000,
        }),
    );
    let project = machine.project.clone();
    machine.kill_and_restart(|_| std::fs::remove_dir_all(&project).unwrap());
    let failed =
        machine.session_report(&machine.prepare_with(SESSION, "bind_2", &project, &both, &[]));
    assert_eq!(
        (&failed["status"], &failed["error"]["code"]),
        (&json!("failed"), &json!("cwd_not_found")),
        "{failed}"
    );
    std::fs::create_dir(&project).unwrap();
    let prepared =
        machine.session_report(&machine.prepare_with(SESSION, "bind_3", &project, &both, &[]));
    assert_eq!(
        (&prepared["status"], prepared.get("lost_changes")),
        (&json!("prepared"), None),
        "{prepared}"
    );
    machine.answers_settled();

    // The removed drive's upload never passes: its final upload ends with
    // the 30th second.
    let seams = &machine.machine().seams;
    let final_uploads = seams.count(FINAL_UPLOAD);
    let change = machine.change_drives(SESSION, "bind_3", &[(OTHER, READ_ONLY)]);
    seams.wait_for_count(FINAL_UPLOAD, final_uploads + 1);
    // The other drive's change is committed while the clock stands.
    wait_until("the commit of the drive made read_only", || {
        drives::commits(&machine.engine, OTHER).len() == 1
    });
    for _ in 0..30 {
        machine.step(Duration::from_secs(1));
    }
    let applied = machine.applied(&change);
    assert_eq!(
        applied["lost_changes"],
        json!([{
            "request_message_id": change, "drive_id": DRIVE,
            "paths": ["c.txt"], "paths_total": 1,
        }]),
        "{applied}"
    );
    assert_eq!(paths(&machine, DRIVE), Vec::<String>::new());
    assert_eq!(paths(&machine, OTHER), ["c.txt"]);
    let requests = machine.engine.state()["requests"]
        .as_array()
        .unwrap()
        .clone();
    let position = |found: &dyn Fn(&Value) -> bool| requests.iter().position(found).unwrap();
    assert!(
        position(&|request| request["endpoint"] == "commit" && request["drive_id"] == OTHER)
            < position(&|request| {
                request["endpoint"] == "session-status"
                    && request["message_id"] == applied["message_id"]
            })
    );
    let after = machine.run_in(
        SESSION,
        "bind_3",
        "after_the_change",
        &format!(
            "test -e {} && echo present || echo absent; (printf x > {}) 2>/dev/null && echo writable || echo read-only",
            in_drive(DRIVE, "c.txt"),
            in_drive(OTHER, "x")
        ),
    );
    let (_, stdout, _) = outcome(&after);
    assert_eq!(stdout, "absent\nread-only\n", "{after}");
    nothing_left_of(&machine, DRIVE, &after);
    case.observe(format!(
        "the preparation without the directory failed with cwd_not_found and the next was prepared; the kept drive's change was committed before the applied report, which lists the removed drive's as lost: {}; the next command ran without the removed drive, could not write the other and listed nothing not synced",
        applied["lost_changes"]
    ));
    case.pass();
}

#[test]
fn an_excluded_folder_is_left_out_of_its_drive_only_also_for_later_files() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "two drives that each hold datasets/a.csv and keep.txt, one bound with drive_exclude datasets/, and files another writer adds to it later",
    );
    let seed = [write("datasets/a.csv", "a"), write("keep.txt", "keep")];
    let machine = DriveMachine::start_with(
        &mut case,
        &[(DRIVE, READ_ONLY), (OTHER, READ_ONLY)],
        &[(DRIVE, &["datasets/"])],
        |engine| {
            commit(engine, DRIVE, &seed);
            commit(engine, OTHER, &seed);
        },
    );
    commit(
        &machine.engine,
        DRIVE,
        &[
            write("later.txt", "later"),
            write("datasets/later.csv", "later"),
        ],
    );
    let mut listed: Vec<String> = machine
        .output("cd \"$TETRAL_DRIVES\" && find . -mindepth 1 | sed 's|^\\./||'")
        .lines()
        .map(str::to_owned)
        .collect();
    listed.sort();
    let mut expected = vec![
        DRIVE.to_owned(),
        format!("{DRIVE}/keep.txt"),
        format!("{DRIVE}/later.txt"),
        OTHER.to_owned(),
        format!("{OTHER}/datasets"),
        format!("{OTHER}/datasets/a.csv"),
        format!("{OTHER}/keep.txt"),
    ];
    expected.sort();
    assert_eq!(listed, expected);
    case.observe(format!("the mounts list {listed:?}"));
    case.pass();
}

#[test]
fn nothing_can_be_made_under_an_excluded_folder_and_nothing_of_it_is_sent() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that makes the excluded folder, creates and appends files in it and renames a file into it",
    );
    let machine = DriveMachine::start_with(
        &mut case,
        &[(DRIVE, READ_WRITE)],
        &[(DRIVE, &["datasets/"])],
        |engine| {
            commit(
                engine,
                DRIVE,
                &[write("datasets/a.csv", "a"), write("notes.md", "notes")],
            );
        },
    );
    let script = format!(
        r#"cd {} || exit 1
ls -A
mkdir datasets 2>&1; echo "exit=$?"
touch datasets/b.csv 2>&1; echo "exit=$?"
printf more | tee -a datasets/a.csv 2>&1 >/dev/null; echo "exit=$?"
mv notes.md datasets/notes.md 2>&1; echo "exit=$?"
ls -A"#,
        in_drive(DRIVE, "")
    );
    let output = machine.output(&script);
    let lines: Vec<&str> = output.lines().collect();
    let expected = [
        "notes.md",
        "Permission denied",
        "exit=1",
        "No such file or directory",
        "exit=1",
        "No such file or directory",
        "exit=1",
        "No such file or directory",
        "exit=1",
        "notes.md",
    ];
    assert_eq!(lines.len(), expected.len(), "{output}");
    for (line, end) in lines.iter().zip(expected) {
        assert!(line.ends_with(end), "{output}");
    }
    no_requests(&machine, "upload_content", DRIVE);
    no_requests(&machine, "commit", DRIVE);
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [
            ("datasets/a.csv".to_owned(), sha256(b"a")),
            ("notes.md".to_owned(), sha256(b"notes")),
        ]
    );
    case.observe(format!(
        "the command wrote {output:?}; the CLI sent no upload and no commit and the drive is unchanged"
    ));
    case.pass();
}

#[test]
fn a_command_reads_its_sessions_drive_but_not_the_cli_credentials() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "one command that reads the CLI's credential file and a file of its Session's drive, both under the state root",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("f.txt", "drive")]);
    });
    let credentials = machine.root.join("credentials/credentials.json");
    assert!(std::fs::metadata(&credentials).unwrap().len() > 0);
    let terminal = machine.command(&format!(
        "cat '{}' >/dev/null; echo \"exit=$?\"; cat {}",
        credentials.display(),
        in_drive(DRIVE, "f.txt")
    ));
    assert_eq!(outcome(&terminal).1, "exit=1\ndrive", "{terminal}");
    case.observe(
        "the read of the credential file exited 1 and the same command read f.txt of the drive",
    );
    case.pass();
}

#[test]
fn a_git_repository_made_in_a_drive_is_listed_once_and_stays_on_the_machine() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that makes a Git repository with a file in a drive, then one that writes a file beside it",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    let created = machine.command(&format!(
        "cd {} && git init -q repo && printf x > repo/f.txt",
        in_drive(DRIVE, "")
    ));
    let (exit, _, sync) = outcome(&created);
    assert_eq!(exit, 0, "{created}");
    let listed = sync["not_synced"].as_array().expect("not_synced");
    assert_eq!(listed.len(), 1, "{created}");
    assert!(
        listed[0]["path"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/drives/{DRIVE}/repo")),
        "{created}"
    );
    assert_eq!(listed[0]["reason"], "git_repository", "{created}");

    let next = machine.command(&format!("printf y > {}", in_drive(DRIVE, "other.txt")));
    let (_, _, sync) = outcome(&next);
    assert_eq!(sync["committed"], 1, "{next}");
    assert_eq!(sync["not_synced"], json!([]), "{next}");
    assert_eq!(paths(&machine, DRIVE), ["other.txt"]);
    assert_eq!(
        drives::requests(&machine.engine, "upload_content", DRIVE).len(),
        1
    );
    case.observe("the first result lists repo once as git_repository, the next lists nothing; the drive holds only other.txt");
    case.pass();
}

#[test]
fn removing_a_git_marker_uploads_the_edits_made_under_it_against_their_recorded_bases() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "two files edited under a Git marker, the marker removed, and another writer's change of one of them before its upload is let through",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(
            engine,
            DRIVE,
            &[write("repo/a.txt", "a1"), write("repo/b.txt", "b1")],
        );
    });
    let repo = in_drive(DRIVE, "repo");
    let edited = machine.command(&format!(
        "git init -q {repo} && printf a-local > {repo}/a.txt && printf b-local > {repo}/b.txt"
    ));
    assert_eq!(outcome(&edited).2["committed"], 0, "{edited}");
    no_requests(&machine, "upload_content", DRIVE);

    answer_fault(
        &machine.engine,
        json!({"endpoint": "upload_content", "action": "hold_request", "count": 2}),
    );
    let removing = machine.start_command(&format!("rm -rf {repo}/.git"));
    wait_until("both edits' uploads held", || {
        drives::held(&machine.engine) == 2
    });
    commit(&machine.engine, DRIVE, &[write("repo/b.txt", "b-remote")]);
    answer_fault(&machine.engine, json!({"action": "release"}));
    let removed = machine.terminal(&removing);
    let (_, _, sync) = outcome(&removed);
    assert_eq!(sync["committed"], 1, "{removed}");
    let rejected = sync["rejected"].as_array().expect("rejections");
    assert_eq!(rejected.len(), 1, "{removed}");
    assert!(
        rejected[0]["path"]
            .as_str()
            .unwrap()
            .ends_with("repo/b.txt"),
        "{removed}"
    );
    assert_eq!(rejected[0]["reason"], "conflict", "{removed}");
    assert_eq!(
        machine.output(&format!("cat {repo}/a.txt {repo}/b.txt")),
        "a-localb-remote"
    );
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [
            ("repo/a.txt".to_owned(), sha256(b"a-local")),
            ("repo/b.txt".to_owned(), sha256(b"b-remote")),
        ]
    );
    case.observe("removing .git uploaded both edits with no new edit: a.txt was committed, b.txt was rejected as a conflict and replaced by the drive's version");
    case.pass();
}
