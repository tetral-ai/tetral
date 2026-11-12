//! Layer 2. Uploads of the real App Server against the reference control
//! server, through real mounts: when a background write is uploaded, how
//! long a result waits for uploads that do not pass and what it and the
//! next one say of them, and what a removed drive's report says of a change
//! whose upload never ended.

use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::{PAUSE, wait_until};
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, FINAL_UPLOAD, READ_WRITE, commit, in_drive, outcome, sha256, write,
};
use tetral_tests::machine::answer_fault;
use tetral_tests::tasks::{BINDING, SESSION, WAIT_CHECKED};

/// The uploads the server has answered for the drive.
fn uploads(machine: &DriveMachine) -> Vec<Value> {
    drives::requests(&machine.engine, "upload_content", DRIVE)
}

/// Whether a result's `drive_sync` lists `path` of the drive as not synced
/// for `reason`.
fn not_synced(terminal: &Value, path: &str, reason: &str) -> bool {
    outcome(terminal).2["not_synced"]
        .as_array()
        .is_some_and(|listed| {
            listed.iter().any(|entry| {
                entry["reason"] == reason
                    && entry["path"]
                        .as_str()
                        .is_some_and(|listed| listed.ends_with(&format!("/drives/{DRIVE}/{path}")))
            })
        })
}

/// Moves the App Server's clock by `step` in steps that keep the
/// connection online.
fn pass(machine: &DriveMachine, step: Duration) {
    let mut left = step;
    while !left.is_zero() {
        let part = left.min(Duration::from_secs(10));
        machine.step(part);
        left -= part;
    }
}

/// Moves the App Server's clock through the `seconds` that the result of an
/// exited command waits, and gives the result: there is none a second
/// before, and it comes with the last second. The wait must have begun.
fn result_with_second(machine: &DriveMachine, task: &str, seconds: u64) -> Value {
    pass(machine, Duration::from_secs(seconds - 1));
    std::thread::sleep(PAUSE);
    assert!(
        machine
            .reports(task)
            .iter()
            .all(|report| report["phase"] != "terminal"),
        "a result before the {seconds}th second of its wait"
    );
    machine.step(Duration::from_secs(1));
    machine.terminal(task)
}

/// [`result_with_second`] for the 30 seconds a result waits for uploads.
fn result_after_the_upload_wait(machine: &DriveMachine, task: &str) -> Value {
    result_with_second(machine, task, 30)
}

#[test]
fn a_background_write_is_uploaded_two_seconds_after_it_and_committed_without_a_tool_call() {
    let record = tetral_tests::record!();
    let mut case = record.case("a command in the background that writes a file into a drive");
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    let at = |name: &str| machine.project.join(name);
    let seams = &machine.machine().seams;
    let checked = seams.count(WAIT_CHECKED);
    let writing = machine.start_background(
        &format!(
            "while [ ! -e '{}' ]; do sleep 0.05; done; printf late > {}; touch '{}'; while [ ! -e '{}' ]; do sleep 0.05; done",
            at("write").display(),
            in_drive(DRIVE, "late.txt"),
            at("written").display(),
            at("stop").display()
        ),
        1_000,
    );
    machine.wait_for_start(checked);
    machine.step(Duration::from_secs(1));
    machine.report(&writing, "running");

    std::fs::write(at("write"), "").unwrap();
    wait_until("the background write", || at("written").exists());
    std::thread::sleep(PAUSE);
    assert!(uploads(&machine).is_empty(), "an upload with the write");
    machine.step(Duration::from_millis(1_900));
    std::thread::sleep(PAUSE);
    assert!(
        uploads(&machine).is_empty(),
        "an upload 1.9 seconds after the write"
    );
    machine.step(Duration::from_millis(100));
    wait_until("the upload", || uploads(&machine).len() == 1);
    let committed = || drives::commits(&machine.engine, DRIVE).len();
    let by = if cfg!(target_os = "macos") {
        // A Mac's mount tells no task's end, so the commit waits for a
        // sync point.
        std::thread::sleep(PAUSE);
        assert_eq!(committed(), 0, "a commit before the next sync point");
        let next = machine.command("true");
        assert_eq!(outcome(&next).2["committed"], 1, "{next}");
        "at the next command's sync point"
    } else {
        wait_until("the commit", || committed() == 1);
        "with no tool call"
    };
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("late.txt".to_owned(), sha256(b"late"))]
    );
    assert_eq!(uploads(&machine).len(), 1, "an upload with the commit");
    case.observe(format!(
        "the file's one upload came when the App Server's clock was 2 seconds past the write, not at 1.9, and its commit {by}"
    ));
    std::fs::write(at("stop"), "").unwrap();
    machine.terminal(&writing);
    case.pass();
}

#[test]
fn a_result_waits_thirty_seconds_for_uploads_that_do_not_pass_and_the_next_one_does_not() {
    let record = tetral_tests::record!();
    let mut case = record.case("a command that writes a file while every upload is answered 503");
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "upload_content", "action": "error",
            "code": "unavailable", "status": 503, "count": 1000,
        }),
    );
    let writing = machine.start_command_waiting(
        &format!("printf late > {}", in_drive(DRIVE, "late.txt")),
        50_000,
    );
    wait_until("the first upload", || !uploads(&machine).is_empty());
    let first = result_after_the_upload_wait(&machine, &writing);
    assert_eq!(outcome(&first).2["committed"], 0, "{first}");
    assert!(not_synced(&first, "late.txt", "uploading"), "{first}");

    // The clock stands: a result that waited for the upload would not come.
    let next = machine.command("true");
    let sync = outcome(&next).2;
    assert!(
        sync.is_null() || sync["not_synced"] == json!([]) && sync["committed"] == 0,
        "{next}"
    );
    let refused = uploads(&machine);
    assert!(
        refused.iter().all(|upload| upload["status"] == 503),
        "{refused:?}"
    );
    case.observe(format!(
        "the writing command's result came with the 30th second after its exit and listed late.txt uploading; the next command's result came with the clock standing and without it; {} uploads were answered 503",
        refused.len()
    ));

    answer_fault(&machine.engine, json!({"action": "clear"}));
    machine.advance_until(
        "the commit of late.txt",
        Duration::from_secs(5),
        Duration::from_secs(120),
        || drives::commits(&machine.engine, DRIVE).len() == 1,
    );
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("late.txt".to_owned(), sha256(b"late"))]
    );
    case.observe("once uploads passed, late.txt was committed with no tool call");
    case.pass();
}

#[test]
fn a_removed_drives_report_lists_the_change_whose_upload_never_ended() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a drive removed from an idle Session while the upload of its one pending change is held",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(engine, DRIVE, &[write("f", "old")]);
    });
    answer_fault(
        &machine.engine,
        json!({"endpoint": "upload_content", "action": "hold_request", "count": 1000}),
    );
    let writing =
        machine.start_command_waiting(&format!("printf new > {}", in_drive(DRIVE, "f")), 50_000);
    wait_until("the held upload", || drives::held(&machine.engine) > 0);
    let written = result_after_the_upload_wait(&machine, &writing);
    assert!(not_synced(&written, "f", "uploading"), "{written}");

    let seams = &machine.machine().seams;
    let final_uploads = seams.count(FINAL_UPLOAD);
    let removal = machine.change_drives(SESSION, BINDING, &[]);
    seams.wait_for_count(FINAL_UPLOAD, final_uploads + 1);
    let reported = || {
        machine
            .engine
            .received("session-status")
            .into_iter()
            .any(|report| report["body"]["request_message_id"] == removal.as_str())
    };
    pass(&machine, Duration::from_secs(29));
    std::thread::sleep(PAUSE);
    assert!(
        !reported(),
        "a report before the end of the wait for uploads"
    );
    machine.step(Duration::from_secs(1));
    let report = machine.applied(&removal);
    assert_eq!(
        report["lost_changes"],
        json!([{
            "request_message_id": removal, "drive_id": DRIVE,
            "paths": ["f"], "paths_total": 1,
        }]),
        "{report}"
    );
    let state = drives::state(&machine.engine, DRIVE);
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("f".to_owned(), sha256(b"old"))]
    );
    assert_eq!(
        state["versions"].as_array().map(Vec::len),
        Some(1),
        "{state}"
    );
    assert!(uploads(&machine).is_empty(), "{:?}", uploads(&machine));
    case.observe(format!(
        "the report of the removal came with the 30th second after the instruction, with lost_changes {}; the drive kept its one version of f",
        report["lost_changes"]
    ));
    case.pass();
}

/// The App Server's `drive.upload_fault` records with `code` for an answer
/// of `status`.
fn upload_faults(machine: &DriveMachine, code: &str, status: u64) -> usize {
    machine
        .records("drive.upload_fault")
        .iter()
        .filter(|record| {
            record["error.code"] == code && record["http.response.status_code"] == status
        })
        .count()
}

#[test]
fn a_faulted_upload_is_listed_pending_and_tried_again_only_at_the_next_sync_point() {
    let record = tetral_tests::record!();
    let machine = {
        let mut case = record.case("a Session with one read_write drive");
        let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
        case.pass();
        machine
    };
    let refuse_uploads = || {
        answer_fault(
            &machine.engine,
            json!({"endpoint": "upload_content", "action": "error", "code": "invalid_message", "count": 1000}),
        );
    };
    let pass_all = || answer_fault(&machine.engine, json!({"action": "clear"}));
    // The next command, at whose sync point the change is tried again; it
    // gives the result and how many uploads came during it.
    let next_command = || {
        let before = uploads(&machine).len();
        let terminal = machine.command("true");
        (terminal, uploads(&machine).len() - before)
    };
    let committed = |file: &str| {
        let (terminal, _) = next_command();
        assert_eq!(outcome(&terminal).2["committed"], 1, "{file}: {terminal}");
    };
    let write_file = |file: &str, text: &str| {
        machine.command(&format!("printf {text} > {}", in_drive(DRIVE, file)))
    };

    let mut case =
        record.case("a command writing a.txt while uploads are refused 400 invalid_message");
    refuse_uploads();
    let first = write_file("a.txt", "a");
    let sent = uploads(&machine).len();
    assert!(
        sent > 0
            && uploads(&machine)
                .iter()
                .all(|upload| upload["status"] == 400),
        "the server refused {sent} uploads"
    );
    assert!(not_synced(&first, "a.txt", "upload_pending"), "{first}");
    // No upload comes for the passing of time alone.
    pass(&machine, Duration::from_secs(60));
    std::thread::sleep(PAUSE);
    assert_eq!(
        uploads(&machine).len(),
        sent,
        "an upload between sync points"
    );
    let (again, during) = next_command();
    assert!(
        not_synced(&again, "a.txt", "upload_pending") && during > 0,
        "{during} uploads during {again}"
    );
    assert!(upload_faults(&machine, "invalid_message", 400) > 0);
    pass_all();
    committed("a.txt");
    case.observe(format!(
        "a.txt was listed upload_pending after {sent} refused uploads; none came in the next 60 seconds of the clock, {during} during the next command, which listed it again; the App Server logged the refusal's code; with uploads passing the command after committed it"
    ));
    case.pass();

    let mut case = record.case(
        "a command writing b.txt whose commit is answered 200 with a body that fails the schema",
    );
    answer_fault(
        &machine.engine,
        json!({"endpoint": "commit", "action": "schema_fail", "body": {"results": "malformed"}}),
    );
    let commits = || drives::requests(&machine.engine, "commit", DRIVE);
    let before = commits().len();
    let second = write_file("b.txt", "b");
    // The server applied the commit and answered it 200 with the body.
    assert_eq!(commits().len(), before + 1);
    assert_eq!(commits()[before]["status"], 200);
    assert!(not_synced(&second, "b.txt", "upload_pending"), "{second}");
    assert!(upload_faults(&machine, "invalid_response", 200) > 0);
    committed("b.txt");
    case.observe(
        "b.txt was listed upload_pending, the App Server logged a fault invalid_response for the answer, and the next command committed it",
    );
    case.pass();

    let mut case = record.case(
        "a command writing c.txt whose upload is held past the result's wait and refused once released",
    );
    answer_fault(
        &machine.engine,
        json!({"endpoint": "upload_content", "action": "hold_request", "count": 1000}),
    );
    let writing =
        machine.start_command_waiting(&format!("printf c > {}", in_drive(DRIVE, "c.txt")), 50_000);
    wait_until("the held upload", || drives::held(&machine.engine) > 0);
    let third = result_after_the_upload_wait(&machine, &writing);
    assert!(not_synced(&third, "c.txt", "uploading"), "{third}");
    // The clock stands, so no new upload comes between the three controls.
    let faults = upload_faults(&machine, "invalid_message", 400);
    pass_all();
    refuse_uploads();
    answer_fault(&machine.engine, json!({"action": "release"}));
    wait_until("the released upload's refusal", || {
        upload_faults(&machine, "invalid_message", 400) > faults
    });
    let (again, during) = next_command();
    assert!(
        not_synced(&again, "c.txt", "upload_pending") && during > 0,
        "{during} uploads during {again}"
    );
    pass_all();
    committed("c.txt");
    let on_drive: Vec<String> = drives::files(&machine.engine, DRIVE)
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    assert_eq!(on_drive, ["a.txt", "b.txt", "c.txt"]);
    case.observe(
        "c.txt was listed uploading while its upload was held and upload_pending once the released upload was refused; the next command tried it again and the one after committed it",
    );
    case.pass();
}

#[test]
fn a_commit_whose_answer_is_lost_is_sent_again_before_the_commit_of_the_rewrite() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a commit that is applied and whose answer is lost, with every later commit answered 503 while the next command rewrites the file",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    answer_fault(
        &machine.engine,
        json!({"endpoint": "commit", "action": "drop"}),
    );
    answer_fault(
        &machine.engine,
        json!({"endpoint": "commit", "action": "error", "code": "unavailable", "status": 503, "count": 1000}),
    );
    let answers = || -> Vec<Value> {
        drives::requests(&machine.engine, "commit", DRIVE)
            .iter()
            .map(|request| request["status"].clone())
            .collect()
    };
    let file = in_drive(DRIVE, "f");
    let writing = machine.start_command_waiting(&format!("printf one > {file}"), 50_000);
    wait_until("the command's commit", || !answers().is_empty());
    // A commit this small has ten seconds for its answer.
    let first = result_with_second(&machine, &writing, 10);
    assert!(not_synced(&first, "f", "uploading"), "{first}");
    assert_eq!(outcome(&first).2["rejected"], json!([]), "{first}");
    let sent = answers();
    // The clock stands: a result that waited for the first commit's answer
    // would not come.
    let second = machine.command(&format!("printf two > {file}"));
    assert!(not_synced(&second, "f", "uploading"), "{second}");
    assert_eq!(outcome(&second).2["rejected"], json!([]), "{second}");
    assert_eq!(
        answers(),
        sent,
        "a commit of the rewrite before the first one's answer"
    );
    assert!(sent.iter().skip(1).all(|status| status == 503), "{sent:?}");
    case.observe(format!(
        "the first result came with the 10th second after its commit was sent, the rewriting command's with the clock standing and no commit of its own; both listed f uploading and no rejection; the commits so far were answered {sent:?}, the first without its answer reaching the machine"
    ));

    answer_fault(&machine.engine, json!({"action": "clear"}));
    machine.advance_until(
        "two commits answered 200",
        Duration::from_secs(5),
        Duration::from_secs(180),
        || answers().iter().filter(|status| **status == 200).count() >= 3,
    );
    // The server's accepted commits: the one whose answer was lost, the
    // same one again, and the rewrite on the first content's base.
    let accepted = drives::commits(&machine.engine, DRIVE);
    assert_eq!(accepted.len(), 3, "{accepted:?}");
    assert_eq!(accepted[0], accepted[1]);
    assert_eq!(
        accepted[2],
        json!([{
            "path": "f", "change": "write", "base_sha256": sha256(b"one"),
            "content_sha256": sha256(b"two"), "executable": false,
        }])
    );
    let state = drives::state(&machine.engine, DRIVE);
    let versions: Vec<&Value> = state["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|version| &version["content_sha256"])
        .collect();
    assert_eq!(
        versions,
        [&json!(sha256(b"one")), &json!(sha256(b"two"))],
        "{state}"
    );
    // A rejection found after both results would come in a drive sync
    // report, sent when it is found, or in the next result.
    let after = machine.command("true");
    assert_eq!(outcome(&after).2, Value::Null, "{after}");
    assert_eq!(
        machine.engine.requests("drive-sync"),
        Vec::<Value>::new(),
        "a drive sync report"
    );
    case.observe(
        "once commits passed, the lost commit was sent again unchanged and made no version, then the rewrite was committed on the first content's base; f has two versions and no drive sync report was sent",
    );
    case.pass();
}
