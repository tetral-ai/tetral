//! Drive sync of the real App Server against the reference control server,
//! through real mounts: what a result says of the changes it covers when
//! another writer, a refused upload or a slow transfer is in the way, and
//! what the machine holds afterwards.

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, READ_WRITE, commit, in_drive, outcome, sha256, write,
};
use tetral_tests::machine::answer_fault;

const OTHER: &str = "drive_b";

/// The one rejection of a result's `drive_sync`.
fn rejection(terminal: &Value) -> Value {
    let rejected = outcome(terminal).2["rejected"]
        .as_array()
        .unwrap_or_else(|| panic!("no rejections in {terminal}"))
        .clone();
    assert_eq!(rejected.len(), 1, "{terminal}");
    rejected[0].clone()
}

#[test]
fn a_delete_of_a_file_another_writer_changed_meanwhile_is_rejected_and_the_file_is_back() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that deletes f.txt after another writer changed it, while the machine's change listings are held",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(engine, DRIVE, &[write("f.txt", "v1")]);
    });
    let file = in_drive(DRIVE, "f.txt");
    assert_eq!(machine.output(&format!("cat {file}")), "v1");
    let (started, release) = (
        machine.project.join("started"),
        machine.project.join("release"),
    );
    let deleting = machine.start_command(&format!(
        "touch '{}'; while [ ! -e '{}' ]; do sleep 0.1; done; rm {file}",
        started.display(),
        release.display()
    ));
    wait_until("the deleting command started", || started.exists());
    // The machine learns of the other writer's change only through its
    // change listings, held until its deletion's commit is answered.
    answer_fault(
        &machine.engine,
        json!({"endpoint": "list_changes", "action": "hold_request", "count": 8}),
    );
    commit(&machine.engine, DRIVE, &[write("f.txt", "v2")]);
    std::fs::write(&release, "").unwrap();
    wait_until("the deletion's commit", || {
        !drives::requests(&machine.engine, "commit", DRIVE).is_empty()
    });
    answer_fault(&machine.engine, json!({"action": "release"}));
    let deleted = machine.terminal(&deleting);
    let rejected = rejection(&deleted);
    assert!(
        rejected["path"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/drives/{DRIVE}/f.txt"))
    );
    assert_eq!(
        (&rejected["change"], &rejected["reason"]),
        (&json!("delete"), &json!("conflict"))
    );
    assert_eq!(
        drives::commits(&machine.engine, DRIVE),
        [json!([{"change": "delete", "path": "f.txt", "base_sha256": sha256(b"v1")}])]
    );
    assert_eq!(machine.output(&format!("cat {file}")), "v2");
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("f.txt".to_owned(), sha256(b"v2"))]
    );
    case.observe("the deletion based on v1 was rejected as a conflict; the next command read v2, which the drive still holds");
    case.pass();
}

#[test]
fn a_write_another_writer_created_first_is_rejected_and_the_machine_reads_the_winner() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that writes a new file whose path another writer creates while the machine's commit is held; then a write nobody else makes",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE)]);
    let file = in_drive(DRIVE, "race.txt");
    answer_fault(
        &machine.engine,
        json!({"endpoint": "commit", "action": "hold_request", "count": 1}),
    );
    let writing = machine.start_command(&format!("printf agent > {file}"));
    wait_until("the machine's commit held", || {
        drives::held(&machine.engine) == 1
    });
    commit(&machine.engine, DRIVE, &[write("race.txt", "developer")]);
    answer_fault(&machine.engine, json!({"action": "release"}));
    let written = machine.terminal(&writing);
    let rejected = rejection(&written);
    assert!(
        rejected["path"].as_str().unwrap().ends_with("/race.txt"),
        "{written}"
    );
    assert_eq!(rejected["reason"], "conflict", "{written}");
    assert_eq!(outcome(&written).2["committed"], 0, "{written}");
    assert_eq!(machine.output(&format!("cat {file}")), "developer");
    case.observe(
        "the write was rejected as a conflict and the machine then read the other writer's content",
    );

    let own = in_drive(DRIVE, "own.txt");
    let written = machine.command(&format!("printf agent > {own}"));
    assert_eq!(outcome(&written).2["committed"], 1, "{written}");
    assert_eq!(machine.output(&format!("cat {own}")), "agent");
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [
            ("own.txt".to_owned(), sha256(b"agent")),
            ("race.txt".to_owned(), sha256(b"developer")),
        ]
    );
    case.observe("a write no other writer made was committed and stays on the machine");
    case.pass();
}

#[test]
fn a_mismatched_upload_is_sent_again_and_content_is_uploaded_to_each_drive_it_is_written_into() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a write whose first upload is answered 400 content_hash_mismatch, then the same content written into two drives",
    );
    let machine = DriveMachine::start(&mut case, &[(DRIVE, READ_WRITE), (OTHER, READ_WRITE)]);
    let uploads = |drive: &str, content: &[u8]| -> Vec<u64> {
        drives::requests(&machine.engine, "upload_content", drive)
            .iter()
            .filter(|upload| upload["content_sha256"] == sha256(content))
            .map(|upload| upload["status"].as_u64().unwrap())
            .collect()
    };
    let write_committed = |drive: &str, path: &str, content: &str| {
        let written = machine.command(&format!("printf '{content}' > {}", in_drive(drive, path)));
        assert_eq!(outcome(&written).2["committed"], 1, "{written}");
    };

    const CONTENT: &str = "content of f";
    answer_fault(
        &machine.engine,
        json!({"endpoint": "upload_content", "action": "error", "code": "content_hash_mismatch"}),
    );
    write_committed(DRIVE, "f.txt", CONTENT);
    assert_eq!(uploads(DRIVE, CONTENT.as_bytes()), [400, 200]);
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("f.txt".to_owned(), sha256(CONTENT.as_bytes()))]
    );
    assert_eq!(drives::requests(&machine.engine, "commit", DRIVE).len(), 1);
    case.observe("the upload answered 400 content_hash_mismatch was sent again and accepted; f.txt was committed once");

    const SHARED: &str = "content of both drives";
    write_committed(OTHER, "s.txt", SHARED);
    write_committed(DRIVE, "s.txt", SHARED);
    assert_eq!(uploads(OTHER, SHARED.as_bytes()), [200]);
    assert_eq!(uploads(DRIVE, SHARED.as_bytes()), [200]);
    assert!(
        drives::files(&machine.engine, DRIVE)
            .contains(&("s.txt".to_owned(), sha256(SHARED.as_bytes())))
    );
    case.observe("content the other drive held was uploaded again to the drive it was written into and committed there");
    case.pass();
}
