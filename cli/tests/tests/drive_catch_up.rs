//! Layer 2. The listings of a drive's mount on the real App Server against
//! the reference control server, through real mounts: what a Session has of
//! a drive whose listings fail, and what a mount holds after commits made
//! while it lists, after a removed version and after an expired cursor.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, READ_ONLY, READ_WRITE, commit, delete, in_drive, outcome, sha256,
    write,
};
use tetral_tests::machine::answer_fault;

const OTHER: &str = "drive_b";

/// The `unmounted` entries of a result's `drive_sync`.
fn unmounted(terminal: &Value) -> Vec<Value> {
    outcome(terminal).2["unmounted"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

#[test]
fn a_drive_whose_listings_fail_stays_unmounted_beside_the_other_until_they_pass() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a Session of two drives, one whose file listing is refused and whose next change listing fails its schema",
    );
    let machine = DriveMachine::start_seeded(
        &mut case,
        &[(DRIVE, READ_WRITE), (OTHER, READ_WRITE)],
        |engine| {
            commit(engine, DRIVE, &[write("f.txt", "failing")]);
            commit(engine, OTHER, &[write("g.txt", "other")]);
            answer_fault(
                engine,
                json!({
                    "endpoint": "list_files", "drive_id": DRIVE, "action": "error",
                    "code": "drive_access_denied", "count": 100,
                }),
            );
        },
    );
    let refused = || {
        drives::requests(&machine.engine, "list_files", DRIVE)
            .iter()
            .filter(|request| request["status"] == 403)
            .count()
    };
    wait_until("the refused file listing", || refused() > 0);

    answer_fault(
        &machine.engine,
        json!({"action": "clear", "endpoint": "list_files"}),
    );
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "list_changes", "drive_id": DRIVE, "action": "schema_fail",
            "body": {"changes": "none"}, "count": 100,
        }),
    );
    let reads = format!(
        "cat {}; echo; cat {} 2>/dev/null || echo unavailable",
        in_drive(OTHER, "g.txt"),
        in_drive(DRIVE, "f.txt")
    );
    let second = machine.command(&reads);
    assert_eq!(outcome(&second).1, "other\nunavailable\n", "{second}");
    let failures = unmounted(&second);
    assert_eq!(
        failures,
        [
            json!({"drive_id": DRIVE, "error": "list files: 403 drive_access_denied"}),
            json!({"drive_id": DRIVE, "error": "list changes: invalid answer"}),
        ],
        "{second}"
    );
    assert!(
        !drives::requests(&machine.engine, "list_changes", DRIVE).is_empty(),
        "the second check sent no change listing"
    );
    case.observe(format!(
        "with the file listing refused and then the change listing failing its schema, the command read the other drive and its result listed {failures:?}"
    ));

    answer_fault(&machine.engine, json!({"action": "clear"}));
    let third = machine.command(&format!("cat {}", in_drive(DRIVE, "f.txt")));
    assert_eq!(outcome(&third).1, "failing", "{third}");
    assert!(unmounted(&third).is_empty(), "{third}");
    case.observe("once the listings passed, the next command read the drive and its result listed no unmounted drive");
    case.pass();
}

/// The paths and contents under `drive`'s mount as a command of the Session
/// reads them, in path order. The command writes them into the project
/// directory, since a result holds only so much output.
fn mounted(machine: &DriveMachine, drive: &str) -> Vec<(String, String)> {
    let listing = machine.project.join("listing");
    let terminal = machine.command_within(
        Duration::from_secs(300),
        &format!(
            "cd \"$TETRAL_DRIVES/{drive}\" && for f in *; do printf '%s=%s\\n' \"$f\" \"$(cat \"$f\")\"; done > '{}'",
            listing.display()
        ),
    );
    assert_eq!(outcome(&terminal).0, 0, "{terminal}");
    let mut files: Vec<(String, String)> = std::fs::read_to_string(&listing)
        .unwrap()
        .lines()
        .map(|line| {
            let (path, text) = line.split_once('=').expect("a listed file");
            (path.to_owned(), text.to_owned())
        })
        .collect();
    files.sort();
    files
}

/// Waits until the server holds one request and has answered `answered`
/// file listings of `drive`.
fn wait_for_held_page(machine: &DriveMachine, drive: &str, answered: usize) {
    wait_until("the next page's request", || {
        drives::held(&machine.engine) == 1
            && drives::requests(&machine.engine, "list_files", drive).len() == answered
    });
}

#[test]
fn a_first_mount_holds_the_commits_another_writer_made_between_its_pages() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a first mount of 2,100 files whose three list pages each wait while another writer writes, rewrites and deletes files",
    );
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        | 1;
    case.observe(format!("the other writer's sequence has the seed {seed}"));
    let mut state: BTreeMap<String, String> = (0..2_100)
        .map(|index| (format!("f{index:04}"), format!("v0 f{index:04}")))
        .collect();
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        let files: Vec<Value> = state.iter().map(|(path, text)| write(path, text)).collect();
        commit(engine, DRIVE, &files);
        answer_fault(
            engine,
            json!({"endpoint": "list_files", "drive_id": DRIVE, "action": "hold_request", "count": 100}),
        );
    });

    // Forty commits while each page waits: a path under 2,400, so some are
    // new, some listed already and some still to list; one in three of the
    // existing ones is deleted.
    let mut random = seed;
    let mut next = |below: u64| {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random % below
    };
    let mut made = 0;
    let mut write_some = |state: &mut BTreeMap<String, String>| {
        for _ in 0..40 {
            made += 1;
            let path = format!("f{:04}", next(2_400));
            if state.contains_key(&path) && next(3) == 0 {
                state.remove(&path);
                commit(&machine.engine, DRIVE, &[delete(&path)]);
            } else {
                let text = format!("v{made} {path}");
                commit(&machine.engine, DRIVE, &[write(&path, &text)]);
                state.insert(path, text);
            }
        }
    };
    let mut pages = 0;
    loop {
        wait_for_held_page(&machine, DRIVE, pages);
        write_some(&mut state);
        answer_fault(&machine.engine, json!({"action": "release"}));
        pages += 1;
        wait_until("the page's answer", || {
            drives::requests(&machine.engine, "list_files", DRIVE).len() == pages
        });
        let last = &drives::requests(&machine.engine, "list_files", DRIVE)[pages - 1];
        assert_eq!(last["status"], 200, "{last}");
        if pages == 3 {
            break;
        }
    }
    answer_fault(&machine.engine, json!({"action": "clear"}));
    write_some(&mut state);

    let listed = mounted(&machine, DRIVE);
    let expected: Vec<(String, String)> = state.into_iter().collect();
    assert_eq!(listed.len(), expected.len());
    assert_eq!(listed, expected);
    assert_eq!(
        drives::requests(&machine.engine, "list_files", DRIVE).len(),
        3,
        "the mount listed the drive once, in three pages"
    );
    case.observe(format!(
        "the mount listed three pages with 40 commits before each and 40 after; a command then read {} files, the paths and contents of the drive",
        listed.len()
    ));
    case.pass();
}

#[test]
fn a_catch_up_passes_a_removed_version_and_an_expired_cursor_lists_the_drive_again() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a mounted drive whose change listings wait while a file is written twelve times and its first version removed",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(engine, DRIVE, &[write("cached.txt", "cached")]);
    });
    assert_eq!(
        machine.output(&format!("cat {}", in_drive(DRIVE, "cached.txt"))),
        "cached"
    );
    let hold_changes = || {
        answer_fault(
            &machine.engine,
            json!({"endpoint": "list_changes", "drive_id": DRIVE, "action": "hold_request", "count": 100}),
        );
    };
    let release = || {
        answer_fault(&machine.engine, json!({"action": "clear"}));
        answer_fault(&machine.engine, json!({"action": "release"}));
    };
    let listings = || drives::requests(&machine.engine, "list_changes", DRIVE);

    hold_changes();
    let first = machine.engine.control(
        "drive_commit",
        json!({"drive_id": DRIVE, "changes": [write("f", "v0")]}),
    );
    for version in 1..=11 {
        commit(
            &machine.engine,
            DRIVE,
            &[write("f", &format!("v{version}"))],
        );
    }
    let oldest = first["changes"][0]["version_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no version in {first}"))
        .to_owned();
    machine.engine.control(
        "drive_remove_version",
        json!({"drive_id": DRIVE, "version_id": oldest}),
    );
    let before = listings().len();
    release();
    assert_eq!(
        machine.output(&format!("cat {}", in_drive(DRIVE, "f"))),
        "v11"
    );
    let caught = listings()[before..].to_vec();
    assert!(
        !caught.is_empty() && caught.iter().all(|listing| listing["status"] == 200),
        "{caught:?}"
    );
    case.observe(format!(
        "released, the catch-up's {} change listings were answered 200 over the record of the removed version {oldest}, and the next command read v11",
        caught.len()
    ));
    case.pass();

    let mut case = record.case(
        "the same mount with a change not yet uploaded, when every change record its cursor needs has expired",
    );
    std::fs::write(machine.mount(DRIVE).join("pending.txt"), "pending").unwrap();
    hold_changes();
    let before = listings().len();
    let lists = drives::requests(&machine.engine, "list_files", DRIVE).len();
    let newest = commit(&machine.engine, DRIVE, &[write("g", "g")]);
    machine.engine.control(
        "drive_expire",
        json!({"drive_id": DRIVE, "before_seq": newest + 1}),
    );
    release();
    wait_until("the drive listed again", || {
        drives::requests(&machine.engine, "list_files", DRIVE).len() > lists
    });
    let gone = listings()[before..].to_vec();
    assert_eq!(gone[0]["status"], 410, "{gone:?}");

    let listed = mounted(&machine, DRIVE);
    assert_eq!(
        listed,
        [
            ("cached.txt".to_owned(), "cached".to_owned()),
            ("f".to_owned(), "v11".to_owned()),
            ("g".to_owned(), "g".to_owned()),
            ("pending.txt".to_owned(), "pending".to_owned()),
        ]
    );
    let on_drive: Vec<String> = drives::files(&machine.engine, DRIVE)
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    assert_eq!(on_drive, ["cached.txt", "f", "g", "pending.txt"]);
    let cached = drives::requests(&machine.engine, "download_content", DRIVE)
        .iter()
        .filter(|request| request["content_sha256"] == sha256(b"cached"))
        .count();
    assert_eq!(cached, 1, "cached.txt was downloaded again");
    // A Mac's NFS client may have answered the read from its own memory.
    assert!(
        machine
            .root
            .join("cache/blobs")
            .join(sha256(b"cached"))
            .exists(),
        "the cache lost cached.txt's content"
    );
    case.observe(
        "the change listing was answered 410 and the machine listed the drive again; a command then read the drive's four files, pending.txt committed among them, and cached.txt came from the one download before the expiry",
    );
    case.pass();
}
