//! Session preparation of `tetral server run` against the reference control
//! server, as separate processes over real HTTP and SSE with real
//! directories and symbolic links, each on its own controlled clock: the
//! failure of each check and what a preparation creates or never creates, a
//! repeated or changed preparation, the bindings of two App Servers and of
//! ended connections, the record and probe limits, and a held probe under
//! heartbeats and shutdown.
//!
//! Besides the counters that `tetral_tests::machine` names, the App Server
//! reports the test seam counter `probe.spawn` for each directory probe it
//! starts, and each probe waits at the barrier `probe.started` after its
//! input is written and before its result is read and it is reaped.

use std::ffi::OsStr;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tetral_tests::app_server::{request, wait_until};
use tetral_tests::machine::{
    CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, answer_fault, bootstrap, credential,
    engine, one_credential_engine, seconds, send, wait_for_received, wait_for_requests,
};
use tetral_tests::reference::ReferenceServer;
use tetral_tests::sessions::{
    entries, failed, new_connection, online_machine, outcome, prepare, prepare_on, prepared,
    quiesce, records, reports_for, sessions, state_of,
};
use tetral_tests::socket_directory;

const PROBE_STARTED: &str = "probe.started";
const PROBE_SPAWN: &str = "probe.spawn";

/// A `session.prepare` payload with no drives and no repositories.
fn payload(session: &str, binding: &str, cwd: &Path) -> Value {
    json!({
        "session_id": session,
        "binding_id": binding,
        "cwd": cwd,
        "drives": [],
        "drive_exclude": [],
        "repositories": [],
    })
}

fn close(engine: &ReferenceServer, session: &str, binding: &str) {
    engine.control(
        "close",
        json!({
            "instance_id": INSTANCE,
            "payload": { "session_id": session, "binding_id": binding },
        }),
    );
}

/// The names in `directory`, sorted.
fn listing(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

fn directory_with_marker(path: PathBuf) -> PathBuf {
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("marker"), "kept").unwrap();
    path
}

#[test]
fn each_failed_check_reports_its_code_and_creates_nothing_and_a_valid_link_prepares_its_target() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case =
        record.case("omitted, empty and relative cwd with the App Server inside a project");
    let machine = Machine::start_in(
        &mut case,
        &base.join("machine"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        &project,
    );
    machine.wait_for("online");
    let root_listing = listing(&machine.root);
    let mut omitted = payload("sess_omitted", "bind_omitted", Path::new("/"));
    omitted.as_object_mut().unwrap().remove("cwd");
    let rows = [
        omitted,
        payload("sess_empty", "bind_empty", Path::new("")),
        payload("sess_relative", "bind_relative", Path::new("project")),
    ];
    for row in &rows {
        let message_id = prepare(&engine, row);
        assert_eq!(
            outcome(&engine, &message_id),
            failed("invalid_input"),
            "{row}"
        );
    }
    assert_eq!(listing(&project), ["marker"]);
    assert_eq!(listing(&machine.root), root_listing);
    assert_eq!(sessions(&machine), Vec::<Value>::new());
    case.observe("each failed with invalid_input; the App Server's working directory and the state root are unchanged and no Session is listed");
    case.pass();

    let mut case = record.case("a session_id .. and a drive_id holding /");
    let mut drive = payload("sess_drive", "bind_drive", &project);
    drive["drives"] = json!([{ "drive_id": "a/b", "access": "read_write" }]);
    for row in [payload("..", "bind_dots", &project), drive] {
        let message_id = prepare(&engine, &row);
        assert_eq!(
            outcome(&engine, &message_id),
            failed("invalid_input"),
            "{row}"
        );
        assert_eq!(
            reports_for(&engine, &message_id)[0]["session_id"],
            row["session_id"]
        );
    }
    assert_eq!(sessions(&machine), Vec::<Value>::new());
    case.observe(
        "both failed with invalid_input naming the Session as given; no Session is listed",
    );
    case.pass();

    let mut case = record
        .case("a missing path, a regular file, the state root, links into it and to a project");
    fs::write(base.join("file"), "not a directory").unwrap();
    let into_root = base.join("into_root");
    symlink(&machine.root, &into_root).unwrap();
    let project_link = base.join("project_link");
    symlink(&project, &project_link).unwrap();
    let spawned = machine.seams.count(PROBE_SPAWN);
    let at_root = prepare(&engine, &payload("sess_root", "bind_root", &machine.root));
    assert_eq!(outcome(&engine, &at_root), failed("cwd_forbidden"));
    assert_eq!(machine.seams.count(PROBE_SPAWN), spawned);
    let rows = [
        (
            "sess_missing",
            base.join("missing"),
            failed("cwd_not_found"),
        ),
        ("sess_file", base.join("file"), failed("cwd_not_directory")),
        ("sess_into_root", into_root.clone(), failed("cwd_forbidden")),
        ("sess_link", project_link, prepared(&project)),
    ];
    for (session, cwd, expected) in rows {
        let message_id = prepare(&engine, &payload(session, "bind_1", &cwd));
        assert_eq!(outcome(&engine, &message_id), expected, "{session}");
    }
    assert_eq!(
        entries(&machine, "sess_link")[0]["cwd"],
        prepared(&project).1
    );
    assert_eq!(state_of(&machine, "sess_file"), "failed");
    assert!(!base.join("missing").exists());
    case.observe("cwd_not_found, cwd_not_directory, cwd_forbidden for the root with no probe spawned and for the link into it, and prepared with the link's canonical target");
    case.pass();

    let mut case = record.case("a directory the App Server's user cannot enter");
    if case.runs_as_non_root("root enters a directory of mode 0000") {
        let blocked = base.join("blocked");
        fs::create_dir(&blocked).unwrap();
        fs::set_permissions(&blocked, fs::Permissions::from_mode(0o000)).unwrap();
        let message_id = prepare(&engine, &payload("sess_blocked", "bind_1", &blocked));
        assert_eq!(outcome(&engine, &message_id), failed("cwd_inaccessible"));
        fs::set_permissions(&blocked, fs::Permissions::from_mode(0o755)).unwrap();
        case.observe("cwd_inaccessible");
        case.pass();
    }

    let mut case = record.case("a link to a directory whose name is not UTF-8");
    if cfg!(target_os = "linux") {
        // Their own directory keeps the name out of the later listings of `base`.
        let holder = base.join("not_utf8");
        let latin1 = holder.join(OsStr::from_bytes(b"caf\xe9"));
        fs::create_dir_all(&latin1).unwrap();
        let link = holder.join("link");
        symlink(&latin1, &link).unwrap();
        let message_id = prepare(&engine, &payload("sess_latin1", "bind_1", &link));
        assert_eq!(outcome(&engine, &message_id), failed("cwd_inaccessible"));
        let error = json!({
            "code": "cwd_inaccessible",
            "message": "the working directory's canonical path is not UTF-8",
        });
        assert_eq!(reports_for(&engine, &message_id)[0]["error"], error);
        assert_eq!(entries(&machine, "sess_latin1")[0]["error"], error);
        case.observe("cwd_inaccessible with exactly the message the working directory's canonical path is not UTF-8, in the report and in GET /v1/sessions");
        case.pass();
    } else {
        case.not_run("macOS file systems hold only UTF-8 names");
    }

    let mut case = record
        .case("create_cwd with a missing directory, without it, and below a link into the root");
    let created = base.join("created/a/b");
    let mut create = payload("sess_create", "bind_1", &created);
    create["create_cwd"] = json!(true);
    let message_id = prepare(&engine, &create);
    assert_eq!(outcome(&engine, &message_id), prepared(&created));
    assert!(created.is_dir());
    let absent = base.join("absent/a");
    let message_id = prepare(&engine, &payload("sess_absent", "bind_1", &absent));
    assert_eq!(outcome(&engine, &message_id), failed("cwd_not_found"));
    assert!(!base.join("absent").exists());
    let root_listing = listing(&machine.root);
    let mut below_root = payload("sess_below_root", "bind_1", &into_root.join("new/dir"));
    below_root["create_cwd"] = json!(true);
    let message_id = prepare(&engine, &below_root);
    assert_eq!(outcome(&engine, &message_id), failed("cwd_forbidden"));
    assert_eq!(listing(&machine.root), root_listing);
    case.observe("the missing directory was created and prepared; without create_cwd cwd_not_found and nothing created; below the link into the state root cwd_forbidden and nothing created there");
    case.pass();

    let mut case = record.case("create_cwd with a .. after a missing directory and after a link");
    let into_run = base.join("into_run");
    symlink(machine.root.join("run"), &into_run).unwrap();
    let base_listing = listing(base);
    let mut after_missing = payload("sess_after_missing", "bind_1", &base.join("gone/../made"));
    after_missing["create_cwd"] = json!(true);
    let message_id = prepare(&engine, &after_missing);
    assert_eq!(outcome(&engine, &message_id), failed("cwd_not_found"));
    let mut after_link = payload("sess_after_link", "bind_1", &into_run.join("../new"));
    after_link["create_cwd"] = json!(true);
    let message_id = prepare(&engine, &after_link);
    assert_eq!(outcome(&engine, &message_id), failed("cwd_forbidden"));
    assert_eq!(listing(base), base_listing);
    assert_eq!(listing(&machine.root), root_listing);
    case.observe("after a missing directory cwd_not_found; after a link into the state root, which the .. leaves for the state root itself, cwd_forbidden; nothing created in either place");
    case.pass();
}

#[test]
fn a_repeated_preparation_starts_one_probe_and_two_app_servers_keep_their_own_bindings() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let projects = [
        directory_with_marker(base.join("project_a")),
        directory_with_marker(base.join("project_b")),
    ];
    let engine = engine(
        &[
            credential(INSTANCE, CREDENTIAL, KEY, "active"),
            credential(
                "inst_two",
                "ccred_two",
                "tetral_ck_connection_two",
                "active",
            ),
        ],
        json!({}),
    );

    let mut case =
        record.case("two App Servers prepare the same two Sessions and one binding closes");
    let one = online_machine(&mut case, &engine, &base.join("one"));
    let two = Machine::start(
        &mut case,
        &base.join("two"),
        &bootstrap(
            &engine.url,
            "inst_two",
            "ccred_two",
            "tetral_ck_connection_two",
        ),
    );
    two.wait_for("online");
    for (instance, machine_name) in [(INSTANCE, "one"), ("inst_two", "two")] {
        for (session, project) in ["sess_a", "sess_b"].into_iter().zip(&projects) {
            let binding = format!("bind_{session}_{machine_name}");
            let message_id = prepare_on(&engine, instance, &payload(session, &binding, project));
            assert_eq!(outcome(&engine, &message_id), prepared(project));
        }
    }
    for (machine, session) in [
        (&one, "sess_a"),
        (&one, "sess_b"),
        (&two, "sess_a"),
        (&two, "sess_b"),
    ] {
        wait_until(&format!("{session} prepared at its ACK"), || {
            state_of(machine, session) == "prepared"
        });
    }
    close(&engine, "sess_a", "bind_sess_a_one");
    quiesce(&one, 3);
    assert_eq!(state_of(&one, "sess_a"), "closing");
    assert_eq!(state_of(&one, "sess_b"), "prepared");
    assert_eq!(state_of(&two, "sess_a"), "prepared");
    assert_eq!(state_of(&two, "sess_b"), "prepared");
    case.observe("the closed binding is closing; the other three stay prepared");
    drop(two);
    case.pass();

    let mut case = record.case(
        "a probe held at its barrier while the preparation is repeated, with its ACKs withheld",
    );
    let target_one = directory_with_marker(base.join("target_one"));
    let link = base.join("link");
    symlink(&target_one, &link).unwrap();
    let spawned = one.seams.count(PROBE_SPAWN);
    one.seams.hold(PROBE_STARTED);
    let held = payload("sess_c", "bind_c", &link);
    let first = prepare(&engine, &held);
    one.seams.wait_until_waiting(PROBE_STARTED, 1);
    let repeated = prepare(&engine, &held);
    quiesce(&one, 5);
    answer_fault(
        &engine,
        json!({ "endpoint": "session-status", "count": 2, "action": "withhold" }),
    );
    one.seams.release(PROBE_STARTED);
    wait_until("two withheld session-status reports", || {
        engine.state()["held"].as_array().map_or(0, Vec::len) == 2
    });
    assert_eq!(state_of(&one, "sess_c"), "prepared_pending_ack");
    engine.control("answer", json!({ "action": "release" }));
    assert_eq!(outcome(&engine, &first), prepared(&target_one));
    assert_eq!(outcome(&engine, &repeated), prepared(&target_one));
    wait_until("sess_c prepared", || state_of(&one, "sess_c") == "prepared");
    assert_eq!(one.seams.count(PROBE_SPAWN), spawned + 1);
    case.observe("one probe.spawn; both instructions got prepared reports; prepared_pending_ack while their ACKs were withheld, prepared after");
    case.pass();

    let mut case = record.case("a retargeted link and a repeat, then a changed payload");
    let target_two = directory_with_marker(base.join("target_two"));
    fs::remove_file(&link).unwrap();
    symlink(&target_two, &link).unwrap();
    let again = prepare(&engine, &held);
    assert_eq!(outcome(&engine, &again), prepared(&target_one));
    let mut changed = held.clone();
    changed["create_cwd"] = json!(true);
    let conflict = prepare(&engine, &changed);
    assert_eq!(outcome(&engine, &conflict), failed("binding_conflict"));
    assert_eq!(one.seams.count(PROBE_SPAWN), spawned + 1);
    assert_eq!(state_of(&one, "sess_c"), "prepared");
    assert_eq!(entries(&one, "sess_c")[0]["cwd"], prepared(&target_one).1);
    case.observe("the repeat reported the saved cwd of the first target with no new probe; the changed payload got binding_conflict and the binding stayed prepared");
    case.pass();
}

#[test]
fn a_removed_directory_leaves_its_binding_prepared_and_a_new_connection_checks_it_again() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let removed = directory_with_marker(base.join("removed"));
    let kept = directory_with_marker(base.join("kept"));
    let engine = one_credential_engine();

    let mut case = record.case("two prepared Sessions, one directory removed");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    for (session, project) in [("sess_removed", &removed), ("sess_kept", &kept)] {
        let message_id = prepare(&engine, &payload(session, "bind_1", project));
        assert_eq!(outcome(&engine, &message_id), prepared(project));
        wait_until(&format!("{session} prepared at its ACK"), || {
            state_of(&machine, session) == "prepared"
        });
    }
    fs::remove_dir_all(&removed).unwrap();
    engine.advance(seconds(15.0));
    wait_for_received(&engine, "heartbeat", 1);
    machine.advance(seconds(15.0));
    assert_eq!(machine.state(), "online");
    assert_eq!(state_of(&machine, "sess_removed"), "prepared");
    assert_eq!(state_of(&machine, "sess_kept"), "prepared");
    assert!(!removed.exists());
    case.observe("both stayed prepared, the heartbeat was answered, nothing was recreated");
    case.pass();

    let mut case = record.case("a new connection prepares both Sessions again");
    new_connection(&engine, &machine);
    assert_eq!(state_of(&machine, "sess_removed"), "unavailable");
    let failing = prepare(&engine, &payload("sess_removed", "bind_2", &removed));
    assert_eq!(outcome(&engine, &failing), failed("cwd_not_found"));
    let message_id = prepare(&engine, &payload("sess_kept", "bind_2", &kept));
    assert_eq!(outcome(&engine, &message_id), prepared(&kept));
    case.observe(
        "the removed directory's Session failed with cwd_not_found and the other prepared",
    );
    case.pass();

    let mut case = record.case("the directory restored and a new binding without a close");
    let restored = directory_with_marker(removed.clone());
    let message_id = prepare(&engine, &payload("sess_removed", "bind_3", &restored));
    assert_eq!(outcome(&engine, &message_id), prepared(&restored));
    wait_until("bind_3 prepared at its ACK", || {
        state_of(&machine, "sess_removed") == "prepared"
    });
    let listed = entries(&machine, "sess_removed");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        (&listed[0]["binding_id"], &listed[0]["state"]),
        (&json!("bind_3"), &json!("prepared"))
    );
    assert_eq!(listing(&restored), ["marker"]);
    case.observe(
        "the new binding replaced the failed one and prepared; the directory kept its content",
    );
    case.pass();
}

#[test]
fn a_probe_result_after_a_new_connection_changes_no_binding_and_a_resumed_stream_starts_no_probe() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("eight probes held across a new connection, then one reaped");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    machine.seams.hold(PROBE_STARTED);
    let old: Vec<String> = (1..=8)
        .map(|index| {
            prepare(
                &engine,
                &payload(&format!("sess_{index}"), "bind_1", &project),
            )
        })
        .collect();
    machine.seams.wait_until_waiting(PROBE_STARTED, 8);
    new_connection(&engine, &machine);
    machine.seams.release_one(PROBE_STARTED);
    // A slot comes back only once a probe of the old connection is reaped,
    // after its owner took the result.
    let late = prepare(&engine, &payload("sess_9", "bind_1", &project));
    machine.seams.wait_for_count(PROBE_SPAWN, 9);
    for index in 1..=8 {
        let listed = entries(&machine, &format!("sess_{index}"));
        assert_eq!(
            (&listed[0]["state"], &listed[0]["cwd"]),
            (&json!("unavailable"), &Value::Null),
            "sess_{index}"
        );
    }
    machine.seams.release(PROBE_STARTED);
    assert_eq!(outcome(&engine, &late), prepared(&project));
    for message_id in &old {
        assert!(reports_for(&engine, message_id).is_empty());
    }
    case.observe("the reaped probe's result changed no binding: all eight are unavailable without a cwd and none was reported; the new connection's preparation prepared");
    case.pass();

    let mut case = record.case("a resumed stream and a preparation written again");
    let spawned = machine.seams.count(PROBE_SPAWN);
    let events = engine.requests("events").len();
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    // The reconnect backoff's second wait, 2 seconds and at most 20% of
    // jitter, since no heartbeat has confirmed the new connection.
    machine.seams.advance(seconds(2.4));
    wait_until("the resume", || engine.requests("events").len() > events);
    machine.wait_for("online");
    assert_eq!(
        engine.requests("events").last().unwrap()["query"]["resume"],
        "true"
    );
    send_resend(&engine, &late);
    let after = prepare(&engine, &payload("sess_10", "bind_1", &project));
    assert_eq!(outcome(&engine, &after), prepared(&project));
    assert_eq!(machine.seams.count(PROBE_SPAWN), spawned + 1);
    assert_eq!(state_of(&machine, "sess_9"), "prepared");
    case.observe("the resumed connection kept the binding prepared; the preparation written again started no probe, only the later one did");
    case.pass();
}

fn send_resend(engine: &ReferenceServer, message_id: &str) {
    engine.control(
        "send",
        json!({ "instance_id": INSTANCE, "resend": message_id }),
    );
}

#[test]
fn a_waiting_preparation_spawns_only_once_a_probe_is_reaped_and_never_once_closed() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("eight probes held and a ninth preparation");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    machine.seams.hold(PROBE_STARTED);
    let held: Vec<String> = (1..=8)
        .map(|index| {
            prepare(
                &engine,
                &payload(&format!("sess_{index}"), "bind_1", &project),
            )
        })
        .collect();
    machine.seams.wait_until_waiting(PROBE_STARTED, 8);
    let ninth = prepare(&engine, &payload("sess_9", "bind_1", &project));
    quiesce(&machine, 9);
    assert_eq!(machine.seams.count(PROBE_SPAWN), 8);
    assert_eq!(state_of(&machine, "sess_9"), "preparing");
    machine.seams.release_one(PROBE_STARTED);
    machine.seams.wait_for_count(PROBE_SPAWN, 9);
    machine.seams.wait_until_waiting(PROBE_STARTED, 8);
    // A pass of the barrier goes to any of the waiting probes, the ninth's
    // included once it has spawned.
    let reaped = |count: usize| {
        wait_until(&format!("{count} reaped probes reported"), || {
            held.iter()
                .chain([&ninth])
                .filter(|message_id| !reports_for(&engine, message_id).is_empty())
                .count()
                == count
        });
    };
    reaped(1);
    case.observe(
        "the ninth spawned nothing while eight ran and spawned once one of them was reaped",
    );
    case.pass();

    let mut case = record.case("a tenth preparation closed while it waits");
    prepare(&engine, &payload("sess_10", "bind_1", &project));
    quiesce(&machine, 10);
    assert_eq!(state_of(&machine, "sess_10"), "preparing");
    close(&engine, "sess_10", "bind_1");
    quiesce(&machine, 11);
    assert_eq!(state_of(&machine, "sess_10"), "closing");
    machine.seams.release_one(PROBE_STARTED);
    reaped(2);
    quiesce(&machine, 11);
    assert_eq!(machine.seams.count(PROBE_SPAWN), 9);
    case.observe("the closed preparation never spawned, also once a slot came back");
    machine.seams.release(PROBE_STARTED);
    case.pass();
}

#[test]
fn a_freed_probe_slot_goes_to_the_longest_waiting_preparation() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("eight probes held, then a ninth and a tenth preparation waiting");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    machine.seams.hold(PROBE_STARTED);
    for index in 1..=8 {
        prepare(
            &engine,
            &payload(&format!("sess_{index}"), "bind_1", &project),
        );
    }
    machine.seams.wait_until_waiting(PROBE_STARTED, 8);
    let ninth = prepare(&engine, &payload("sess_9", "bind_1", &project));
    quiesce(&machine, 9);
    let tenth = prepare(&engine, &payload("sess_10", "bind_1", &project));
    quiesce(&machine, 10);
    assert_eq!(machine.seams.count(PROBE_SPAWN), 8);
    machine.seams.release_one(PROBE_STARTED);
    machine.seams.wait_for_count(PROBE_SPAWN, 9);
    machine.seams.wait_until_waiting(PROBE_STARTED, 8);
    // The probe that took the slot runs out its limit, and the preparation
    // still waiting runs out its wait, at the same time.
    machine.seams.advance(seconds(5.0));
    assert_eq!(outcome(&engine, &ninth), failed("cwd_check_timeout"));
    assert_eq!(outcome(&engine, &tenth), failed("resource_exhausted"));
    assert_eq!(machine.seams.count(PROBE_SPAWN), 9);
    case.observe("once one of the eight was reaped the ninth spawned and the tenth still waited: at 5 seconds the ninth's probe timed out with cwd_check_timeout and the tenth's wait ended with resource_exhausted");
    machine.seams.release(PROBE_STARTED);
    case.pass();
}

#[test]
fn eight_probes_held_for_five_seconds_time_out_and_keep_their_slots_until_reaped() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("eight probes held for five seconds and a ninth waiting");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    machine.seams.hold(PROBE_STARTED);
    let held: Vec<String> = (1..=8)
        .map(|index| {
            prepare(
                &engine,
                &payload(&format!("sess_{index}"), "bind_1", &project),
            )
        })
        .collect();
    machine.seams.wait_until_waiting(PROBE_STARTED, 8);
    let ninth = prepare(&engine, &payload("sess_9", "bind_1", &project));
    quiesce(&machine, 9);
    // Each Session binding owner acts on its passed probe deadline before
    // it answers the listing, and the Connection owner has settled on the
    // clock's time, so the listing shows both the probes and the waiting
    // preparation as of 4.999 seconds.
    machine.advance(seconds(4.999));
    for index in 1..=9 {
        assert_eq!(
            state_of(&machine, &format!("sess_{index}")),
            "preparing",
            "sess_{index}"
        );
    }
    machine.seams.advance(seconds(0.001));
    assert_eq!(outcome(&engine, &ninth), failed("resource_exhausted"));
    for message_id in &held {
        assert_eq!(outcome(&engine, message_id), failed("cwd_check_timeout"));
    }
    assert_eq!(machine.seams.count(PROBE_SPAWN), 8);
    wait_until("a pressure.rejecting_started record", || {
        !records(&machine.root, "pressure.rejecting_started").is_empty()
    });
    let started = records(&machine.root, "pressure.rejecting_started");
    assert_eq!(started.len(), 1);
    assert_eq!(
        (&started[0]["pressure.point"], &started[0]["pressure.cause"]),
        (&json!("tetral_probes"), &json!("timeout"))
    );
    case.observe("all nine were preparing at 4.999 seconds; at 5 the ninth failed with resource_exhausted, the eight with cwd_check_timeout, probe.spawn counts 8, and one pressure.rejecting_started names tetral_probes and timeout");
    case.pass();

    let mut case =
        record.case("a preparation after the timeouts while the eight are still unreaped");
    let tenth = prepare(&engine, &payload("sess_10", "bind_1", &project));
    quiesce(&machine, 10);
    assert_eq!(machine.seams.count(PROBE_SPAWN), 8);
    assert_eq!(state_of(&machine, "sess_10"), "preparing");
    machine.seams.release(PROBE_STARTED);
    assert_eq!(outcome(&engine, &tenth), prepared(&project));
    assert_eq!(machine.seams.count(PROBE_SPAWN), 9);
    case.observe(
        "the timed-out probes kept their slots until reaped; then the tenth spawned and prepared",
    );
    case.pass();
}

#[test]
fn a_new_session_beyond_one_hundred_twenty_eight_records_is_refused_and_existing_ones_still_work() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("128 Sessions, then a 129th");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    let first: Vec<(String, Value)> = (0..128)
        .map(|index| {
            let row = payload(&format!("sess_{index}"), "bind_1", &project);
            (prepare(&engine, &row), row)
        })
        .collect();
    for (message_id, _) in &first {
        assert_eq!(outcome(&engine, message_id), prepared(&project));
    }
    let spawned = machine.seams.count(PROBE_SPAWN);
    let over = prepare(&engine, &payload("sess_128", "bind_1", &project));
    assert_eq!(outcome(&engine, &over), failed("resource_exhausted"));
    assert_eq!(sessions(&machine).len(), 128);
    assert!(entries(&machine, "sess_128").is_empty());
    assert_eq!(machine.seams.count(PROBE_SPAWN), spawned);
    case.observe("the 129th Session failed with resource_exhausted with no record and no probe; 128 are listed");
    case.pass();

    let mut case = record.case("a repeat and a close of existing Sessions at the limit");
    let repeat = prepare(&engine, &first[0].1);
    assert_eq!(outcome(&engine, &repeat), prepared(&project));
    close(&engine, "sess_1", "bind_1");
    quiesce(&machine, 131);
    assert_eq!(state_of(&machine, "sess_1"), "closing");
    case.observe("the repeat got its saved result and the close was handled");
    case.pass();
}

#[test]
fn a_held_probe_blocks_neither_heartbeats_nor_the_shutdown() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("a probe held through four heartbeats");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    machine.seams.hold(PROBE_STARTED);
    let held = prepare(&engine, &payload("sess_held", "bind_1", &project));
    machine.seams.wait_until_waiting(PROBE_STARTED, 1);
    // The ready ACK made the first grant; each heartbeat's ACK makes one.
    for (heartbeat, grants) in (1..=4).zip(2..) {
        engine.advance(seconds(15.0));
        wait_for_received(&engine, "heartbeat", heartbeat);
        machine.seams.wait_for_count("connection.granted", grants);
        machine.advance(seconds(15.0));
    }
    assert_eq!(machine.state(), "online");
    assert_eq!(outcome(&engine, &held), failed("cwd_check_timeout"));
    machine.seams.wait_until_waiting(PROBE_STARTED, 1);
    case.observe("sixty seconds after the ready the connection was online: each heartbeat's ACK moved its deadline while the probe, timed out after five seconds, stayed held unreaped");
    case.pass();

    let mut case = record.case("POST /v1/shutdown with the probe still held");
    let shutdown = request(&machine.app_server.socket, "POST", "/v1/shutdown").unwrap();
    assert_eq!(shutdown.status, 202);
    wait_until("the connection stopped", || machine.state() == "stopped");
    machine.settle();
    assert_eq!(machine.seams.count("shutdown.waiting"), 0);
    machine.seams.advance(seconds(4.999));
    machine.seams.wait_for_more("shutdown.checked", 2);
    assert_eq!(machine.seams.count("shutdown.waiting"), 0);
    machine.seams.advance(seconds(0.001));
    let Machine {
        app_server, seams, ..
    } = machine;
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object()["connection_state"], "stopped");
    assert_eq!(seams.count("shutdown.waiting"), 1);
    case.observe("the shutdown still waited for the unreaped probe once it had read 4.999 seconds on the controlled clock, and the process exited 0 at 5 seconds");
    seams.release(PROBE_STARTED);
    case.pass();
}

#[test]
fn get_sessions_lists_a_staged_preparation_and_the_error_of_a_failed_one() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("a session.prepare staged before the ready ACK");
    answer_fault(
        &engine,
        json!({ "endpoint": "ready", "count": 1, "action": "withhold" }),
    );
    let machine = Machine::start(
        &mut case,
        &base.join("machine"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    wait_for_received(&engine, "ready", 1);
    let staged = prepare(&engine, &payload("sess_staged", "bind_1", &project));
    wait_until("the staged preparation listed", || {
        !sessions(&machine).is_empty()
    });
    assert_eq!(
        sessions(&machine),
        [json!({
            "session_id": "sess_staged",
            "binding_id": "bind_1",
            "connection_id": FIRST_CONNECTION,
            "cwd": null,
            "state": "staged",
            "error": null,
        })]
    );
    case.observe(
        "GET /v1/sessions listed the staged preparation as staged with cwd and error null",
    );
    answer_fault(&engine, json!({ "action": "release" }));
    assert_eq!(outcome(&engine, &staged), prepared(&project));
    case.pass();

    let mut case = record.case("a preparation that failed");
    let missing = prepare(
        &engine,
        &payload("sess_missing", "bind_1", &base.join("missing")),
    );
    assert_eq!(outcome(&engine, &missing), failed("cwd_not_found"));
    let reported = reports_for(&engine, &missing)[0]["error"].clone();
    assert!(reported["message"].is_string(), "{reported}");
    let entry = entries(&machine, "sess_missing")[0].clone();
    assert_eq!(
        (&entry["state"], &entry["error"]),
        (&json!("failed"), &reported)
    );
    case.observe("its entry is failed and carries the reported error's code and message");
    case.pass();
}

/// Writes a `session.prepare` with `payload` on the instance's stream under
/// `message_id`, issuing nothing.
fn send_prepare(engine: &ReferenceServer, machine: &Machine, message_id: &str, payload: &Value) {
    send(
        engine,
        json!({
            "protocol_version": 1,
            "connection_id": machine.status()["connection_id"],
            "message_id": message_id,
            "type": "session.prepare",
            "payload": payload,
        }),
    );
}

#[test]
fn a_report_for_an_unissued_instruction_or_another_binding_is_refused_and_never_sent_again() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("a preparation Engine never issued");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    send_prepare(
        &engine,
        &machine,
        "msg_unissued",
        &payload("sess_unissued", "bind_1", &project),
    );
    let refused = wait_for_requests(&engine, "session-status", 1);
    assert_eq!(
        (refused[0]["status"].as_i64(), refused[0]["code"].as_str()),
        (Some(409), Some("unknown_instruction"))
    );
    case.observe("its prepared report got 409 unknown_instruction");
    case.pass();

    let mut case = record.case("a preparation whose message_id Engine issued for another binding");
    machine.seams.hold(PROBE_STARTED);
    send_prepare(
        &engine,
        &machine,
        "msg_other",
        &payload("sess_other", "bind_2", &project),
    );
    machine.seams.wait_until_waiting(PROBE_STARTED, 1);
    engine.control(
        "prepare",
        json!({
            "instance_id": INSTANCE,
            "message_id": "msg_other",
            "payload": payload("sess_other", "bind_1", &project),
        }),
    );
    wait_for_received(&engine, "protocol-error", 1);
    machine.seams.release(PROBE_STARTED);
    let refused = wait_for_requests(&engine, "session-status", 2);
    assert_eq!(
        (refused[1]["status"].as_i64(), refused[1]["code"].as_str()),
        (Some(409), Some("unknown_instruction"))
    );
    case.observe("Engine's copy under the same message_id was a message_conflict; the report for the binding the CLI prepared got 409 unknown_instruction");
    case.pass();

    let mut case = record.case("past the first retry wait, a later report");
    wait_until("two report.dropped records", || {
        records(&machine.root, "report.dropped").len() == 2
    });
    for dropped in records(&machine.root, "report.dropped") {
        assert_eq!(dropped["error.code"], "unknown_instruction");
    }
    machine.advance(seconds(1.5));
    let later = prepare(&engine, &payload("sess_later", "bind_1", &project));
    assert_eq!(outcome(&engine, &later), prepared(&project));
    let sent = engine.requests("session-status");
    assert_eq!(sent.len(), 3);
    let mut message_ids: Vec<&str> = sent
        .iter()
        .map(|request| request["message_id"].as_str().unwrap())
        .collect();
    message_ids.sort_unstable();
    message_ids.dedup();
    assert_eq!(message_ids.len(), 3);
    case.observe("both refusals were recorded as report.dropped and neither report was sent again; the later report was accepted");
    case.pass();
}

#[test]
fn a_new_binding_finds_no_room_in_a_replay_ledger_with_only_its_reserve_free() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("4,032 closes recorded, then a preparation of a new binding");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    let connection_id = machine.status()["connection_id"].clone();
    // The ledger's 4,096 entries less its 64 reserved ones; closes of
    // Sessions without a record stay recorded, since nothing reports them.
    let filling = 4_032;
    for index in 0..filling {
        send(
            &engine,
            json!({
                "protocol_version": 1,
                "connection_id": connection_id,
                "message_id": format!("msg_fill_{index}"),
                "type": "session.close",
                "payload": { "session_id": format!("sess_fill_{index}"), "binding_id": "bind_1" },
            }),
        );
    }
    machine.seams.wait_for_count("route.session", filling);
    let message_id = prepare(&engine, &payload("sess_new", "bind_1", &project));
    assert_eq!(outcome(&engine, &message_id), failed("resource_exhausted"));
    assert_eq!(sessions(&machine), Vec::<Value>::new());
    assert_eq!(machine.seams.count(PROBE_SPAWN), 0);
    case.observe("the preparation failed with resource_exhausted, with no record and no probe");
    case.pass();
}

#[test]
fn a_preparation_addressed_to_an_earlier_connection_fails_with_stale_connection() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let project = directory_with_marker(base.join("project"));
    let engine = one_credential_engine();

    let mut case = record.case("a session.prepare naming the connection before the current one");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    // The second connection's ready ACK is withheld, so that the copy naming
    // the first connection waits in staging while Engine issues the same
    // message_id on the second; the report then answers an issued
    // instruction and is accepted.
    answer_fault(
        &engine,
        json!({ "endpoint": "ready", "count": 1, "action": "withhold" }),
    );
    send(
        &engine,
        json!({
            "protocol_version": 1,
            "connection_id": FIRST_CONNECTION,
            "message_id": "msg_disconnect",
            "type": "disconnect",
            "payload": { "scope": "connection", "retryable": true, "code": "stale_connection" },
        }),
    );
    machine.wait_for("disconnected");
    engine.advance(seconds(46.0));
    machine.seams.advance(seconds(1.2));
    machine.wait_for("registering");
    let stale = payload("sess_stale", "bind_1", &project);
    send(
        &engine,
        json!({
            "protocol_version": 1,
            "connection_id": FIRST_CONNECTION,
            "message_id": "msg_stale",
            "type": "session.prepare",
            "payload": stale,
        }),
    );
    engine.control(
        "prepare",
        json!({ "instance_id": INSTANCE, "message_id": "msg_stale", "payload": stale }),
    );
    wait_for_received(&engine, "protocol-error", 1);
    engine.control("answer", json!({ "action": "release" }));
    let current = machine.wait_for("online")["connection_id"].clone();
    assert_eq!(outcome(&engine, "msg_stale"), failed("stale_connection"));
    assert_eq!(
        reports_for(&engine, "msg_stale")[0]["connection_id"],
        current
    );
    quiesce(&machine, 1);
    assert!(entries(&machine, "sess_stale").is_empty());
    assert_eq!(machine.seams.count(PROBE_SPAWN), 0);
    case.observe("the copy naming the first connection failed with stale_connection on the second; the Session lists no binding and no probe was spawned");
    case.pass();
}
