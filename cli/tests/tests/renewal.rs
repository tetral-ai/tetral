//! Credential renewal of `tetral server run` against the reference control
//! server, as separate processes over real HTTP and SSE, each on its own
//! controlled clock: a connection that ends before the fetch, fetches that
//! Engine refuses or answers with a malformed key, a credential that cannot
//! be written, a restart right after the write, and reports in flight, held
//! back or submitted across the resume with the new credential. The seam
//! counters these tests read are those `tetral_tests::machine` names, and
//! `outbox.key_awaited`, which the Report outbox reports for each attempt
//! it does not start while it waits for the switch's new key; the
//! Connection owner waits at the barrier `renewal.persisted` once the new
//! credential is written.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, answer_fault, bootstrap, credential,
    one_credential_engine, seconds, send, wait_for_received, wait_for_requests,
};
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{TaskMachine, policy};

/// The pending credential that each renewal fetches.
const RENEWED: &str = "ccred_renewed";
const RENEWED_KEY: &str = "tetral_ck_renewed";

/// Past the first wait of a request's retry backoff: 1 second and at most
/// 20% of jitter.
const PAST_FIRST_RETRY: f64 = 1.5;

/// A time within the connection's grant that holds many of a fetch's
/// retry waits, after which a fetch that was not sent again never is.
const NO_RETRY_WITHIN: f64 = 10.0;

/// The reference server's lease after its last grant, with a margin.
const PAST_LEASE: f64 = 46.0;

fn online_machine(
    case: &mut tetral_tests::record::Case,
    engine: &ReferenceServer,
) -> (Machine, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    (machine, directory)
}

/// Adds the pending credential `credential_id` with `key`, which the
/// reference server offers on the instance's connection as
/// `credential.renew`.
fn offer(engine: &ReferenceServer, credential_id: &str, key: &str) {
    engine.control(
        "credential",
        json!({
            "action": "add", "renew": true,
            "credential": credential(INSTANCE, credential_id, key, "pending"),
        }),
    );
}

/// Writes a `credential.renew` for `credential_id` on the stream.
fn notice(engine: &ReferenceServer, message_id: &str, credential_id: &str) {
    send(
        engine,
        json!({
            "protocol_version": 1,
            "connection_id": FIRST_CONNECTION,
            "message_id": message_id,
            "type": "credential.renew",
            "payload": { "credential_id": credential_id },
        }),
    );
}

/// Writes `data` on the instance's stream as one event's data.
fn send_data(engine: &ReferenceServer, data: &str) {
    engine.control("send", json!({ "instance_id": INSTANCE, "data": data }));
}

fn credential_state(engine: &ReferenceServer, credential_id: &str) -> Value {
    engine.state()["credentials"]
        .as_array()
        .unwrap()
        .iter()
        .find(|credential| credential["credential_id"] == credential_id)
        .cloned()
        .unwrap_or(Value::Null)
}

fn held_count(engine: &ReferenceServer) -> usize {
    engine.state()["held"].as_array().map_or(0, Vec::len)
}

/// The records of `event` that the App Server has written.
fn records(root: &Path, event: &str) -> Vec<Value> {
    let log = std::fs::read_to_string(root.join("logs/tetral.log")).unwrap_or_default();
    log.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|record| record["event"] == event)
        .collect()
}

/// Whether any record shows the connection `blocked`.
fn ever_blocked(root: &Path) -> bool {
    records(root, "connection.state_changed")
        .iter()
        .any(|record| record["connection.state"] == "blocked")
}

fn credentials_file(root: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(root.join("credentials.json")).unwrap()).unwrap()
}

#[test]
fn a_notice_while_registering_is_fetched_once_online_and_not_when_the_connection_ends_first() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("credential.renew while registering, then ready's ACK");
    let engine = one_credential_engine();
    answer_fault(&engine, json!({"endpoint": "ready", "action": "withhold"}));
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("registering");
    wait_until("the ready held", || held_count(&engine) == 1);
    offer(&engine, RENEWED, RENEWED_KEY);
    assert_eq!(credential_state(&engine, RENEWED)["renewals_sent"], 1);
    assert!(engine.requests("credential").is_empty());
    answer_fault(&engine, json!({"action": "release"}));
    wait_until("the switch to the renewed credential", || {
        machine.status()["credential_id"] == RENEWED
    });
    let fetches = engine.requests("credential");
    assert_eq!(fetches.len(), 1, "{fetches:?}");
    assert_eq!(fetches[0]["status"], 200);
    assert_eq!(credential_state(&engine, RENEWED)["state"], "active");
    case.observe("no fetch while registering; once ready's ACK came the notice was fetched once and the connection switched to the renewed credential");
    case.pass();
    drop(machine);

    let mut case = record
        .case("credential.renew while registering, then the connection ends before ready's ACK");
    let engine = one_credential_engine();
    answer_fault(&engine, json!({"endpoint": "ready", "action": "withhold"}));
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("registering");
    wait_until("the ready held", || held_count(&engine) == 1);
    offer(&engine, RENEWED, RENEWED_KEY);
    assert_eq!(credential_state(&engine, RENEWED)["renewals_sent"], 1);
    send(
        &engine,
        json!({
            "protocol_version": 1,
            "connection_id": FIRST_CONNECTION,
            "message_id": "msg_connection_disconnect",
            "type": "disconnect",
            "payload": { "scope": "connection", "retryable": true, "code": "stale_connection" },
        }),
    );
    machine.wait_for("disconnected");
    answer_fault(&engine, json!({"action": "release"}));
    engine.advance(seconds(PAST_LEASE));
    machine.advance(seconds(PAST_FIRST_RETRY));
    let status = machine.wait_for("online");
    assert_ne!(status["connection_id"], FIRST_CONNECTION);
    assert_eq!(status["credential_id"], CREDENTIAL);
    let renewed = credential_state(&engine, RENEWED);
    assert_eq!(
        (
            &renewed["state"],
            &renewed["fetched"],
            &renewed["renewals_sent"]
        ),
        (&json!("pending"), &json!(false), &json!(1))
    );
    assert!(engine.requests("credential").is_empty());
    assert_eq!(credentials_file(&machine.root)["credential_id"], CREDENTIAL);
    case.observe("the notice came while registering and the connection ended before ready's ACK, so no fetch was sent; the next connection is online on the old credential, no credential.renew was sent on it, and the renewed credential stays pending and unfetched");
    case.pass();
}

#[test]
fn a_fetch_that_engine_answers_404_is_sent_once_and_the_current_credential_stays() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("fetches of credentials that cannot be fetched");
    let engine = one_credential_engine();
    let (machine, _directory) = online_machine(&mut case, &engine);
    for (credential_id, state, instance) in [
        ("ccred_other_instance", "pending", "inst_other"),
        ("ccred_active", "active", INSTANCE),
        ("ccred_expired", "expired", INSTANCE),
        ("ccred_revoked", "revoked", INSTANCE),
    ] {
        engine.control(
            "credential",
            json!({
                "action": "add",
                "credential": credential(instance, credential_id, &format!("tetral_ck_{credential_id}"), state),
            }),
        );
    }
    let ids = [
        "ccred_unknown",
        "ccred_other_instance",
        "ccred_active",
        "ccred_expired",
        "ccred_revoked",
    ];
    for (index, credential_id) in ids.iter().enumerate() {
        notice(&engine, &format!("msg_renew_{index}"), credential_id);
        let requests = wait_for_requests(&engine, "credential", index + 1);
        assert_eq!(
            (
                requests[index]["status"].as_i64(),
                requests[index]["code"].as_str()
            ),
            (Some(404), Some("not_found")),
            "{credential_id}"
        );
        wait_until("the rejection's record", || {
            records(&machine.root, "credential.renewal_rejected").len() == index + 1
        });
    }
    case.observe("each of an unknown, another instance's, an active, an expired and a revoked credential got one fetch answered 404 not_found");

    answer_fault(&engine, json!({"endpoint": "credential", "action": "drop"}));
    offer(&engine, RENEWED, RENEWED_KEY);
    wait_for_requests(&engine, "credential", ids.len() + 1);
    machine.advance(seconds(PAST_FIRST_RETRY));
    let requests = wait_for_requests(&engine, "credential", ids.len() + 2);
    let (dropped, retry) = (&requests[ids.len()], &requests[ids.len() + 1]);
    assert_eq!(dropped["message_id"], retry["message_id"]);
    assert_eq!(
        (retry["status"].as_i64(), retry["code"].as_str()),
        (Some(404), Some("not_found"))
    );
    case.observe("a fetch whose 200 the reference server dropped was sent again with its message_id and got 404 not_found, the key's one answer being gone");

    machine.advance(seconds(NO_RETRY_WITHIN));
    assert_eq!(engine.requests("credential").len(), ids.len() + 2);
    wait_until("the last rejection's record", || {
        records(&machine.root, "credential.renewal_rejected").len() == ids.len() + 1
    });
    let rejected = records(&machine.root, "credential.renewal_rejected");
    let named: Vec<&str> = rejected
        .iter()
        .map(|record| record["credential.id"].as_str().unwrap())
        .collect();
    let mut expected = ids.to_vec();
    expected.push(RENEWED);
    assert_eq!(named, expected);
    assert!(
        rejected
            .iter()
            .all(|record| record["error.code"] == "not_found" && record["level"] == "WARN")
    );
    let status = machine.status();
    assert_eq!(
        (
            &status["connection_state"],
            &status["connection_id"],
            &status["credential_id"]
        ),
        (
            &json!("online"),
            &json!(FIRST_CONNECTION),
            &json!(CREDENTIAL)
        )
    );
    assert_eq!(credentials_file(&machine.root)["credential_id"], CREDENTIAL);
    assert_eq!(engine.requests("events").len(), 1);
    case.observe("no 404 was sent again within 10 seconds; one WARN credential.renewal_rejected not_found per notice; the connection stays online on its credential and credentials.json is unchanged");
    case.pass();
}

#[test]
fn a_fetched_key_without_its_prefix_is_never_installed_or_fetched_again() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a 200 whose connection_key lacks tetral_ck_");
    let engine = one_credential_engine();
    let (machine, _directory) = online_machine(&mut case, &engine);
    answer_fault(
        &engine,
        json!({
            "endpoint": "credential", "action": "schema_fail",
            "body": {
                "credential_id": RENEWED, "connection_key": "ck_renewed",
                "expires_at": "2099-01-01T00:00:00Z",
            },
        }),
    );
    let before = std::fs::read(machine.root.join("credentials.json")).unwrap();
    offer(&engine, RENEWED, RENEWED_KEY);
    wait_for_requests(&engine, "credential", 1);
    wait_until("the rejection's record", || {
        !records(&machine.root, "credential.renewal_rejected").is_empty()
    });
    machine.advance(seconds(NO_RETRY_WITHIN));
    let rejected = records(&machine.root, "credential.renewal_rejected");
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0]["error.code"], "invalid_response");
    assert_eq!(rejected[0]["credential.id"], RENEWED);
    assert_eq!(engine.requests("credential").len(), 1);
    assert_eq!(engine.requests("events").len(), 1);
    assert_eq!(
        std::fs::read(machine.root.join("credentials.json")).unwrap(),
        before
    );
    assert_eq!(machine.status()["credential_id"], CREDENTIAL);
    assert_eq!(machine.state(), "online");
    case.observe("one fetch; WARN credential.renewal_rejected invalid_response; no second fetch, no resume and credentials.json byte-identical 10 seconds later; online on the current credential");
    case.pass();
}

#[test]
fn a_credential_that_cannot_be_written_is_not_used_and_the_connection_stays_online() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("the state root made non-writable while the fetch is answered");
    let engine = one_credential_engine();
    let (machine, _directory) = online_machine(&mut case, &engine);
    answer_fault(
        &engine,
        json!({"endpoint": "credential", "action": "hold_request"}),
    );
    offer(&engine, RENEWED, RENEWED_KEY);
    wait_until("the fetch held", || held_count(&engine) == 1);
    let before = std::fs::read(machine.root.join("credentials.json")).unwrap();
    std::fs::set_permissions(&machine.root, std::fs::Permissions::from_mode(0o555)).unwrap();
    answer_fault(&engine, json!({"action": "release"}));
    wait_until("the write's failure record", || {
        !records(&machine.root, "credential.renewal_not_saved").is_empty()
    });
    machine.advance(seconds(PAST_FIRST_RETRY));
    let after = std::fs::read(machine.root.join("credentials.json")).unwrap();
    std::fs::set_permissions(&machine.root, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(after, before);
    let failure = &records(&machine.root, "credential.renewal_not_saved")[0];
    assert_eq!(
        (
            &failure["level"],
            &failure["error.code"],
            &failure["credential.id"]
        ),
        (
            &json!("ERROR"),
            &json!("credential_not_saved"),
            &json!(RENEWED)
        )
    );
    assert_eq!(engine.requests("events").len(), 1);
    assert_eq!(credential_state(&engine, RENEWED)["state"], "pending");
    let status = machine.status();
    assert_eq!(
        (&status["connection_state"], &status["credential_id"]),
        (&json!("online"), &json!(CREDENTIAL))
    );
    case.observe("the fetch succeeded, the write failed and was recorded; credentials.json is byte-identical, no resume was attempted, the new credential stays pending and the connection stays online on the old one");
    case.pass();
}

#[test]
fn a_restart_after_the_write_connects_with_the_new_credential() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a kill at renewal.persisted, then a restart");
    let engine = one_credential_engine();
    let (machine, _directory) = online_machine(&mut case, &engine);
    let Machine {
        app_server,
        seams,
        root,
    } = machine;
    seams.hold("renewal.persisted");
    offer(&engine, RENEWED, RENEWED_KEY);
    seams.wait_until_waiting("renewal.persisted", 1);
    app_server.kill();
    seams.release("renewal.persisted");
    assert_eq!(credentials_file(&root)["credential_id"], RENEWED);
    assert_eq!(credentials_file(&root)["connection_key"], RENEWED_KEY);
    engine.advance(seconds(PAST_LEASE));
    let app_server = Machine::run(&root, &seams);
    let machine = Machine {
        app_server,
        seams,
        root,
    };
    let status = machine.wait_for("online");
    assert_eq!(status["credential_id"], RENEWED);
    let events = engine.requests("events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["query"]["credential_id"], RENEWED);
    assert_eq!(credential_state(&engine, RENEWED)["state"], "active");
    assert_eq!(credential_state(&engine, CREDENTIAL)["state"], "revoked");
    case.observe("credentials.json held the new credential at the kill; after the restart the first connection used it and the reference server committed it, revoking the old one");
    case.pass();
}

#[test]
fn reports_in_flight_held_back_or_submitted_across_the_switch_go_with_the_new_key() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("four reports around a renewal's resume");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let engine = &tasks.engine;
    let machine = tasks.machine();
    let root = &machine.root;

    // A terminal report that the task submits during the switch.
    machine.seams.hold("task.exit_observed");
    tasks.execute(
        "task_switch",
        json!({"cmd": "true", "on_wait_expiry": "kill", "wait_ms": 1000}),
        policy(&tasks.project, "disabled"),
    );
    machine.seams.wait_until_waiting("task.exit_observed", 1);
    // A report whose attempt with the old key waits at the reference server
    // until after the resume, and then meets the revoked key.
    answer_fault(
        engine,
        json!({"endpoint": "protocol-error", "action": "hold_request"}),
    );
    send_data(engine, "not json");
    wait_until("the first report held", || held_count(engine) == 1);
    // A report accepted before the switch whose ACK is held across it.
    answer_fault(
        engine,
        json!({"endpoint": "protocol-error", "action": "withhold"}),
    );
    send_data(engine, "[1]");
    wait_until("the second report held", || held_count(engine) == 2);
    assert_eq!(engine.received("protocol-error").len(), 1);

    engine.control(
        "events",
        json!({"mode": "delay_connected", "delay_ms": 1000, "count": 1}),
    );
    offer(engine, RENEWED, RENEWED_KEY);
    machine.wait_for("resuming");
    let awaited = machine.seams.count("outbox.key_awaited");
    machine.seams.release("task.exit_observed");
    machine
        .seams
        .wait_for_count("outbox.key_awaited", awaited + 1);
    assert!(engine.requests("tool-status").is_empty());
    case.observe("during the switch the task's terminal report waited for the new key");

    engine.advance(seconds(1.0));
    wait_until("the resume with the new credential", || {
        machine.status()["credential_id"] == RENEWED
    });
    let resume = engine.requests("events");
    let resume = resume.last().unwrap();
    assert_eq!(
        (
            &resume["query"]["credential_id"],
            &resume["query"]["resume"],
            &resume["connection_id"]
        ),
        (&json!(RENEWED), &json!("true"), &json!(FIRST_CONNECTION))
    );
    answer_fault(engine, json!({"action": "release"}));
    send_data(engine, "[2]");
    wait_for_received(engine, "protocol-error", 3);
    tasks.terminal("task_switch");
    machine.advance(seconds(PAST_FIRST_RETRY));

    let requests = engine.requests("protocol-error");
    let first = requests
        .iter()
        .find(|request| {
            request["credential_id"] == CREDENTIAL && request["code"] == "credential_revoked"
        })
        .expect("the held attempt met the revoked key");
    // Refused before its body is read, the attempt's log names no
    // message_id: the same bytes are the same report.
    let again: Vec<&Value> = requests
        .iter()
        .filter(|request| request["body_sha256"] == first["body_sha256"])
        .collect();
    assert_eq!(again.len(), 2, "{requests:?}");
    assert_eq!(
        (&again[1]["credential_id"], &again[1]["status"]),
        (&json!(RENEWED), &json!(200))
    );
    let later = requests.last().unwrap();
    assert_eq!(
        (&later["credential_id"], &later["status"]),
        (&json!(RENEWED), &json!(200))
    );
    assert_eq!(engine.received("protocol-error").len(), 3);
    let terminal = engine.requests("tool-status");
    assert_eq!(terminal.len(), 1, "{terminal:?}");
    assert_eq!(
        (&terminal[0]["credential_id"], &terminal[0]["status"]),
        (&json!(RENEWED), &json!(200))
    );
    assert_eq!(machine.state(), "online");
    assert!(!ever_blocked(root));
    assert_eq!(credential_state(engine, CREDENTIAL)["state"], "revoked");
    case.observe("the held attempt got 401 credential_revoked under the old key and was sent again with the new key and its message_id; the report accepted before the switch got its ACK after the resume; the terminal report submitted during the switch and the report after it went with the new key only; every report was acknowledged and the connection never entered blocked");
    case.pass();
}
