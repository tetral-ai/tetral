//! The Connection owner of `tetral server run` against the reference control
//! server, as separate processes over real HTTP and SSE, each on its own
//! controlled clock: credential and version refusals, a credential expired
//! before start, the attempt timeout and the reconnect backoff, Session
//! instructions staged until the ready ACK and their limit, a stream lost
//! before that ACK, grants measured from first sends, conflicts and the
//! predecessor rules, a clock that jumps past the deadline, answers of an
//! ended connection, the event size limit and the protocol version, the
//! startup phases in ready, and a redirect that is never followed. The seam
//! counters these tests read are those `tetral_tests::machine` names.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    APP, CLI_START, CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, answer_fault, bootstrap,
    credential, engine, one_credential_engine, seconds, send, wait_for_received, wait_for_requests,
};
use tetral_tests::record::Case;
use tetral_tests::run::Tetral;
use tetral_tests::seams::Seams;
use tetral_tests::socket_directory;

/// The test seam barrier between the Connection owner taking a message and
/// acting on the times that have passed.
const CONNECTION_MESSAGE: &str = "connection.message";

fn prepare(index: usize) -> Value {
    json!({
        "session_id": format!("sess_{index}"),
        "binding_id": format!("bind_{index}"),
        "cwd": "/tmp",
        "drives": [],
        "drive_exclude": [],
        "repositories": [],
    })
}

fn session_close(message_id: &str, protocol_version: i64) -> Value {
    json!({
        "protocol_version": protocol_version,
        "connection_id": FIRST_CONNECTION,
        "message_id": message_id,
        "type": "session.close",
        "payload": { "session_id": "sess_closed", "binding_id": "bind_closed" },
    })
}

#[test]
fn a_refused_credential_blocks_before_ready_and_no_attempt_follows() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let engine = engine(
        &[
            credential(INSTANCE, CREDENTIAL, KEY, "active"),
            credential("inst_two", "ccred_two", "tetral_ck_instance_two", "active"),
            credential(INSTANCE, "ccred_revoked", "tetral_ck_revoked", "revoked"),
            credential(INSTANCE, "ccred_expired", "tetral_ck_expired", "expired"),
        ],
        json!({}),
    );
    for (name, credential_id, key, code) in [
        (
            "a key for another instance",
            "ccred_two",
            "tetral_ck_instance_two",
            "credential_invalid",
        ),
        (
            "a key for another credential ID",
            "ccred_other",
            KEY,
            "credential_invalid",
        ),
        (
            "an invalid key",
            CREDENTIAL,
            "tetral_ck_unknown",
            "credential_invalid",
        ),
        (
            "a revoked key",
            "ccred_revoked",
            "tetral_ck_revoked",
            "credential_revoked",
        ),
        (
            "an expired key",
            "ccred_expired",
            "tetral_ck_expired",
            "credential_expired",
        ),
    ] {
        let mut case = record.case(&format!("{name}: 401 {code}, blocked"));
        let directory = tempfile::tempdir().unwrap();
        let before = engine.requests("events").len();
        let machine = Machine::start(
            &mut case,
            directory.path(),
            &bootstrap(&engine.url, INSTANCE, credential_id, key),
        );
        let status = machine.wait_for("blocked");
        assert_eq!(status["last_error"]["code"], code);
        assert_eq!(status["connection_id"], Value::Null);
        machine.advance(seconds(120.0));
        let events = engine.requests("events");
        assert_eq!(events.len(), before + 1, "{events:?}");
        assert_eq!(events[before]["status"], 401);
        assert_eq!(events[before]["code"], code);
        assert_eq!(machine.attempts(), 1);
        assert_eq!(machine.state(), "blocked");
        case.observe(format!(
            "one events request answered 401 {code}; blocked with that last_error; still one attempt 120 seconds later"
        ));
    }
    assert!(engine.requests("ready").is_empty());
}

#[test]
fn a_credential_expired_before_start_blocks_and_sends_no_request() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("credentials.json with an expires_at before this machine's time");
    let engine = one_credential_engine();
    let mut expired = bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY);
    expired["expires_at"] = json!("2000-01-01T00:00:00Z");
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(&mut case, directory.path(), &expired);
    let status = machine.wait_for("blocked");
    assert_eq!(status["last_error"]["code"], "credential_expired");
    machine.advance(seconds(120.0));
    assert_eq!(machine.attempts(), 0);
    assert_eq!(engine.state()["requests"].as_array().map_or(0, Vec::len), 0);
    case.observe("blocked with last_error credential_expired; no attempt and no request recorded by the reference server 120 seconds later");
}

#[test]
fn a_grant_capped_at_the_credential_expiry_ends_the_connection_there() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("a credential expiring 10 seconds after the grant starts");
    let mut expiring = credential(INSTANCE, CREDENTIAL, KEY, "active");
    expiring["expires_at"] = json!("2030-01-01T00:00:10Z");
    let engine = engine(&[expiring], json!({}));
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    machine.advance(seconds(9.999));
    assert_eq!(machine.state(), "online");
    machine.advance(seconds(0.001));
    assert_eq!(machine.state(), "disconnected");
    case.observe(
        "online 9.999 seconds after the ready's first send; at 10 seconds the connection ended",
    );

    engine.advance(seconds(10.0));
    machine.advance(seconds(1.2));
    let status = machine.wait_for("blocked");
    assert_eq!(status["last_error"]["code"], "credential_expired");
    let events = engine.requests("events");
    assert_eq!(events.last().unwrap()["code"], "credential_expired");
    case.observe(
        "after the expiry the next attempt got 401 credential_expired and the CLI is blocked",
    );
}

#[test]
fn failed_attempts_end_at_ten_seconds_and_wait_by_the_backoff() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case =
        record.case("seven failed attempts, online with one confirmed heartbeat, then a failure");
    let engine = one_credential_engine();
    let modes = [
        "hold_headers",
        "headers_only",
        "comments_only",
        "hold_headers",
        "headers_only",
        "comments_only",
        "hold_headers",
    ];
    for mode in modes {
        engine.control("events", json!({ "mode": mode, "count": 1 }));
    }
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    let bases = [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0];
    for (index, (mode, base)) in modes.iter().zip(bases).enumerate() {
        let attempt = index as u64 + 1;
        machine.seams.wait_for_count("connection.attempt", attempt);
        wait_for_requests(&engine, "events", index + 1);
        // A comment written meanwhile resets nothing.
        engine.advance(seconds(1.0));
        machine.advance(seconds(9.999));
        assert_eq!(machine.state(), "connecting", "attempt {attempt}, {mode}");
        machine.advance(seconds(0.001));
        assert_eq!(machine.state(), "disconnected", "attempt {attempt}, {mode}");
        machine.advance(seconds(base - 0.001));
        assert_eq!(
            machine.attempts(),
            attempt,
            "the wait after attempt {attempt}"
        );
        machine.seams.advance(seconds(base * 0.2 + 0.001));
        machine
            .seams
            .wait_for_count("connection.attempt", attempt + 1);
        case.observe(format!(
            "attempt {attempt} ({mode}) connecting at 9.999 seconds and failed at 10; the next began after at least {base} and at most {} seconds",
            base * 1.2
        ));
    }
    machine.wait_for("online");
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.advance(seconds(29.999));
    assert_eq!(machine.attempts(), 8);
    machine.seams.advance(seconds(6.002));
    machine.seams.wait_for_count("connection.attempt", 9);
    let events = wait_for_requests(&engine, "events", 9);
    assert_eq!(events[8]["query"]["resume"], "true");
    machine.wait_for("online");
    case.observe("online; the stream broken before any heartbeat ACK was resumed after at least 30 and at most 36 seconds, the ready ACK resetting no backoff");

    engine.advance(seconds(15.0));
    machine.seams.wait_for_count("connection.granted", 2);
    case.observe("the heartbeat 15 seconds later was acknowledged");

    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.advance(seconds(0.999));
    assert_eq!(machine.attempts(), 9);
    machine.seams.advance(seconds(0.201));
    machine.seams.wait_for_count("connection.attempt", 10);
    let events = wait_for_requests(&engine, "events", 10);
    assert_eq!(events[9]["query"]["resume"], "true");
    case.observe(
        "after the stream broke, the resume began after at least 1 and at most 1.2 seconds",
    );
}

#[test]
fn session_instructions_wait_for_the_ready_ack() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("staged until the matching ready ACK");
    let engine = one_credential_engine();
    answer_fault(
        &engine,
        json!({ "endpoint": "ready", "action": "wrong_identity" }),
    );
    answer_fault(
        &engine,
        json!({ "endpoint": "ready", "action": "withhold" }),
    );
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    wait_for_received(&engine, "ready", 1);

    send(
        &engine,
        json!({
            "protocol_version": 1,
            "connection_id": FIRST_CONNECTION,
            "message_id": "msg_stream_disconnect",
            "type": "disconnect",
            "payload": { "scope": "stream" },
        }),
    );
    let events = wait_for_requests(&engine, "events", 2);
    assert_eq!(events[1]["query"]["resume"], "true");
    assert_eq!(events[1]["connection_id"], FIRST_CONNECTION);
    machine.wait_for("registering");
    wait_for_received(&engine, "heartbeat", 1);
    case.observe("while staging, a stream-only disconnect resumed the connection at once and the resumed stream's heartbeat was answered");

    // The second and third prepare one binding with two payloads, so the
    // order they are routed in decides which one conflicts.
    let mut repeated = prepare(1);
    repeated["cwd"] = json!("/");
    let issued: Vec<Value> = [prepare(0), prepare(1), repeated]
        .into_iter()
        .map(|payload| {
            engine.control(
                "prepare",
                json!({ "instance_id": INSTANCE, "payload": payload }),
            )["message_id"]
                .clone()
        })
        .collect();
    engine.advance(seconds(15.0));
    wait_for_received(&engine, "heartbeat", 2);
    assert_eq!(machine.routed(), 0);
    case.observe("three session.prepare, the last two for one binding with different cwd, and a later heartbeat arrived; the heartbeat was answered and no instruction was routed");

    machine.seams.advance(seconds(1.2));
    let readies = wait_for_requests(&engine, "ready", 2);
    assert_eq!(readies[0]["message_id"], readies[1]["message_id"]);
    assert_eq!(machine.routed(), 0);
    assert_eq!(machine.state(), "registering");
    case.observe(
        "the ACK with a wrong identity released nothing; ready was sent again with its message_id",
    );

    answer_fault(&engine, json!({ "action": "release" }));
    machine.seams.wait_for_count("route.session", 3);
    machine.wait_for("online");
    machine.settle();
    assert_eq!(machine.routed(), 3);
    let answered = |message_id: &Value| {
        let mut report = Value::Null;
        wait_until(&format!("the report answering {message_id}"), || {
            report = engine
                .received("session-status")
                .into_iter()
                .map(|received| received["body"].clone())
                .find(|body| body["request_message_id"] == *message_id)
                .unwrap_or(Value::Null);
            !report.is_null()
        });
        (report["status"].clone(), report["error"]["code"].clone())
    };
    assert_eq!(answered(&issued[1]), (json!("prepared"), Value::Null));
    assert_eq!(
        answered(&issued[2]),
        (json!("failed"), json!("binding_conflict"))
    );
    case.observe("after the matching ACK the three staged instructions were routed in arrival order: of the two for one binding the first prepared and the second got binding_conflict; the connection is online");
}

#[test]
fn a_stream_lost_before_the_ready_ack_resumes_the_connection_without_a_second_ready() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("the stream ends after connected and before the ready is answered");
    let engine = one_credential_engine();
    answer_fault(
        &engine,
        json!({ "endpoint": "ready", "action": "withhold" }),
    );
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    wait_for_received(&engine, "ready", 1);
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(1.2));
    let events = wait_for_requests(&engine, "events", 2);
    assert_eq!(events[1]["query"]["resume"], "true");
    assert_eq!(
        events[1]["query"]["previous_connection_id"],
        FIRST_CONNECTION
    );
    assert_eq!(events[1]["connection_id"], FIRST_CONNECTION);
    machine.wait_for("registering");
    case.observe(
        "the next attempt resumed the same connection with resume=true, which is registering again",
    );

    answer_fault(&engine, json!({ "action": "release" }));
    machine.wait_for("online");
    machine.settle();
    assert_eq!(engine.requests("ready").len(), 1);
    case.observe("the first ready's ACK brought online and no second ready was sent");
}

#[test]
fn sixty_four_staged_instructions_are_kept_and_one_more_abandons_the_connection() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    for (count, abandoned) in [(64, false), (65, true)] {
        let mut case = record.case(&format!(
            "{count} Session instructions before the ready ACK"
        ));
        let engine = one_credential_engine();
        let instructions: Vec<Value> = (0..count)
            .map(|index| json!({ "type": "session.prepare", "payload": prepare(index) }))
            .collect();
        engine.control(
            "dispatch_before_ready_ack",
            json!({ "instance_id": INSTANCE, "instructions": instructions }),
        );
        answer_fault(
            &engine,
            json!({ "endpoint": "ready", "action": "withhold" }),
        );
        let directory = tempfile::tempdir().unwrap();
        let machine = Machine::start(
            &mut case,
            directory.path(),
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        );
        wait_for_received(&engine, "ready", 1);
        if !abandoned {
            engine.advance(seconds(15.0));
            wait_for_received(&engine, "heartbeat", 1);
            answer_fault(&engine, json!({ "action": "release" }));
            machine.seams.wait_for_count("route.session", 64);
            let status = machine.wait_for("online");
            assert_eq!(status["connection_id"], FIRST_CONNECTION);
            machine.settle();
            assert_eq!(machine.routed(), 64);
            case.observe(
                "64 staged instructions were all routed after the ACK, on the same connection",
            );
            continue;
        }
        machine.wait_for("disconnected");
        assert_eq!(machine.routed(), 0);
        machine.advance(seconds(1.2));
        let events = wait_for_requests(&engine, "events", 2);
        assert_eq!(
            events[1]["query"]["previous_connection_id"],
            FIRST_CONNECTION
        );
        assert_eq!(events[1]["query"]["resume"], "false");
        assert_eq!(events[1]["code"], "previous_connection_active");
        machine.wait_for("disconnected");
        case.observe("the 65th abandoned the connection unrouted; the next attempt named it with resume=false and got 409 previous_connection_active");

        engine.advance(seconds(45.0));
        machine.advance(seconds(45.0));
        let status = machine.wait_for("online");
        assert_ne!(status["connection_id"], FIRST_CONNECTION);
        assert_eq!(machine.routed(), 0);
        case.observe("after the abandoned connection's lease, a new connection is online");
    }
}

#[test]
fn a_replayed_ack_grants_from_the_first_send() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    for (name, fault) in [
        (
            "a dropped ready ACK, replayed",
            json!({ "endpoint": "ready", "action": "drop" }),
        ),
        (
            "a ready ACK with valid_for_ms 45,001, then a valid one",
            json!({
                "endpoint": "ready",
                "action": "schema_fail",
                "body": {
                    "connection_id": FIRST_CONNECTION,
                    "message_id": "c1",
                    "accepted": true,
                    "valid_for_ms": 45_001,
                },
            }),
        ),
    ] {
        let mut case = record.case(name);
        let engine = one_credential_engine();
        answer_fault(&engine, fault);
        let directory = tempfile::tempdir().unwrap();
        let machine = Machine::start(
            &mut case,
            directory.path(),
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        );
        wait_for_received(&engine, "ready", 1);
        machine.seams.wait_for_count("connection.answered", 1);
        machine.settle();
        assert_eq!(machine.state(), "registering");
        machine.seams.advance(seconds(1.2));
        machine.wait_for("online");
        let readies = engine.requests("ready");
        assert_eq!(readies.len(), 2);
        assert_eq!(readies[0]["message_id"], "c1");
        assert_eq!(readies[1]["message_id"], "c1");
        assert_eq!(engine.received("ready").len(), 1);
        machine.advance(seconds(43.799));
        assert_eq!(machine.state(), "online");
        machine.advance(seconds(0.001));
        assert_ne!(machine.state(), "online");
        case.observe("ready was sent again as c1 and online followed only the valid ACK; the deadline was the first send plus 45 seconds, not the retry's");
    }

    let mut case = record.case("a dropped heartbeat ACK, replayed");
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        &directory.path().join("dropped"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    machine.advance(seconds(20.0));
    answer_fault(
        &engine,
        json!({ "endpoint": "heartbeat", "action": "drop" }),
    );
    engine.advance(seconds(15.0));
    wait_for_received(&engine, "heartbeat", 1);
    machine.seams.wait_for_count("connection.answered", 2);
    machine.settle();
    machine.seams.advance(seconds(1.2));
    machine.seams.wait_for_count("connection.granted", 2);
    let heartbeats = engine.requests("heartbeat");
    assert_eq!(heartbeats.len(), 2);
    assert_eq!(heartbeats[0]["message_id"], heartbeats[1]["message_id"]);
    // First sent 20 seconds after the ready, the heartbeat holds the
    // connection to 65 seconds after it; from the retry it would be 66.2.
    machine.advance(seconds(43.799));
    assert_eq!(machine.state(), "online");
    machine.advance(seconds(0.001));
    assert_ne!(machine.state(), "online");
    case.observe("the heartbeat was sent again under its message_id; its replayed ACK held the connection to the first send plus 45 seconds");

    let mut case = record.case("an older heartbeat ACK after a newer one");
    let engine = one_credential_engine();
    let machine = Machine::start(
        &mut case,
        &directory.path().join("older"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    machine.advance(seconds(10.0));
    answer_fault(
        &engine,
        json!({ "endpoint": "heartbeat", "action": "delay", "delay_ms": 20_000 }),
    );
    engine.advance(seconds(15.0));
    wait_for_received(&engine, "heartbeat", 1);
    machine.advance(seconds(5.0));
    engine.advance(seconds(15.0));
    wait_for_received(&engine, "heartbeat", 2);
    machine.seams.wait_for_count("connection.granted", 2);
    let answered = machine.seams.count("connection.answered");
    engine.advance(seconds(5.0));
    machine
        .seams
        .wait_for_count("connection.answered", answered + 1);
    machine.settle();
    assert_eq!(machine.seams.count("connection.granted"), 2);
    case.observe(
        "the delayed ACK of the first heartbeat arrived after the second's and renewed nothing",
    );

    // The heartbeats were first sent 10 and 15 seconds after the ready; had
    // the first one's ACK renewed, the connection would end at 55.
    machine.advance(seconds(42.0));
    assert_eq!(machine.state(), "online");
    machine.advance(seconds(3.0));
    assert_ne!(machine.state(), "online");
    case.observe("the connection lasted to the second heartbeat's first send plus 45 seconds");
}

#[test]
fn a_conflict_blocks_only_from_forty_five_seconds_after_start() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "two more App Servers while the first holds the live connection, each conflicting at once and then once more 44.999 or 45 seconds after its start",
    );
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let first_bootstrap = bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY);
    let first = Machine::start(&mut case, &directory.path().join("first"), &first_bootstrap);
    first.wait_for("online");
    let mut seen = Vec::new();
    for (name, after_start, expected) in [
        ("second", 44.999, "disconnected"),
        ("third", 45.0, "blocked"),
    ] {
        let machine = Machine::start(&mut case, &directory.path().join(name), &first_bootstrap);
        wait_until("the first attempt's conflict", || {
            machine.attempts() == 1 && machine.state() == "disconnected"
        });
        // The retry is due within 1.2 seconds, so it starts, and its answer
        // comes, at the time the clock is moved to.
        machine.advance(CLI_START + seconds(after_start) - machine.seams.now());
        wait_until("the second attempt's conflict", || {
            machine.attempts() == 2 && machine.state() != "connecting"
        });
        let status = machine.status();
        assert_eq!(status["connection_state"], expected, "{name}");
        assert_eq!(
            status["last_error"]["code"], "connection_conflict",
            "{name}"
        );
        seen.push(format!("{after_start} s: {expected}"));
    }
    let events = engine.requests("events");
    assert_eq!(events.len(), 5);
    assert!(
        events[1..]
            .iter()
            .all(|event| event["code"] == "connection_conflict")
    );
    let first_status = first.status();
    assert_eq!(first_status["connection_state"], "online");
    assert_eq!(first_status["connection_id"], FIRST_CONNECTION);
    case.observe(format!(
        "every attempt of the other two got 409 connection_conflict; a conflict 44.999 seconds after start was retried and one at 45 seconds blocked ({}); the first stayed online on its connection",
        seen.join(", ")
    ));

    let mut case = record.case("an immediate restart after server stop");
    let stop = Tetral::new(["server", "stop", "--app", APP])
        .home(&first.root)
        .run();
    case.command("tetral server stop --app app_one", &stop.exit());
    let Machine {
        app_server,
        seams: first_seams,
        root: first_root,
    } = first;
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one (first)", &run.exit());
    drop(first_seams);
    let restart_directory = directory.path().join("restart");
    std::fs::create_dir_all(&restart_directory).unwrap();
    let seams = Seams::serve(&restart_directory, CLI_START);
    let before = engine.requests("events").len();
    let restarted = Machine {
        app_server: Machine::run(&first_root, &seams),
        seams,
        root: first_root,
    };
    case.command("tetral server run --app app_one (restart)", "running");
    let events = wait_for_requests(&engine, "events", before + 1);
    assert_eq!(events[before]["code"], "connection_conflict");
    restarted.wait_for("disconnected");
    engine.advance(seconds(46.0));
    restarted.seams.advance(seconds(1.2));
    let status = restarted.wait_for("online");
    assert_ne!(status["connection_id"], FIRST_CONNECTION);
    assert_eq!(status["last_error"], Value::Null);
    case.observe("the restart got 409 connection_conflict while the old lease lasted, was not blocked, and was online at its first attempt after the lease");
}

#[test]
fn a_deadline_passed_in_one_step_ends_the_connection_and_a_live_lease_delays_the_next() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case =
        record.case("the clock moves past the deadline in one step with the stream open");
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        &directory.path().join("sleep"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    let stepped = Instant::now();
    machine.seams.advance(seconds(46.0));
    wait_until("the connection's end", || machine.state() != "online");
    let took = stepped.elapsed();
    assert!(took < seconds(1.0), "{took:?}");
    case.observe(format!(
        "the connection ended {took:?} after the step, at the stepped time"
    ));
    drop(machine);

    let mut case = record.case("resume withheld past the connection deadline");
    let engine = one_credential_engine();
    let machine = Machine::start(
        &mut case,
        &directory.path().join("withheld"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    engine.control("events", json!({ "mode": "hold_headers", "count": 1 }));
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(1.2));
    let events = wait_for_requests(&engine, "events", 2);
    assert_eq!(events[1]["query"]["resume"], "true");
    engine.advance(seconds(44.5));
    machine.advance(seconds(43.81));
    assert_eq!(machine.state(), "disconnected");
    machine.seams.advance(seconds(2.4));
    let events = wait_for_requests(&engine, "events", 3);
    assert_eq!(
        events[2]["query"]["previous_connection_id"],
        FIRST_CONNECTION
    );
    assert_eq!(events[2]["query"]["resume"], "false");
    assert_eq!(events[2]["code"], "previous_connection_active");
    machine.wait_for("disconnected");
    let attempts = machine.attempts();
    machine.advance(seconds(0.999));
    assert_eq!(machine.attempts(), attempts);
    engine.advance(seconds(1.0));
    machine.seams.advance(seconds(0.001));
    let status = machine.wait_for("online");
    assert_ne!(status["connection_id"], FIRST_CONNECTION);
    case.observe("the held resume ran past the deadline, which ended the connection; the next attempt got 409 previous_connection_active with 500 ms left and waited 1 second; then a new connection was online");
}

#[test]
fn an_ack_of_an_ended_connection_changes_nothing() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("a heartbeat ACK delivered after a new connection");
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    answer_fault(
        &engine,
        json!({ "endpoint": "heartbeat", "action": "delay", "delay_ms": 60_000 }),
    );
    engine.advance(seconds(15.0));
    wait_for_received(&engine, "heartbeat", 1);
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
    engine.advance(seconds(46.0));
    machine.seams.advance(seconds(1.2));
    let status = machine.wait_for("online");
    assert_ne!(status["connection_id"], FIRST_CONNECTION);
    machine.seams.wait_for_count("connection.granted", 2);

    let answered = machine.seams.count("connection.answered");
    engine.advance(seconds(14.0));
    machine
        .seams
        .wait_for_count("connection.answered", answered + 1);
    machine.settle();
    assert_eq!(machine.seams.count("connection.granted"), 2);
    case.observe("the old connection's delayed heartbeat ACK arrived on the new connection and granted nothing");

    machine.advance(seconds(44.999));
    assert_eq!(machine.state(), "online");
    machine.advance(seconds(0.001));
    assert_ne!(machine.state(), "online");
    case.observe("the new connection ended at its own ready's first send plus 45 seconds");
}

#[test]
fn the_event_limit_and_the_protocol_version_are_checked_before_an_event_is_handled() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("protocol_version 2 and unknown optional fields");
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    let mut extended = session_close("msg_unknown_fields", 1);
    extended["extension"] = json!({ "kept": false });
    extended["payload"]["future"] = json!(true);
    send(&engine, extended);
    send(&engine, session_close("msg_version_two", 2));
    // The heartbeat after them shows both were handled.
    engine.advance(seconds(15.0));
    wait_for_received(&engine, "heartbeat", 1);
    assert_eq!(machine.routed(), 1);
    case.observe("the instruction with unknown members was routed and the one with protocol_version 2 was not");

    // `connected` is the event that every connection reads before its ready.
    let mut case = record.case("a connected event of 65,537 data bytes, then one of 65,536");
    let engine = one_credential_engine();
    for data_bytes in [65_537, 65_536] {
        engine.control(
            "events",
            json!({ "mode": "pad_connected", "data_bytes": data_bytes }),
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    wait_for_requests(&engine, "events", 1);
    machine.wait_for("disconnected");
    assert_eq!(engine.requests("ready").len(), 0);
    // The server's connection of that stream holds the instance until its
    // lease ends, since the CLI never learned it.
    engine.advance(seconds(45.0));
    machine.seams.advance(seconds(1.201));
    machine.seams.wait_for_count("connection.attempt", 2);
    let ready = wait_for_received(&engine, "ready", 1);
    machine.wait_for("online");
    let events = engine.requests("events");
    assert_eq!(
        ready[0]["body"]["connection_id"],
        events[1]["connection_id"]
    );
    case.observe("the connected of 65,537 data bytes closed the stream and no ready was sent; the next attempt's connected of 65,536 was processed and its ready sent");
}

#[test]
fn a_cli_below_the_minimum_version_is_blocked() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("ready refused with 403 cli_upgrade_required");
    let engine = engine(
        &[credential(INSTANCE, CREDENTIAL, KEY, "active")],
        json!({ "min_cli_version": "999.0.0" }),
    );
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    let status = machine.wait_for("blocked");
    assert_eq!(status["last_error"]["code"], "cli_upgrade_required");
    machine.advance(seconds(120.0));
    assert_eq!(machine.attempts(), 1);
    assert_eq!(engine.requests("ready").len(), 1);
    case.observe("blocked with cli_upgrade_required; no attempt or ready followed in 120 seconds");
}

#[test]
fn a_ready_without_an_ack_for_forty_five_seconds_abandons_the_connection() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case =
        record.case("ready kept from the reference server for 45 seconds after connected");
    let engine = one_credential_engine();
    answer_fault(
        &engine,
        json!({ "endpoint": "ready", "action": "error", "code": "unavailable", "status": 503, "count": 2 }),
    );
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    wait_for_requests(&engine, "ready", 1);
    machine.seams.wait_for_count("connection.answered", 1);
    machine.advance(seconds(44.999));
    wait_for_requests(&engine, "ready", 2);
    assert_eq!(machine.state(), "registering");
    machine.advance(seconds(0.001));
    assert_eq!(machine.state(), "disconnected");
    assert!(engine.received("ready").is_empty());
    case.observe(
        "ready never reached the reference server; registering at 44.999 seconds, abandoned at 45",
    );

    machine.advance(seconds(1.2));
    let events = wait_for_requests(&engine, "events", 2);
    assert_eq!(
        events[1]["query"]["previous_connection_id"],
        FIRST_CONNECTION
    );
    assert_eq!(events[1]["query"]["resume"], "false");
    assert_eq!(events[1]["code"], "previous_connection_active");
    machine.wait_for("disconnected");
    engine.advance(seconds(45.0));
    machine.advance(seconds(45.0));
    let status = machine.wait_for("online");
    assert_ne!(status["connection_id"], FIRST_CONNECTION);
    case.observe("the retry got 409 previous_connection_active, and after the lease a new connection is online");
}

#[test]
fn a_heartbeat_ack_taken_only_after_the_connection_deadline_renews_nothing() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a heartbeat first sent 15 seconds after the ready, its ACK taken by the Connection owner once the connection deadline has passed",
    );
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    machine.advance(seconds(15.0));
    answer_fault(
        &engine,
        json!({ "endpoint": "heartbeat", "action": "withhold" }),
    );
    engine.advance(seconds(15.0));
    wait_for_received(&engine, "heartbeat", 1);
    machine.seams.hold(CONNECTION_MESSAGE);
    engine.control("answer", json!({ "action": "release" }));
    machine.seams.wait_until_waiting(CONNECTION_MESSAGE, 1);
    machine.seams.advance(seconds(30.0));
    machine.seams.release(CONNECTION_MESSAGE);
    assert_ne!(machine.state(), "online");
    assert_eq!(machine.seams.count("connection.granted"), 1);
    case.observe("the connection ended at the ready's first send plus 45 seconds; the heartbeat's ACK, valid to 60 seconds, set no deadline");
}

/// The startup records of the App Server's log file, as
/// `(event, phase)` pairs in order.
fn startup_records(root: &Path) -> Vec<(String, String)> {
    let log = std::fs::read_to_string(root.join("logs/tetral.log")).unwrap();
    log.lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|record| {
            record["event"]
                .as_str()
                .is_some_and(|event| event.starts_with("cold_start."))
        })
        .map(|record| {
            assert_eq!(record["cold_start.kind"], "cli");
            (
                record["event"].as_str().unwrap().to_owned(),
                record["cold_start.phase"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn stop(case: &mut Case, machine: Machine) -> PathBuf {
    let stop = Tetral::new(["server", "stop", "--app", APP])
        .home(&machine.root)
        .run();
    case.command("tetral server stop --app app_one", &stop.exit());
    let (run, _) = machine.app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    machine.root
}

#[test]
fn ready_carries_the_startup_phases_until_one_is_accepted() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("a first ready answered 409 stale_connection, then an accepted one");
    let engine = one_credential_engine();
    answer_fault(
        &engine,
        json!({ "endpoint": "ready", "action": "error", "code": "stale_connection" }),
    );
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        &directory.path().join("stale"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    wait_for_requests(&engine, "ready", 1);
    machine.wait_for("disconnected");
    machine.seams.advance(seconds(1.2));
    let events = wait_for_requests(&engine, "events", 2);
    assert_eq!(events[1]["code"], "previous_connection_active");
    machine.wait_for("disconnected");
    engine.advance(seconds(45.0));
    machine.seams.advance(seconds(45.0));
    machine.wait_for("online");
    let readies = engine.received("ready");
    assert_eq!(readies.len(), 1);
    assert_eq!(
        readies[0]["body"]["startup"],
        json!({ "config_ms": 0, "connect_ms": 0, "ready_ms": 46_200 })
    );
    let root = stop(&mut case, machine);
    let phases = ["config", "connect", "ready"];
    let expected: Vec<(String, String)> = phases
        .iter()
        .flat_map(|phase| {
            ["cold_start.phase_started", "cold_start.phase_completed"]
                .map(|event| (event.to_owned(), (*phase).to_owned()))
        })
        .collect();
    assert_eq!(startup_records(&root), expected);
    case.observe("the accepted ready carried ready_ms 46,200, from the first connected to its own first send; the log holds each phase's start and completion once, in order");

    let mut case = record.case("a connect phase held past 2,147,483,647 ms");
    let engine = one_credential_engine();
    engine.control("events", json!({ "mode": "hold_headers", "count": 1 }));
    let machine = Machine::start(
        &mut case,
        &directory.path().join("held"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    wait_for_requests(&engine, "events", 1);
    machine.advance(seconds(2_147_484.0));
    machine.seams.advance(seconds(1.2));
    machine.wait_for("online");
    let readies = engine.received("ready");
    assert_eq!(
        readies[0]["body"]["startup"],
        json!({ "config_ms": 0, "connect_ms": 2_147_483_647, "ready_ms": 0 })
    );
    case.observe("connect_ms was sent as 2,147,483,647");

    let mut case =
        record.case("a first connected delayed by 2 seconds, a resume and a new connection");
    let engine = one_credential_engine();
    engine.control(
        "events",
        json!({ "mode": "delay_connected", "delay_ms": 2_000 }),
    );
    let machine = Machine::start(
        &mut case,
        &directory.path().join("delayed"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.seams.wait_for_count("connection.attempt", 1);
    // The CLI's clock moves first, so that the connected event it waits for
    // arrives 2 seconds after its process started.
    machine.advance(seconds(2.0));
    engine.advance(seconds(2.0));
    machine.wait_for("online");
    let first = engine.received("ready")[0]["body"]["startup"].clone();
    let phase = |name: &str| first[name].as_i64().unwrap();
    assert!(phase("connect_ms") >= 2_000, "{first}");
    assert!(
        phase("config_ms") + phase("connect_ms") + phase("ready_ms") <= 2_000,
        "{first}"
    );
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(2.4));
    machine.wait_for("online");
    assert_eq!(engine.requests("ready").len(), 1);
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
    engine.advance(seconds(46.0));
    machine.seams.advance(seconds(4.8));
    machine.wait_for("online");
    let readies = engine.received("ready");
    assert_eq!(readies.len(), 2);
    assert_eq!(readies[1]["body"].get("startup"), None);
    case.observe(format!(
        "the first ready, 2,000 ms after the process started, carried {first}; the resume sent no ready; the new connection's ready carried no startup"
    ));
}

/// A server that answers every request with a redirect to `target`.
fn redirecting_server(target: &str) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let answered = Arc::new(AtomicUsize::new(0));
    let counted = answered.clone();
    let location = format!("{target}/v1/client/events");
    std::thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(mut connection) = connection else {
                continue;
            };
            let mut reader = BufReader::new(connection.try_clone().unwrap());
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|read| read > 2) {
                line.clear();
            }
            counted.fetch_add(1, Ordering::SeqCst);
            let _ = connection.write_all(
                format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            );
        }
    });
    (format!("http://{address}"), answered)
}

/// A server that only counts the connections it accepts.
fn counting_server() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counted = accepted.clone();
    std::thread::spawn(move || {
        for connection in listener.incoming() {
            if connection.is_ok() {
                counted.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    (format!("http://{address}"), accepted)
}

#[test]
fn a_redirect_is_never_followed() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("a server_url that answers with a redirect");
    let (target, reached) = counting_server();
    let (redirecting, redirected) = redirecting_server(&target);
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&redirecting, INSTANCE, CREDENTIAL, KEY),
    );
    for attempt in 1..=3 {
        machine.seams.wait_for_count("connection.attempt", attempt);
        wait_until("the redirect", || {
            redirected.load(Ordering::SeqCst) >= attempt as usize
        });
        machine.wait_for("disconnected");
        machine
            .seams
            .advance(seconds(2.0_f64.powi(attempt as i32 - 1) * 1.2));
    }
    machine.seams.wait_for_count("connection.attempt", 4);
    assert_eq!(reached.load(Ordering::SeqCst), 0);
    assert_eq!(machine.status()["last_error"], Value::Null);
    case.observe("three attempts were redirected, each counted as a failed attempt; the redirect target received no request");
}
