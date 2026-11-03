//! Local command deadlines against an ordinary Unix HTTP peer. No sandbox
//! or signal delivery is involved; the controlled clock advances each call.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::sync::mpsc;
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::{socket_path, wait_until};
use tetral_tests::run::{self, Tetral};
use tetral_tests::seams::{Seams, WAIT_LIMIT};
use tetral_tests::socket_directory;

const DEADLINE_CHECKED: &str = "local.response_deadline_checked";

fn response(command: &str) -> Value {
    if command == "stop" {
        json!({})
    } else {
        json!({
            "server_state": "running", "connection_state": "disconnected",
            "instance_id": "instance", "connection_id": null,
            "credential_id": "credential", "expires_at": "2099-01-01T00:00:00Z",
            "cli_version": env!("CARGO_PKG_VERSION"), "protocol_version": 1,
            "last_error": null
        })
    }
}

#[test]
fn status_and_stop_allow_responses_within_the_deadline_and_close_incomplete_ones_at_it() {
    let _sockets = socket_directory::isolated();
    let record = tetral_tests::record!();
    for command in ["status", "stop"] {
        for partial_body in [false, true] {
            for completes in [false, true] {
                let mut case = record.case(&format!(
                    "{command}, partial body: {partial_body}, completes in time: {completes}"
                ));
                let directory = tempfile::tempdir().unwrap();
                let root = directory.path().join("root");
                let bootstrap = serde_json::to_vec(&json!({
                    "server_url": "http://127.0.0.1:1", "instance_id": "instance",
                    "credential_id": "credential", "connection_key": "tetral_ck_local_deadline",
                    "expires_at": "2099-01-01T00:00:00Z"
                }))
                .unwrap();
                let configured = Tetral::new(["configure", "--app", "app", "--from-stdin"])
                    .home(&root)
                    .stdin(bootstrap)
                    .run();
                assert!(configured.status.success(), "{}", configured.exit());
                let socket = socket_path(&root);
                fs::create_dir_all(socket.parent().unwrap()).unwrap();
                fs::set_permissions(socket.parent().unwrap(), fs::Permissions::from_mode(0o700))
                    .unwrap();
                let listener = UnixListener::bind(&socket).unwrap();
                fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
                let (received, ready) = mpsc::channel();
                let (complete, completion) = mpsc::channel();
                let peer = std::thread::spawn(move || {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream.set_read_timeout(Some(WAIT_LIMIT)).unwrap();
                    let mut request = Vec::new();
                    let mut byte = [0];
                    while !request.ends_with(b"\r\n\r\n") {
                        assert_eq!(stream.read(&mut byte).unwrap(), 1);
                        request.push(byte[0]);
                    }
                    let body = serde_json::to_vec(&response(command)).unwrap();
                    let status = if command == "stop" {
                        "202 Accepted"
                    } else {
                        "200 OK"
                    };
                    let headers = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n",
                        body.len()
                    );
                    if partial_body {
                        stream.write_all(headers.as_bytes()).unwrap();
                        stream.write_all(&body[..1]).unwrap();
                    }
                    received.send(()).unwrap();
                    if completes {
                        completion.recv_timeout(WAIT_LIMIT).unwrap();
                        // A shortened deadline may already have closed the connection.
                        // The command result below, not a peer-side write panic, owns
                        // that failure's diagnosis.
                        if !partial_body {
                            let _ = stream.write_all(headers.as_bytes());
                        }
                        let _ = stream.write_all(&body[usize::from(partial_body)..]);
                    }
                    assert_eq!(
                        stream.read(&mut byte).unwrap(),
                        0,
                        "completion or timeout closes the request's connection"
                    );
                });
                let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
                let mut call = Tetral::new(["server", command, "--app", "app"])
                    .home(&root)
                    .seams(&seams)
                    .spawn();
                ready.recv_timeout(WAIT_LIMIT).unwrap();
                if completes {
                    seams.advance(Duration::from_millis(4_900));
                    let observed = seams.count(DEADLINE_CHECKED);
                    // One count can have been in flight before the clock moved;
                    // another is sent before the timeout branch returns. Three
                    // further checks prove a post-advance timer iteration continued.
                    // Also observe exit so an early timeout fails promptly.
                    wait_until("the client to process the advanced clock or exit", || {
                        seams.count(DEADLINE_CHECKED) >= observed + 3
                            || call.try_wait().unwrap().is_some()
                    });
                    complete.send(()).unwrap();
                } else {
                    seams.advance(Duration::from_secs(5));
                }
                let result = run::finish(call);
                peer.join().unwrap();
                if completes {
                    assert!(
                        result.status.success(),
                        "{}: {}",
                        result.exit(),
                        String::from_utf8_lossy(&result.stdout)
                    );
                    assert_eq!(result.object(), response(command));
                    case.observe("a complete valid response at 4.9 seconds succeeds after the client has processed the advanced clock");
                } else {
                    assert_eq!(result.error_code(), "server_not_responding");
                    case.observe("an incomplete response at the original five-second deadline fails and closes its connection");
                }
                fs::remove_file(socket).unwrap();
                case.command(
                    &format!("tetral server {command} --app app"),
                    &result.exit(),
                );
                case.pass();
            }
        }
    }
}

#[test]
fn diagnostic_status_and_metrics_share_the_original_five_second_deadline() {
    let _sockets = socket_directory::isolated();
    let record = tetral_tests::record!();
    for (silent_status, completes) in [(true, false), (false, false), (false, true)] {
        let mut case = record.case(&format!("diagnostic status silent: {silent_status}, metrics complete before deadline: {completes}"));
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fs::create_dir_all(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = socket_path(&root);
        fs::create_dir_all(socket.parent().unwrap()).unwrap();
        fs::set_permissions(socket.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        let (received, ready) = mpsc::channel();
        let (complete, completion) = mpsc::channel();
        let peer = std::thread::spawn(move || {
            for path in ["/v1/status", "/v1/metrics"] {
                let (mut stream, _) = listener.accept().unwrap();
                stream.set_read_timeout(Some(WAIT_LIMIT)).unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    assert_eq!(stream.read(&mut byte).unwrap(), 1);
                    request.push(byte[0]);
                }
                assert!(
                    String::from_utf8(request)
                        .unwrap()
                        .starts_with(&format!("GET {path} "))
                );
                received.send(path).unwrap();
                if (path == "/v1/status" && !silent_status) || (path == "/v1/metrics" && completes)
                {
                    completion.recv_timeout(WAIT_LIMIT).unwrap();
                    let body = if path == "/v1/status" {
                        serde_json::to_vec(&response("status")).unwrap()
                    } else {
                        b"test_metric 7\n".to_vec()
                    };
                    let headers =
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
                    let _ = stream.write_all(headers.as_bytes());
                    let _ = stream.write_all(&body);
                }
                assert_eq!(
                    stream.read(&mut byte).unwrap(),
                    0,
                    "the completed or expired request closes"
                );
                if silent_status {
                    break;
                }
            }
        });
        let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
        let output = directory.path().join("diagnostic.tar.gz");
        let mut child = Tetral::new(["diag", "--app", "app", "--output", output.to_str().unwrap()])
            .home(&root)
            .seams(&seams)
            .spawn();
        assert_eq!(ready.recv_timeout(WAIT_LIMIT).unwrap(), "/v1/status");
        if !silent_status {
            seams.advance(Duration::from_secs(4));
            complete.send(()).unwrap();
            assert_eq!(ready.recv_timeout(WAIT_LIMIT).unwrap(), "/v1/metrics");
        }
        // The second HTTP request gets only the remaining original budget.
        seams.advance(Duration::from_millis(if silent_status {
            4_900
        } else {
            900
        }));
        let checked = seams.count(DEADLINE_CHECKED);
        wait_until(
            "the diagnostic client reads 4.9 seconds or exits early",
            || seams.count(DEADLINE_CHECKED) >= checked + 3 || child.try_wait().unwrap().is_some(),
        );
        assert!(
            child.try_wait().unwrap().is_none(),
            "no timeout before five seconds"
        );
        if completes {
            complete.send(()).unwrap();
        } else {
            seams.advance(Duration::from_millis(100));
        }
        wait_until(
            "the diagnostic command completes at the shared deadline",
            || child.try_wait().unwrap().is_some(),
        );
        let result = run::finish(child);
        peer.join().unwrap();
        assert!(result.status.success(), "{}", result.exit());
        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(
            fs::File::open(&output).unwrap(),
        ));
        let entries: std::collections::BTreeMap<_, _> = archive
            .entries()
            .unwrap()
            .map(|entry| {
                let mut entry = entry.unwrap();
                let name = entry.path().unwrap().to_string_lossy().into_owned();
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).unwrap();
                (name, bytes)
            })
            .collect();
        let status: Value = serde_json::from_slice(&entries["status.json"]).unwrap();
        if completes {
            assert_eq!(status, response("status"));
            assert_eq!(entries["metrics.txt"], b"test_metric 7\n");
        } else {
            assert_eq!(status, json!({"server_state":"not_running"}));
            assert!(!entries.contains_key("metrics.txt"));
        }
        fs::remove_file(&socket).unwrap();
        case.command("tetral diag --output diagnostic.tar.gz", &result.exit());
        case.observe("the shared deadline includes status and metrics; a valid answer at 4.9 seconds succeeds, a silent request closes at five seconds and contributes no partial status or metrics");
        case.pass();
    }
}
