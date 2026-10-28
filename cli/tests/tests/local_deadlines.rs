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
                    seams.advance(Duration::from_secs(3));
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
                    case.observe("a complete valid response at three seconds succeeds after the client has processed the advanced clock");
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
