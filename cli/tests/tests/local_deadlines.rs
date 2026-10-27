//! Local command deadlines against an ordinary Unix HTTP peer. No sandbox
//! or signal delivery is involved; the controlled clock advances each call.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::sync::mpsc;
use std::time::Duration;

use serde_json::json;
use tetral_tests::app_server::socket_path;
use tetral_tests::run::{self, Tetral};
use tetral_tests::seams::{Seams, WAIT_LIMIT};
use tetral_tests::socket_directory;

#[test]
fn status_and_stop_bound_the_complete_response_and_close_timed_out_connections() {
    let _sockets = socket_directory::isolated();
    let record = tetral_tests::record!();
    for command in ["status", "stop"] {
        for partial_body in [false, true] {
            let mut case = record.case(&format!("{command}, partial body: {partial_body}"));
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
            let peer = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream.set_read_timeout(Some(WAIT_LIMIT)).unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    assert_eq!(stream.read(&mut byte).unwrap(), 1);
                    request.push(byte[0]);
                }
                if partial_body {
                    let status = if command == "stop" {
                        "202 Accepted"
                    } else {
                        "200 OK"
                    };
                    write!(stream, "HTTP/1.1 {status}\r\nContent-Length: 2\r\n\r\n{{").unwrap();
                }
                received.send(()).unwrap();
                assert_eq!(
                    stream.read(&mut byte).unwrap(),
                    0,
                    "the timed-out request closes its connection"
                );
            });
            let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
            let mut call = Tetral::new(["server", command, "--app", "app"])
                .home(&root)
                .seams(&seams)
                .spawn();
            ready.recv_timeout(WAIT_LIMIT).unwrap();
            seams.advance(Duration::from_millis(4_999));
            assert!(call.try_wait().unwrap().is_none());
            seams.advance(Duration::from_millis(1));
            let result = run::finish(call);
            assert_eq!(result.error_code(), "server_not_responding");
            peer.join().unwrap();
            fs::remove_file(socket).unwrap();
            case.command(
                &format!("tetral server {command} --app app"),
                &result.exit(),
            );
            case.observe("no complete response at the shared five-second deadline; one typed failure and the HTTP connection closed");
            case.pass();
        }
    }
}
