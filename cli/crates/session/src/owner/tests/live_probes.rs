//! Linux process evidence runs in its own test subprocess: making the shared
//! test runner a child subreaper would change unrelated tests' child ownership.
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::time::Instant;

use rustix::process::{Pid, Signal, WaitOptions, kill_process, waitpid};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;

use super::*;

const CASE: &str = "TETRAL_LIVE_PROBE_CASE";
const LIMIT: Duration = Duration::from_secs(5);
const TEST: &str =
    "owner::tests::live_probes::ending_a_preparation_terminates_and_reaps_its_live_probe_tree";

#[test]
fn ending_a_preparation_terminates_and_reaps_its_live_probe_tree() {
    if let Ok(reason) = std::env::var(CASE) {
        rustix::process::set_child_subreaper(Pid::from_raw(1)).unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(check_end(&reason));
        return;
    }
    for reason in ["timeout", "close", "connection_end"] {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env(CASE, reason)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("live probe {reason} subprocess exceeded its watchdog");
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(status.success(), "live probe {reason}: {status}");
    }
}

// Own only these two fixture PIDs. On assertion failure, kill and reap any
// still-owned child before the isolated subreaper exits. Never waitpid(-1).
struct Children(Vec<Pid>);
impl Children {
    fn reaped(&mut self, pid: Pid) {
        self.0.retain(|owned| *owned != pid);
    }
}
impl Drop for Children {
    fn drop(&mut self) {
        for pid in &self.0 {
            if matches!(waitpid(Some(*pid), WaitOptions::NOHANG), Ok(None)) {
                let _ = kill_process(*pid, Signal::KILL);
                let _ = waitpid(Some(*pid), WaitOptions::empty());
            }
        }
    }
}

async fn check_end(reason: &str) {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("ready.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let python = Command::new("python3")
        .args(["-c", "import sys; print(sys.executable)"])
        .output()
        .unwrap();
    assert!(python.status.success());
    let python = String::from_utf8(python.stdout).unwrap();
    let program = directory.path().join("probe");
    // Each process holds stdout open and waits on a private handshake socket.
    // A direct-child-only kill leaves the descendant alive and blocks EOF.
    std::fs::write(
        &program,
        format!(
            r#"#!{}
import json, os, socket
child = os.fork()
s = socket.socket(socket.AF_UNIX)
s.connect({})
if child:
    message = {{"parent": os.getpid(), "descendant": child}}
else:
    message = {{"child": os.getpid()}}
s.sendall((json.dumps(message) + "\n").encode())
s.recv(1)
os._exit(0)
"#,
            python.trim(),
            serde_json::to_string(&socket).unwrap()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (mut owner, connection, reports) = probe_owner(program);
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    owner.mailbox = mailbox;
    owner.handle(prepare_binding("bind_1"));
    let request = connection.requests.lock().unwrap()[0];
    let operation = owner.requests[&request];
    owner.handle(SessionMessage::ProbeSlot {
        request,
        answer: ProbeSlot::Granted,
    });
    assert_eq!(owner.probes.len(), 1);
    let mut children = Children(Vec::new());
    let mut peers = Vec::new();
    let mut child_ready = None;
    for _ in 0..2 {
        let (stream, _) = tokio::time::timeout(LIMIT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(LIMIT, reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let hello: serde_json::Value = serde_json::from_str(&line).unwrap();
        if let Some(parent) = hello["parent"].as_i64() {
            children.0 = vec![
                Pid::from_raw(parent.try_into().unwrap()).unwrap(),
                Pid::from_raw(hello["descendant"].as_i64().unwrap().try_into().unwrap()).unwrap(),
            ];
        } else {
            child_ready = hello["child"].as_i64();
        }
        peers.push(reader);
    }
    let [parent, descendant] = children.0[..] else {
        panic!("missing parent handshake")
    };
    assert_eq!(
        child_ready,
        Some(i64::from(descendant.as_raw_nonzero().get()))
    );
    assert!(
        waitpid(Some(parent), WaitOptions::NOHANG)
            .unwrap()
            .is_none()
    );
    for peer in &peers {
        assert_eq!(
            peer.get_ref().try_read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    assert!(
        messages.try_recv().is_err(),
        "no completion while both processes are live"
    );
    assert_eq!(connection.freed.load(Ordering::SeqCst), 0);

    match reason {
        "timeout" => {
            owner.probes.get_mut(&operation).unwrap().deadline = state::seams::now();
            owner.check_deadlines();
        }
        "close" => {
            let (reply, mut accepted) = oneshot::channel();
            owner.handle(SessionMessage::Close {
                close: close_of("bind_1"),
                reply,
            });
            assert!(accepted.try_recv().is_ok());
        }
        "connection_end" => owner.handle(SessionMessage::ConnectionEnded(id("conn_1"))),
        _ => unreachable!(),
    }
    assert_eq!(
        connection.freed.load(Ordering::SeqCst),
        0,
        "slot held until completion"
    );
    let done = tokio::time::timeout(LIMIT, messages.recv())
        .await
        .expect("termination must end both live processes and deliver completion")
        .unwrap();
    assert!(
        matches!(done, SessionMessage::ProbeDone { operation: completed, output: None } if completed == operation)
    );
    assert_eq!(
        waitpid(Some(parent), WaitOptions::NOHANG).unwrap_err(),
        rustix::io::Errno::CHILD,
        "the direct child was already reaped before completion"
    );
    children.reaped(parent);
    // EOF proves its descriptors closed, but Linux can still be finishing
    // do_exit before the descendant becomes waitable. Observe that transition
    // directly; only the direct child must already be reaped at ProbeDone.
    let status = tokio::time::timeout(LIMIT, async {
        loop {
            if let Some((_, status)) = waitpid(Some(descendant), WaitOptions::NOHANG).unwrap() {
                break status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the terminated descendant must become waitable");
    children.reaped(descendant);
    assert_eq!(status.terminating_signal(), Some(Signal::KILL.as_raw()));
    owner.handle(done);
    assert!(owner.probes.is_empty());
    assert_eq!(connection.freed.load(Ordering::SeqCst), 1);
    let count = reports.0.lock().unwrap().len();
    assert_eq!(count, usize::from(reason == "timeout"));
    if reason == "timeout" {
        let reports = reports.0.lock().unwrap();
        let SessionOutcome::Failed { error } = &reports[0].outcome else {
            panic!("timeout must fail preparation")
        };
        assert_eq!(error.code, PreparationErrorCode::CwdCheckTimeout);
    }
    owner.check_deadlines();
    owner.end_operation(operation);
    assert_eq!(connection.freed.load(Ordering::SeqCst), 1);
    assert_eq!(reports.0.lock().unwrap().len(), count);
    drop(owner);
    assert!(
        tokio::time::timeout(LIMIT, messages.recv())
            .await
            .unwrap()
            .is_none(),
        "exactly one completion, and its sender has ended"
    );
    drop(peers);
}
