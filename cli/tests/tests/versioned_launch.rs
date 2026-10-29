//! An installer changes its `current` symlink while an App Server keeps the
//! executable and bundled helper paths of its retained version directory.
//! Both installations stay present and unchanged until their servers exit.

#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use serde_json::{Value, json};
use tetral_tests::app_server::{AppServer, request, wait_until};
use tetral_tests::binaries;
use tetral_tests::machine::{
    APP, CREDENTIAL, INSTANCE, KEY, bootstrap, one_credential_engine, tool_execution_fragment,
    wait_for_received,
};
use tetral_tests::reference::ReferenceServer;
use tetral_tests::run::Tetral;
use tetral_tests::seams::Seams;
use tetral_tests::socket_directory;
use tetral_tests::tasks::policy;

fn install(directory: &Path) {
    fs::create_dir(directory).unwrap();
    let executable = directory.join("tetral");
    // The two fixtures use the same build. Their actual executed paths,
    // observed below, distinguish installations without changing live files.
    if fs::hard_link(binaries::tetral(), &executable).is_err() {
        fs::copy(binaries::tetral(), &executable).unwrap();
    }
    let bwrap = ["/usr/bin/bwrap", "/bin/bwrap"]
        .into_iter()
        .find(|path| Path::new(path).is_file())
        .expect("the Linux acceptance host has bubblewrap");
    fs::copy(bwrap, directory.join("bwrap")).unwrap();
}

fn start(current: &Path, root: &Path, seams: &Seams) -> AppServer {
    let server = AppServer::start(
        Tetral::at(&current.join("tetral"), ["server", "run", "--app", APP])
            .home(root)
            // No system bwrap is offered: the real launch must find the
            // bundled helper beside its own resolved executable.
            .env("PATH", "")
            .seams(seams),
        root,
    );
    wait_until("the versioned server is online", || {
        server.status()["connection_state"] == "online"
    });
    server
}

/// All children, including ones spawned by a worker thread, while the task
/// entry is held at an existing barrier. No process is signalled or changed.
fn descendant_images(parent: u32) -> BTreeSet<PathBuf> {
    let mut pending = vec![parent];
    let mut visited = BTreeSet::new();
    let mut images = BTreeSet::new();
    while let Some(pid) = pending.pop() {
        if !visited.insert(pid) {
            continue;
        }
        if pid != parent {
            images.insert(fs::read_link(format!("/proc/{pid}/exe")).unwrap());
        }
        for thread in fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
            let children = fs::read_to_string(thread.unwrap().path().join("children")).unwrap();
            pending.extend(
                children
                    .split_whitespace()
                    .map(|pid| pid.parse::<u32>().unwrap()),
            );
        }
    }
    images
}

fn reported(engine: &ReferenceServer, endpoint: &str, instruction: &str) -> Value {
    let mut report = Value::Null;
    wait_until("the versioned launch report", || {
        let found = engine
            .received(endpoint)
            .into_iter()
            .map(|entry| entry["body"].clone())
            .find(|body| body["request_message_id"] == instruction);
        match found {
            Some(found) => {
                report = found;
                true
            }
            None => false,
        }
    });
    report
}

fn exercise(
    server: &AppServer,
    seams: &Seams,
    engine: &ReferenceServer,
    version: &Path,
    project: &Path,
    binding: &str,
) {
    assert_eq!(
        fs::read_link(format!("/proc/{}/exe", server.pid())).unwrap(),
        version.join("tetral")
    );
    let prepare = engine.control(
        "prepare",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": "sess_version", "binding_id": binding, "cwd": project,
            "drives": [], "drive_exclude": [], "repositories": [],
        }}),
    );
    let prepared = reported(
        engine,
        "session-status",
        prepare["message_id"].as_str().unwrap(),
    );
    assert_eq!(prepared["status"], "prepared", "{prepared}");

    fs::write(project.join("text"), "before").unwrap();
    let png = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a1Z0AAAAASUVORK5CYII=")
        .unwrap();
    fs::write(project.join("image.png"), &png).unwrap();
    for (tool, input, barrier) in [
        (
            "exec_command",
            json!({"cmd": "printf versioned", "wait_ms": 1000}),
            "task.launch",
        ),
        (
            "edit",
            json!({"path": "text", "old_string": "before", "new_string": "after"}),
            "file_tool.started",
        ),
        (
            "view_image",
            json!({"path": "image.png"}),
            "file_tool.started",
        ),
    ] {
        seams.hold(barrier);
        let issued = engine.control(
            "execute",
            json!({"instance_id": INSTANCE, "payload": {
                "session_id": "sess_version", "binding_id": binding,
                "task_id": format!("{binding}_{tool}"),
                "tool_use_event_id": format!("call_{binding}_{tool}"),
                "tool": tool, "input": input, "sandbox_policy": policy(project, "direct"),
            }}),
        );
        seams.wait_until_waiting(barrier, 1);
        let images = descendant_images(server.pid());
        // Both the actual restricted entry and bubblewrap must come from
        // this server's installation. Merely producing a successful result
        // would not distinguish two installations of the same test build.
        assert_eq!(
            images,
            BTreeSet::from([version.join("tetral"), version.join("bwrap")]),
            "executables of {tool} at {barrier}"
        );
        seams.release(barrier);
        let report = reported(
            engine,
            "tool-status",
            issued["message_id"].as_str().unwrap(),
        );
        assert_eq!(report["phase"], "terminal", "{report}");
        assert_eq!(report["result"]["status"], "success", "{report}");
        match tool {
            "exec_command" => assert_eq!(report["result"]["result"]["stdout"]["text"], "versioned"),
            "edit" => assert_eq!(fs::read(project.join("text")).unwrap(), b"after"),
            "view_image" => assert_eq!(
                report["result"]["result"]["data_base64"],
                base64::engine::general_purpose::STANDARD.encode(&png)
            ),
            _ => unreachable!(),
        }
    }
}

fn stop(server: AppServer) {
    assert_eq!(
        request(&server.socket, "POST", "/v1/shutdown")
            .unwrap()
            .status,
        202
    );
    let (run, _) = server.exited();
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
}

#[test]
fn a_running_server_keeps_its_version_when_current_changes() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "current switches from A to B while A retains its tool entries and bundled helper; a later server uses B",
    );
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().canonicalize().unwrap();
    let (a, b) = (base.join("A"), base.join("B"));
    install(&a);
    install(&b);
    let current = base.join("current");
    symlink(&a, &current).unwrap();
    let engine = one_credential_engine();
    let root = base.join("root");
    let configured = Tetral::at(
        &current.join("tetral"),
        ["configure", "--app", APP, "--from-stdin"],
    )
    .home(&root)
    .stdin(serde_json::to_vec(&bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY)).unwrap())
    .run();
    assert_eq!(configured.status.code(), Some(0), "{}", configured.exit());
    let project = base.join("project");
    fs::create_dir(&project).unwrap();
    let seams = Seams::serve(&base, Duration::from_secs(1_000));
    let old = start(&current, &root, &seams);
    symlink(&b, base.join("next")).unwrap();
    fs::rename(base.join("next"), &current).unwrap();
    assert_eq!(current.canonicalize().unwrap(), b);
    exercise(&old, &seams, &engine, &a, &project, "bind_a");
    stop(old);
    // The previous logical connection's lease must end before the next
    // server registers. No executable or helper is replaced or removed.
    engine.advance(Duration::from_secs(46));
    let new = start(&current, &root, &seams);
    exercise(&new, &seams, &engine, &b, &project, "bind_b");
    let ready = wait_for_received(&engine, "ready", 2);
    let expected: Value = serde_json::from_str(&tool_execution_fragment()).unwrap();
    for ready in ready {
        assert_eq!(ready["body"]["capabilities"]["tool_execution"], expected);
    }
    stop(new);
    for version in [&a, &b] {
        assert!(version.join("tetral").is_file());
        assert!(version.join("bwrap").is_file());
    }
    case.observe("the normal ready advertised all tools; preparation and all three tool calls succeeded after the pointer switch, with actual child and bundled-helper executable paths in A, then in B after orderly restart; both installations remained present");
    case.pass();
}
