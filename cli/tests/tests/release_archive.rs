//! The release archive of the machine's target as `cli/scripts/package.sh`
//! writes it: its entries, its digest, and its `tetral` serving against the
//! reference control server. The test is ignored by default. Run it in
//! `cli/` with `TETRAL_TEST_ARCHIVE` naming the archive and
//! `TETRAL_TEST_ARCHIVE_SHA256` the digest the script printed:
//! `cargo test --test release_archive -- --ignored`. The archive's `tetral`
//! is a release build, so it uses the machine's own socket directory.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::{Value, json};
use tetral_tests::app_server::{request, wait_until};
use tetral_tests::machine::{APP, CREDENTIAL, INSTANCE, KEY, bootstrap, one_credential_engine};
use tetral_tests::run::Tetral;

fn variable(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} names the archive under test"))
}

/// Each entry's path and permission bits, sorted by path.
fn entries(archive: &[u8]) -> Vec<(String, u32)> {
    let mut reader = tar::Archive::new(flate2::read::GzDecoder::new(archive));
    let mut listed: Vec<(String, u32)> = reader
        .entries()
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.path().unwrap().to_string_lossy().into_owned(),
                entry.header().mode().unwrap() & 0o7777,
            )
        })
        .collect();
    listed.sort();
    listed
}

/// Whether the system's digest check, as a launcher runs it on
/// `SHA256SUMS`, accepts the file `name` in `directory` against `digest`.
fn digest_check(directory: &Path, name: &str, digest: &str) -> bool {
    fs::write(directory.join("SHA256SUMS"), format!("{digest}  {name}\n")).unwrap();
    let mut check = if cfg!(target_os = "macos") {
        let mut check = Command::new("shasum");
        check.args(["-a", "256", "-c", "SHA256SUMS"]);
        check
    } else {
        let mut check = Command::new("sha256sum");
        check.args(["-c", "SHA256SUMS"]);
        check
    };
    check
        .current_dir(directory)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

/// The `tetral` of the archive with only `TETRAL_HOME` and an empty `PATH`,
/// so that no `bwrap` is found on `PATH`.
fn tetral(program: &Path, root: &Path, arguments: &[&str]) -> Command {
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .env("TETRAL_HOME", root)
        .env("PATH", "");
    command
}

/// The report of `endpoint` that answers the control message `message_id`.
#[cfg(target_os = "linux")]
fn reported(
    engine: &tetral_tests::reference::ReferenceServer,
    endpoint: &str,
    message_id: &Value,
) -> Value {
    let mut report = Value::Null;
    wait_until(&format!("the {endpoint} report"), || {
        let found = engine
            .received(endpoint)
            .into_iter()
            .map(|entry| entry["body"].clone())
            .find(|body| body["request_message_id"] == *message_id && body["phase"] != "running");
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

/// The executables of `parent`'s descendants.
#[cfg(target_os = "linux")]
fn descendant_images(parent: u32) -> Vec<PathBuf> {
    let mut pending = vec![parent];
    let mut images = Vec::new();
    while let Some(pid) = pending.pop() {
        if pid != parent
            && let Ok(image) = fs::read_link(format!("/proc/{pid}/exe"))
        {
            images.push(image);
        }
        let Ok(threads) = fs::read_dir(format!("/proc/{pid}/task")) else {
            continue;
        };
        for thread in threads.flatten() {
            let children = fs::read_to_string(thread.path().join("children")).unwrap_or_default();
            pending.extend(
                children
                    .split_whitespace()
                    .filter_map(|pid| pid.parse::<u32>().ok()),
            );
        }
    }
    images
}

/// On Linux, a prepared Session and an `exec_command` that waits for a file
/// of the test's: while it waits, the `bwrap` among the server's descendants
/// is the archive's, and once the file exists the command completes.
#[cfg(target_os = "linux")]
fn command_under_the_archives_bwrap(
    engine: &tetral_tests::reference::ReferenceServer,
    server: u32,
    install: &Path,
    project: &Path,
) -> String {
    let prepare = engine.control(
        "prepare",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": "sess_release", "binding_id": "bind_release", "cwd": project,
            "drives": [], "drive_exclude": [], "repositories": [],
        }}),
    );
    let prepared = reported(engine, "session-status", &prepare["message_id"]);
    assert_eq!(prepared["status"], "prepared", "{prepared}");
    let issued = engine.control(
        "execute",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": "sess_release", "binding_id": "bind_release",
            "task_id": "task_release", "tool_use_event_id": "call_release",
            "tool": "exec_command",
            "input": {"cmd": "while [ ! -e stop ]; do /bin/sleep 0.05; done; printf served", "wait_ms": 30_000},
            "sandbox_policy": tetral_tests::tasks::policy(project, "disabled"),
        }}),
    );
    let mut bwraps = Vec::new();
    wait_until("the command's bwrap", || {
        bwraps = descendant_images(server)
            .into_iter()
            .filter(|image| image.file_name().is_some_and(|name| name == "bwrap"))
            .collect();
        !bwraps.is_empty()
    });
    fs::write(project.join("stop"), "").unwrap();
    let report = reported(engine, "tool-status", &issued["message_id"]);
    assert!(
        bwraps.iter().all(|image| *image == install.join("bwrap")),
        "{bwraps:?}"
    );
    assert_eq!(
        json!([
            report["result"]["status"],
            report["result"]["result"]["stdout"]["text"]
        ]),
        json!(["success", "served"]),
        "{report}"
    );
    format!("; the exec_command ran under {bwraps:?} and printed served")
}

#[test]
#[ignore = "needs the archive named by TETRAL_TEST_ARCHIVE"]
fn the_archive_holds_its_files_matches_its_digest_and_serves() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "the release archive of this machine's target: its entries, the digest check with one byte flipped, server run",
    );
    let archive = PathBuf::from(variable("TETRAL_TEST_ARCHIVE"));
    let published = variable("TETRAL_TEST_ARCHIVE_SHA256");
    let bytes = fs::read(&archive).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().canonicalize().unwrap();

    let name = archive.file_name().unwrap().to_str().unwrap();
    fs::write(base.join(name), &bytes).unwrap();
    assert!(
        digest_check(&base, name, &published),
        "{name} against {published}"
    );
    let mut flipped = bytes.clone();
    let middle = flipped.len() / 2;
    flipped[middle] ^= 1;
    fs::write(base.join(name), &flipped).unwrap();
    assert!(!digest_check(&base, name, &published));

    let listed = entries(&bytes);
    let expected: &[(&str, u32)] = if cfg!(target_os = "macos") {
        &[("tetral", 0o755)]
    } else {
        &[
            ("COPYING", 0o644),
            ("COPYING.libcap", 0o644),
            ("COPYING.openssl", 0o644),
            ("bwrap", 0o755),
            ("tetral", 0o755),
        ]
    };
    let expected: Vec<(String, u32)> = expected
        .iter()
        .map(|(path, mode)| (path.to_string(), *mode))
        .collect();
    assert_eq!(listed, expected);

    let install = base.join("install");
    fs::create_dir(&install).unwrap();
    let mut unpacking = tar::Archive::new(flate2::read::GzDecoder::new(&bytes[..]));
    unpacking.set_preserve_permissions(true);
    unpacking.unpack(&install).unwrap();
    let program = install.join("tetral");

    let engine = one_credential_engine();
    let root = base.join("root");
    let mut configure = tetral(
        &program,
        &root,
        &["configure", "--app", APP, "--from-stdin"],
    )
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    configure
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY)).unwrap())
        .unwrap();
    let configured = configure.wait_with_output().unwrap();
    assert!(configured.status.success(), "{configured:?}");

    let socket =
        state::paths::socket_path(&state::paths::StateRoot::new(root.as_os_str()).unwrap())
            .unwrap()
            .expect("the configured root exists");
    // Killed and reaped when dropped, so that a failure before the shutdown
    // leaves no App Server running.
    let mut server = Tetral::at(&program, ["server", "run", "--app", APP])
        .home(&root)
        .env("PATH", "")
        .spawn();
    let mut status = Value::Null;
    wait_until("the release server online", || {
        status = request(&socket, "GET", "/v1/status")
            .filter(|response| response.status == 200)
            .map(|response| serde_json::from_slice(&response.body).unwrap())
            .unwrap_or(Value::Null);
        status["connection_state"] == "online"
    });
    assert_eq!(
        json!([status["server_state"], status["cli_version"]]),
        json!(["running", env!("CARGO_PKG_VERSION")]),
        "{status}"
    );

    #[cfg(target_os = "linux")]
    let command = {
        let project = base.join("project");
        fs::create_dir(&project).unwrap();
        command_under_the_archives_bwrap(&engine, server.id(), &install, &project)
    };
    #[cfg(not(target_os = "linux"))]
    let command = String::new();

    assert_eq!(
        request(&socket, "POST", "/v1/shutdown").map(|response| response.status),
        Some(202)
    );
    let mut exit = None;
    wait_until("the release server's exit", || {
        exit = server.try_wait().unwrap();
        exit.is_some()
    });
    assert_eq!(exit.unwrap().code(), Some(0));
    case.observe(format!(
        "{name}: entries {}; the digest check passed, and failed with byte {middle} flipped; server run reached online with server_state running and cli_version {}{command}; the shutdown answered 202 and server run exited 0",
        listed
            .iter()
            .map(|(path, mode)| format!("{path} {mode:04o}"))
            .collect::<Vec<_>>()
            .join(", "),
        status["cli_version"]
    ));
    case.pass();
}
