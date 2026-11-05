//! The release archive of the machine's target as `cli/scripts/package.sh`
//! writes it: its entries, its third-party notices, its digest, its static
//! Linux executables, and its `tetral` serving against the reference control
//! server. The test is ignored by default. Run it in
//! `cli/` with `TETRAL_TEST_ARCHIVE` naming the archive and
//! `TETRAL_TEST_ARCHIVE_SHA256` the digest the script printed:
//! `cargo test --test release_archive -- --ignored`. The archive's `tetral`
//! is a release build, so it uses the machine's own socket directory.

use std::collections::BTreeSet;
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

/// A licence expression with its spacing collapsed and the `/` of older
/// manifests read as `OR`, as cargo-about writes it.
fn expression(licence: &str) -> String {
    licence
        .replace('/', " OR ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// `<name> <version>: <licence>` of each package that `cargo tree` lists as
/// built into `tetral` for `target`, other than the workspace's own.
fn third_party_packages(target: &str) -> BTreeSet<String> {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/../Cargo.toml");
    let cargo = |arguments: &[&str]| {
        let output = Command::new(env!("CARGO"))
            .args(arguments)
            .args(["--locked", "--manifest-path", manifest])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap()
    };
    let metadata: Value =
        serde_json::from_str(&cargo(&["metadata", "--format-version", "1", "--no-deps"])).unwrap();
    let workspace: BTreeSet<&str> = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|package| package["name"].as_str().unwrap())
        .collect();
    let tree = cargo(&[
        "tree",
        "-p",
        "tetral",
        "--target",
        target,
        "-e",
        "normal,build",
        "--prefix",
        "none",
        "--format",
        "{l}\t{p}",
    ]);
    tree.lines()
        .filter_map(|line| {
            let (licence, package) = line.split_once('\t').unwrap();
            let mut words = package.split(' ');
            let name = words.next().unwrap();
            let version = words.next().unwrap().strip_prefix('v').unwrap();
            (!workspace.contains(name))
                .then(|| format!("{name} {version}: {}", expression(licence)))
        })
        .collect()
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

/// `PT_INTERP`, the program header that names a dynamic loader.
#[cfg(target_os = "linux")]
const PT_INTERP: u32 = 3;
/// `PT_DYNAMIC`, the program header of the dynamic section.
#[cfg(target_os = "linux")]
const PT_DYNAMIC: u32 = 2;
/// `DT_NEEDED`, the dynamic entry that names a shared library.
#[cfg(target_os = "linux")]
const DT_NEEDED: u64 = 1;

/// Whether the little-endian ELF64 file `binary` has a `PT_INTERP` program
/// header, and how many `DT_NEEDED` entries its dynamic section holds.
#[cfg(target_os = "linux")]
fn loader_and_libraries(binary: &[u8]) -> (bool, usize) {
    assert_eq!(
        binary[..6],
        [0x7f, b'E', b'L', b'F', 2, 1],
        "an ELF64 LSB file"
    );
    let u16_at = |at: usize| usize::from(u16::from_le_bytes([binary[at], binary[at + 1]]));
    let u32_at = |at: usize| u32::from_le_bytes(binary[at..at + 4].try_into().unwrap());
    let u64_at = |at: usize| u64::from_le_bytes(binary[at..at + 8].try_into().unwrap());
    let (table, size, count) = (u64_at(0x20) as usize, u16_at(0x36), u16_at(0x38));
    let mut loader = false;
    let mut libraries = 0;
    for header in (0..count).map(|index| table + index * size) {
        match u32_at(header) {
            PT_INTERP => loader = true,
            PT_DYNAMIC => {
                let offset = u64_at(header + 8) as usize;
                let length = u64_at(header + 32) as usize;
                libraries += (offset..offset + length)
                    .step_by(16)
                    .map(u64_at)
                    .take_while(|tag| *tag != 0)
                    .filter(|tag| *tag == DT_NEEDED)
                    .count();
            }
            _ => {}
        }
    }
    (loader, libraries)
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

/// On Linux, with one byte of the archive's `bwrap` changed, an
/// `exec_command` of the prepared Session is rejected `sandbox_unavailable`.
#[cfg(target_os = "linux")]
fn command_with_a_changed_bwrap(
    engine: &tetral_tests::reference::ReferenceServer,
    install: &Path,
    project: &Path,
) -> String {
    let bwrap = install.join("bwrap");
    let mut changed = fs::read(&bwrap).unwrap();
    let middle = changed.len() / 2;
    changed[middle] ^= 1;
    fs::write(&bwrap, &changed).unwrap();
    let issued = engine.control(
        "execute",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": "sess_release", "binding_id": "bind_release",
            "task_id": "task_changed_bwrap", "tool_use_event_id": "call_changed_bwrap",
            "tool": "exec_command", "input": {"cmd": "printf served"},
            "sandbox_policy": tetral_tests::tasks::policy(project, "disabled"),
        }}),
    );
    let report = reported(engine, "tool-status", &issued["message_id"]);
    assert_eq!(
        json!([report["phase"], report["error"]["code"]]),
        json!(["rejected", "sandbox_unavailable"]),
        "{report}"
    );
    format!(
        "; with byte {middle} of bwrap changed, an exec_command was rejected sandbox_unavailable"
    )
}

#[test]
#[ignore = "needs the archive named by TETRAL_TEST_ARCHIVE"]
fn the_archive_holds_its_files_matches_its_digest_and_serves() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "the release archive of this machine's target: its entries, its third-party notices against cargo tree, the digest check with one byte flipped, its static Linux executables, server run, and on Linux a changed bwrap",
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
        &[("THIRD-PARTY-NOTICES", 0o644), ("tetral", 0o755)]
    } else {
        &[
            ("COPYING", 0o644),
            ("COPYING.libcap", 0o644),
            ("COPYING.openssl", 0o644),
            ("THIRD-PARTY-NOTICES", 0o644),
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

    let target = name
        .strip_prefix("tetral-")
        .and_then(|rest| rest.strip_suffix(".tar.gz"))
        .unwrap();
    let notices = fs::read_to_string(install.join("THIRD-PARTY-NOTICES")).unwrap();
    let listed_packages: BTreeSet<String> = notices
        .lines()
        .take_while(|line| !line.starts_with("===="))
        .filter_map(|line| {
            let (package, licence) = line.split_once(": ")?;
            Some(format!("{package}: {}", expression(licence)))
        })
        .collect();
    let built = third_party_packages(target);
    let unnamed: Vec<&String> = built.difference(&listed_packages).collect();
    assert!(unnamed.is_empty(), "THIRD-PARTY-NOTICES lacks {unnamed:?}");

    #[cfg(target_os = "linux")]
    {
        let linked: Vec<(&str, (bool, usize))> = ["tetral", "bwrap"]
            .into_iter()
            .map(|name| {
                (
                    name,
                    loader_and_libraries(&fs::read(install.join(name)).unwrap()),
                )
            })
            .collect();
        assert_eq!(linked, [("tetral", (false, 0)), ("bwrap", (false, 0))]);
    }

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
        let ran = command_under_the_archives_bwrap(&engine, server.id(), &install, &project);
        let refused = command_with_a_changed_bwrap(&engine, &install, &project);
        format!("; tetral and bwrap have no PT_INTERP and no DT_NEEDED{ran}{refused}")
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
        "{name}: entries {}; THIRD-PARTY-NOTICES names all {} packages that cargo tree lists as built into tetral for {target}, other than the workspace's own; the digest check passed, and failed with byte {middle} flipped; server run reached online with server_state running and cli_version {}{command}; the shutdown answered 202 and server run exited 0",
        listed
            .iter()
            .map(|(path, mode)| format!("{path} {mode:04o}"))
            .collect::<Vec<_>>()
            .join(", "),
        built.len(),
        status["cli_version"]
    ));
    case.pass();
}
