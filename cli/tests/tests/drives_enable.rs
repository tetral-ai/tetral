//! `tetral drives enable`: it fails with `server_running` while an App
//! Server owns the root. On Linux it otherwise succeeds with `{}` and
//! changes nothing. On a Mac it mounts its test drive at
//! `run/drives-enable` and reads its one file: an allowed read records
//! `drives_enabled = true`; a denied read, a signal while the read waits
//! and a `mount` that fails each record nothing; no run leaves the mount.
//! The failing `mount`'s output reaches neither the command's stdout nor
//! its stderr.

use serde_json::json;
use tetral_tests::machine::{CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine};
use tetral_tests::run::Tetral;
use tetral_tests::socket_directory;

const APP: &str = "app_one";

#[cfg(target_os = "linux")]
#[test]
fn drives_enable_changes_nothing_and_needs_the_root_free() {
    use tetral_tests::snapshot::snapshot;

    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("drives enable while an App Server owns the root");
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    let root = machine.root.clone();
    let run = Tetral::new(["drives", "enable", "--app", APP])
        .home(&root)
        .run();
    case.command("tetral drives enable --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(1), "{}", run.exit());
    assert_eq!(run.error_code(), "server_running");
    case.observe("server_running with exit 1");
    case.pass();

    let mut case = record.case("drives enable on a root no App Server owns");
    machine.app_server.kill();
    let before = snapshot(&root);
    let run = Tetral::new(["drives", "enable", "--app", APP])
        .home(&root)
        .run();
    case.command("tetral drives enable --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), json!({}));
    assert_eq!(snapshot(&root), before, "the root is unchanged");
    case.observe("{} with exit 0; the root is unchanged");
    case.pass();
}

#[cfg(target_os = "macos")]
#[test]
fn drives_enable_records_consent_only_when_the_test_read_succeeds() {
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    const CHECK_READ: &str = "TETRAL_TEST_CHECK_READ";
    const MOUNT: &str = "TETRAL_TEST_MOUNT";

    fn enable(root: &Path) -> Tetral {
        Tetral::new(["drives", "enable", "--app", APP]).home(root)
    }

    fn config(root: &Path) -> String {
        std::fs::read_to_string(root.join("config.toml")).unwrap()
    }

    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("drives enable while an App Server owns the root");
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    let root = machine.root.clone();
    let point: PathBuf = root
        .canonicalize()
        .unwrap()
        .join("run")
        .join("drives-enable");
    let mounted = |point: &Path| drive::mount_macos::listed(point).unwrap();
    let run = enable(&root).run();
    case.command("tetral drives enable --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(1), "{}", run.exit());
    assert_eq!(run.error_code(), "server_running");
    assert_eq!(mounted(&point), None);
    case.observe("server_running with exit 1; nothing mounted");
    case.pass();
    machine.app_server.kill();
    let unchanged = config(&root);
    assert!(!unchanged.contains("drives_enabled = true"), "{unchanged}");

    let mut case = record.case("drives enable whose test read macOS denies");
    let run = enable(&root).env(CHECK_READ, "denied").run();
    case.command(
        "TETRAL_TEST_CHECK_READ=denied tetral drives enable --app app_one",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(1), "{}", run.exit());
    assert_eq!(run.error_code(), "drives_access_denied");
    assert_eq!(config(&root), unchanged);
    assert_eq!(mounted(&point), None);
    case.observe("drives_access_denied with exit 1; config.toml unchanged; no mount left");
    case.pass();

    let mut case =
        record.case("drives enable whose mount writes a line to stdout and to stderr and exits 32");
    let script = directory.path().join("mount-exits-32");
    std::fs::write(
        &script,
        "#!/bin/sh\necho stdout of mount\necho stderr of mount >&2\nexit 32\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let run = enable(&root).env(MOUNT, &script).run();
    case.command(
        "TETRAL_TEST_MOUNT=<a script that exits 32> tetral drives enable --app app_one",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(1), "{}", run.exit());
    assert_eq!(run.error_code(), "drives_mount_failed");
    assert_eq!(run.message(), "mount: exit status 32");
    let free_text: Vec<&str> = std::str::from_utf8(&run.stderr)
        .unwrap()
        .lines()
        .filter(|line| serde_json::from_str::<serde_json::Value>(line).is_err())
        .collect();
    assert!(free_text.is_empty(), "{free_text:?}");
    assert_eq!(config(&root), unchanged);
    assert_eq!(mounted(&point), None);
    case.observe("drives_mount_failed `mount: exit status 32` with exit 1, stdout holding only that failure and stderr only JSON records; config.toml unchanged; the command, which served the test drive, has exited");
    case.pass();

    let mut case = record.case("drives enable whose test read macOS allows");
    let run = enable(&root).run();
    case.command("tetral drives enable --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), json!({}));
    let enabled = config(&root);
    assert!(enabled.contains("drives_enabled = true"), "{enabled}");
    assert_eq!(mounted(&point), None);
    case.observe("{} with exit 0; config.toml records drives_enabled = true; no mount left");
    case.pass();
}

#[cfg(target_os = "macos")]
fn held_read_cancellation(name: &str, signal: rustix::process::Signal) {
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};
    use tetral_tests::run::finish;
    const CHECK_READ: &str = "TETRAL_TEST_CHECK_READ";
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let mut setup = record.case("bootstrap a root for held-read cancellation");
    let machine = Machine::start(
        &mut setup,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    let root = machine.root.clone();
    machine.app_server.kill();
    setup.pass();
    let unchanged = std::fs::read_to_string(root.join("config.toml")).unwrap();
    let point = root.canonicalize().unwrap().join("run/drives-enable");
    let mounted = |point: &Path| drive::mount_macos::listed(point).unwrap();
    let enable = |root: &Path| Tetral::new(["drives", "enable", "--app", APP]).home(root);
    let config = |root: &Path| std::fs::read_to_string(root.join("config.toml")).unwrap();
    // The command owns its NFS listener. Reaping this child also closes
    // that listener; always detach its exact test mount, even on a failure.
    struct HeldRead {
        child: Option<tetral_tests::run::RunningChild>,
        point: PathBuf,
    }
    impl Drop for HeldRead {
        fn drop(&mut self) {
            if drive::mount_macos::listed(&self.point)
                .ok()
                .flatten()
                .is_some()
            {
                eprintln!("held-read cleanup: force-detach {}", self.point.display());
                let _ = std::process::Command::new("/sbin/umount")
                    .arg("-f")
                    .arg(&self.point)
                    .status();
            }
            if let Some(child) = &mut self.child {
                eprintln!(
                    "held-read cleanup: terminate/reap owned command {}",
                    child.id()
                );
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let mut case = record.case(&format!(
        "drives enable receiving {name} while its test read waits"
    ));
    let entered = directory.path().join(format!("read-entered-{name}"));
    let mut held = HeldRead {
        child: Some(
            enable(&root)
                .env(CHECK_READ, "never")
                // The drive test server writes this file on entering its held READ.
                .env("TETRAL_TEST_CHECK_READ_ENTERED", &entered)
                .spawn(),
        ),
        point: point.clone(),
    };
    let child = held.child.as_mut().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !entered.exists() {
        assert!(Instant::now() < deadline, "the test read entered");
        assert!(child.try_wait().unwrap().is_none(), "the command waits");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(mounted(&point).is_some());
    let pid = rustix::process::Pid::from_raw(child.id() as i32).unwrap();
    rustix::process::kill_process(pid, signal).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "{name} ends the held command");
        std::thread::sleep(Duration::from_millis(20));
    }
    let run = finish(held.child.take().unwrap());
    case.command(
        &format!("TETRAL_TEST_CHECK_READ=never tetral drives enable --app app_one, then {name}"),
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(1), "{name}: {}", run.exit());
    assert_eq!(run.error_code(), "cancelled", "{name}");
    assert_eq!(config(&root), unchanged);
    assert_eq!(mounted(&point), None);
    drop(held);
    case.observe(format!("{name}: read-entered observed; cancelled with exit 1; config.toml unchanged; no test mount; command and its owned NFS listener reaped"));
    case.pass();
}

#[cfg(target_os = "macos")]
#[test]
fn drives_enable_cancels_sigint_while_its_test_read_waits() {
    held_read_cancellation("SIGINT", rustix::process::Signal::INT);
}

#[cfg(target_os = "macos")]
#[test]
fn drives_enable_cancels_sigterm_while_its_test_read_waits() {
    held_read_cancellation("SIGTERM", rustix::process::Signal::TERM);
}
