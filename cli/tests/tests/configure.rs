//! `tetral configure` and `tetral --version` as separate processes: the
//! argument, input and root checks, the private modes of what configure
//! creates, every failure code it writes, a failed command changing nothing in
//! the root, configure over what another run left in the root, and no key
//! reaching its output or its argument list. Against the reference control
//! server: the credential a `server run` authenticates with after an
//! interrupted configure, configure beside an online App Server, a broken
//! configuration that never reaches Engine, and no key in the output, log
//! file or argument list of a `server run` that connects.

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::{AppServer, wait_until};
use tetral_tests::other_user;
use tetral_tests::record::Case;
use tetral_tests::reference::ReferenceServer;
use tetral_tests::run::{self, Run, Tetral};
use tetral_tests::seams::Seams;
use tetral_tests::snapshot::snapshot;
use tetral_tests::socket_directory;

const APP: &str = "app_one";

fn bootstrap(key: &str) -> Value {
    json!({
        "server_url": "https://engine.example.com",
        "instance_id": "inst_one",
        "credential_id": "ccred_one",
        "connection_key": key,
        "expires_at": "2027-01-01T00:00:00Z",
    })
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn configure(root: &Path, stdin: impl Into<Vec<u8>>) -> Tetral {
    Tetral::new(["configure", "--app", APP, "--from-stdin"])
        .home(root)
        .stdin(stdin)
}

/// A root that a first `configure` installed.
fn configured_root(case: &mut Case, directory: &Path) -> PathBuf {
    let root = directory.join("root");
    let run = configure(&root, bytes(&bootstrap("tetral_ck_first"))).run();
    case.command(
        "tetral configure --app app_one --from-stdin (first bootstrap)",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), json!({}), "the first configure succeeds");
    root
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
}

/// Runs a command that must fail with `code` and leave `root` unchanged.
fn fails_and_changes_nothing(
    case: &mut Case,
    root: &Path,
    command: &str,
    tetral: Tetral,
    code: &str,
) -> Run {
    let before = snapshot(root);
    let run = tetral.run();
    case.command(command, &run.exit());
    assert_eq!(run.error_code(), code, "{command}: {}", run.message());
    assert_eq!(snapshot(root), before, "{command} changed the root");
    case.observe(format!("{code}: {}; the root is unchanged", run.message()));
    run
}

#[test]
fn unsupported_root_encoding_is_refused_before_configuration_writes() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let record = tetral_tests::record!();
    let mut case = record.case("given and resolved state roots must be UTF-8 before command work");
    let directory = tempfile::tempdir().unwrap();
    let target = directory
        .path()
        .join(OsString::from_vec(b"root-\xff".to_vec()));
    fs::create_dir(&target).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
    let alias = directory.path().join("alias");
    symlink(&target, &alias).unwrap();
    let missing = alias.join("missing/root");
    let configured = configure(&missing, bytes(&bootstrap("tetral_ck_unused"))).run();
    assert_eq!(configured.error_code(), "unsafe_control_path");
    assert_eq!(configured.message(), "the state root path is not UTF-8");
    assert!(!target.join("missing").exists());
    let status = Tetral::new(["server", "status", "--app", APP])
        .home(&target)
        .run();
    assert_eq!(status.error_code(), "unsafe_control_path");
    assert_eq!(status.message(), configured.message());
    assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
    let version = Tetral::new(["--version"]).home(&target).run();
    assert!(version.status.success());
    case.observe("configure refused the resolved non-UTF-8 root before creating its missing prefix; server status refused the given root; --version still succeeded without root access");
    case.pass();
}

#[test]
fn refused_arguments_input_and_root_paths_fail_and_change_nothing() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();

    let mut case = record.case("arguments are refused with invalid_arguments");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let valid = bytes(&bootstrap("tetral_ck_second"));
    let rows: [(&str, Tetral); 4] = [
        (
            "configure with no TETRAL_HOME",
            Tetral::new(["configure", "--app", APP, "--from-stdin"]).stdin(valid.clone()),
        ),
        (
            "configure with TETRAL_HOME=root, a relative path",
            Tetral::new(["configure", "--app", APP, "--from-stdin"])
                .env("TETRAL_HOME", "root")
                .stdin(valid.clone()),
        ),
        (
            "configure with no --app",
            Tetral::new(["configure", "--from-stdin"])
                .home(&root)
                .stdin(valid.clone()),
        ),
        (
            "configure with TETRAL_LOG_LEVEL=verbose",
            configure(&root, valid.clone()).env("TETRAL_LOG_LEVEL", "verbose"),
        ),
    ];
    for (command, tetral) in rows {
        fails_and_changes_nothing(&mut case, &root, command, tetral, "invalid_arguments");
    }
    case.pass();

    let mut case = record.case("a refused bootstrap creates no missing root");
    let missing = directory.path().join("missing");
    let run = configure(&missing, b"{\"server_url\": \"https://engine".to_vec()).run();
    case.command("configure on a missing root < invalid JSON", &run.exit());
    assert_eq!(run.error_code(), "invalid_bootstrap");
    assert!(!missing.exists(), "a refused configure created the root");
    case.observe("invalid_bootstrap; the root was not created");
    case.pass();

    let mut case = record.case("bootstraps that break a rule are refused with invalid_bootstrap");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let with = |member: &str, value: Value| {
        let mut bootstrap = bootstrap("tetral_ck_second");
        bootstrap[member] = value;
        bytes(&bootstrap)
    };
    // A valid bootstrap of 65,536 bytes and one space, which a reader that
    // stops at the limit would accept.
    let mut padded = bootstrap("tetral_ck_second");
    padded["padding"] = json!("");
    let fill = 65_536 - bytes(&padded).len();
    padded["padding"] = json!("x".repeat(fill));
    let mut oversized = bytes(&padded);
    oversized.push(b' ');
    let mut rows: Vec<(String, Vec<u8>)> = vec![
        (
            "invalid JSON".into(),
            b"{\"server_url\": \"https://engine".to_vec(),
        ),
        (
            "65,537 bytes: a valid bootstrap of 65,536 bytes and a space".into(),
            oversized,
        ),
        ("expires_at null".into(), with("expires_at", Value::Null)),
        (
            "expires_at 2026-02-30T00:00:00Z".into(),
            with("expires_at", json!("2026-02-30T00:00:00Z")),
        ),
        (
            "http:// to a host that is not loopback".into(),
            with("server_url", json!("http://192.0.2.1:8080")),
        ),
        (
            "an empty instance_id".into(),
            with("instance_id", json!("")),
        ),
    ];
    for member in [
        "server_url",
        "instance_id",
        "credential_id",
        "connection_key",
        "expires_at",
    ] {
        let mut missing = bootstrap("tetral_ck_second");
        missing.as_object_mut().unwrap().remove(member);
        rows.push((format!("{member} missing"), bytes(&missing)));
    }
    let mut invalid_utf8 = bytes(&bootstrap("tetral_ck_second"));
    invalid_utf8.push(0xff);
    rows.push(("invalid UTF-8".into(), invalid_utf8));
    let mut trailing_value = bytes(&bootstrap("tetral_ck_second"));
    trailing_value.extend_from_slice(b" {}");
    rows.push(("a second JSON value".into(), trailing_value));
    assert_eq!(rows[1].1.len(), 65_537);
    for (input, stdin) in rows {
        let command =
            format!("tetral configure --app app_one --from-stdin < bootstrap with {input}");
        fails_and_changes_nothing(
            &mut case,
            &root,
            &command,
            configure(&root, stdin),
            "invalid_bootstrap",
        );
    }
    case.pass();

    let mut case = record.case("a root recorded for another app is refused with app_mismatch");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let other_app = Tetral::new(["configure", "--app", "app_two", "--from-stdin"])
        .home(&root)
        .stdin(valid.clone());
    fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral configure --app app_two --from-stdin",
        other_app,
        "app_mismatch",
    );
    case.pass();

    let mut case =
        record.case("unsafe control paths in the root are refused with unsafe_control_path");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    let run = fails_and_changes_nothing(
        &mut case,
        &root,
        "configure on a root of mode 0755",
        configure(&root, valid.clone()),
        "unsafe_control_path",
    );
    assert!(run.message().contains(root.to_str().unwrap()));
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();

    let elsewhere = directory.path().join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    fs::set_permissions(&elsewhere, fs::Permissions::from_mode(0o700)).unwrap();
    fs::rename(root.join("run"), elsewhere.join("run")).unwrap();
    symlink(elsewhere.join("run"), root.join("run")).unwrap();
    let run = fails_and_changes_nothing(
        &mut case,
        &root,
        "configure with run/ a symbolic link",
        configure(&root, valid.clone()),
        "unsafe_control_path",
    );
    assert!(run.message().contains(root.join("run").to_str().unwrap()));
    fs::remove_file(root.join("run")).unwrap();
    fs::rename(elsewhere.join("run"), root.join("run")).unwrap();

    fs::rename(
        root.join("credentials.json"),
        elsewhere.join("credentials.json"),
    )
    .unwrap();
    symlink(
        elsewhere.join("credentials.json"),
        root.join("credentials.json"),
    )
    .unwrap();
    let run = fails_and_changes_nothing(
        &mut case,
        &root,
        "configure with credentials.json a symbolic link",
        configure(&root, valid.clone()),
        "unsafe_control_path",
    );
    assert!(
        run.message()
            .contains(root.join("credentials.json").to_str().unwrap())
    );
    fs::remove_file(root.join("credentials.json")).unwrap();
    fs::rename(
        elsewhere.join("credentials.json"),
        root.join("credentials.json"),
    )
    .unwrap();

    let link = directory.path().join("link");
    symlink(&root, &link).unwrap();
    for (named, suffix) in [("as it is", ""), ("with /", "/"), ("with /.", "/.")] {
        let home = PathBuf::from(format!("{}{suffix}", link.display()));
        let run = fails_and_changes_nothing(
            &mut case,
            &root,
            &format!("configure with the root a symbolic link, named {named}"),
            configure(&home, valid.clone()),
            "unsafe_control_path",
        );
        assert!(run.message().contains(link.to_str().unwrap()));
        assert!(run.message().contains("symbolic link"));
    }
    case.pass();

    let mut case = record.case("a root owned by another user is refused with unsafe_control_path");
    match other_user::directory() {
        Ok(foreign) => {
            let run = fails_and_changes_nothing(
                &mut case,
                foreign,
                &format!("configure with TETRAL_HOME={}", foreign.display()),
                configure(foreign, valid.clone()),
                "unsafe_control_path",
            );
            assert!(run.message().contains("owned by another user"));
            case.pass();
        }
        Err(reason) => case.not_run(reason),
    }
}

#[test]
fn an_unsafe_socket_directory_is_refused_and_changes_nothing() {
    let mut socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let valid = bytes(&bootstrap("tetral_ck_second"));

    let mut case = record.case("a configured root");
    let root = configured_root(&mut case, directory.path());
    case.pass();
    for socket_mode in [0o755, 0o500] {
        let mut case = record.case(&format!(
            "a socket directory with mode {socket_mode:04o} is refused with unsafe_control_path"
        ));
        socket_dir.directory_with_mode(socket_mode);
        let run = fails_and_changes_nothing(
            &mut case,
            &root,
            &format!("configure with S of mode {socket_mode:04o}"),
            configure(&root, valid.clone()),
            "unsafe_control_path",
        );
        assert!(run.message().contains(socket_dir.path().to_str().unwrap()));
        assert_eq!(mode(&socket_dir.path()), socket_mode);
        fs::remove_dir(socket_dir.path()).unwrap();
        case.pass();
    }

    let mut case = record
        .case("a socket directory that is a symbolic link is refused with unsafe_control_path");
    let target = directory.path().join("socket-target");
    fs::create_dir(&target).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
    socket_dir.link_to(&target);
    let run = fails_and_changes_nothing(
        &mut case,
        &root,
        "configure with S a symbolic link to a directory of this user",
        configure(&root, valid.clone()),
        "unsafe_control_path",
    );
    assert!(run.message().contains("symbolic link"));
    fs::remove_file(socket_dir.path()).unwrap();
    case.pass();

    let mut case =
        record.case("a socket directory owned by another user is refused with unsafe_control_path");
    if case.runs_as_non_root("root owns every directory it makes") {
        match socket_dir.directory_owned_by_root() {
            Ok(()) => {
                let run = fails_and_changes_nothing(
                    &mut case,
                    &root,
                    "configure with S owned by root",
                    configure(&root, valid.clone()),
                    "unsafe_control_path",
                );
                assert!(run.message().contains("owned by another user"));
                case.pass();
            }
            Err(reason) => case.not_run(reason),
        }
    }
}

#[test]
fn version_needs_no_root_and_touches_nothing() {
    let record = tetral_tests::record!();
    let mut case = record.case("tetral --version writes the version and touches nothing");
    // The test package's version is the workspace's, the version of cli/Cargo.toml.
    let expected = format!("{{\"cli_version\":\"{}\"}}\n", env!("CARGO_PKG_VERSION"));
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing");
    let writes_only_the_version = |case: &mut Case, command: &str, tetral: Tetral| {
        let before = snapshot(directory.path());
        let run = tetral.run();
        case.command(command, &run.exit());
        assert_eq!(run.status.code(), Some(0), "{command}");
        assert_eq!(String::from_utf8_lossy(&run.stdout), expected, "{command}");
        assert!(run.stderr.is_empty(), "{command} wrote to stderr");
        assert_eq!(
            snapshot(directory.path()),
            before,
            "{command} changed something"
        );
        assert!(!missing.exists());
    };
    writes_only_the_version(
        &mut case,
        "tetral --version with no TETRAL_HOME",
        Tetral::new(["--version"]),
    );
    writes_only_the_version(
        &mut case,
        "tetral --version with TETRAL_HOME naming a missing directory",
        Tetral::new(["--version"]).home(&missing),
    );
    case.observe(format!(
        "stdout is exactly {expected:?}, stderr is empty, nothing is created"
    ));
    case.pass();

    let mut case = record.case("tetral --version with a root owned by another user");
    match other_user::directory() {
        Ok(foreign) => {
            writes_only_the_version(
                &mut case,
                &format!("tetral --version with TETRAL_HOME={}", foreign.display()),
                Tetral::new(["--version"]).home(foreign),
            );
            case.observe(format!(
                "stdout is exactly {expected:?}, stderr is empty, the root checks did not run"
            ));
            case.pass();
        }
        Err(reason) => case.not_run(reason),
    }

    let mut case = record.case("--help and --version with another argument are invalid_arguments");
    for arguments in [vec!["--help"], vec!["--version", "--app", "x"]] {
        let run = Tetral::new(&arguments).home(&missing).run();
        case.command(&format!("tetral {}", arguments.join(" ")), &run.exit());
        assert_eq!(run.error_code(), "invalid_arguments", "{arguments:?}");
    }
    assert!(!missing.exists());
    case.pass();
}

#[test]
fn an_oversized_bootstrap_is_refused_without_waiting_for_stdin_eof() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("an oversized bootstrap is refused while its stdin writer stays open");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let before = snapshot(&root);
    let mut padded = bootstrap("tetral_ck_second");
    padded["padding"] = json!("");
    let fill = 65_536 - bytes(&padded).len();
    padded["padding"] = json!("p".repeat(fill));
    let mut input = bytes(&padded);
    assert_eq!(input.len(), 65_536);
    assert!(serde_json::from_slice::<serde_json::Value>(&input).is_ok());
    input.push(b' ');
    let run = configure(&root, input).keep_stdin_open().run();
    case.command(
        "configure with 65,537 bytes and stdin kept open",
        &run.exit(),
    );
    assert_eq!(run.error_code(), "invalid_bootstrap");
    assert_eq!(snapshot(&root), before);
    case.observe("the valid 65,536-byte JSON followed by one space was rejected before EOF, leaving the installed configuration unchanged");
    case.pass();
}

#[test]
fn configure_installs_a_bootstrap_of_the_size_limit_with_private_modes() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("a bootstrap of exactly 65,536 bytes installs a new root with private modes");
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("new").join("root");
    let mut padded = bootstrap("tetral_ck_padded");
    padded["padding"] = json!("");
    let fill = 65_536 - bytes(&padded).len();
    padded["padding"] = json!("x".repeat(fill));
    let stdin = bytes(&padded);
    assert_eq!(stdin.len(), 65_536);
    let run = configure(&root, stdin).run();
    case.command(
        "tetral configure --app app_one --from-stdin < a 65,536-byte bootstrap",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(0));
    assert_eq!(run.stdout, b"{}\n");
    for (path, expected) in [
        (root.clone(), 0o700),
        (root.join("run"), 0o700),
        (root.join("config.toml"), 0o600),
        (root.join("credentials.json"), 0o600),
        (root.join("run/app-server.lock"), 0o600),
    ] {
        assert_eq!(mode(&path), expected, "{}", path.display());
    }
    let names: Vec<String> = fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    let mut names = names;
    names.sort();
    assert_eq!(names, ["config.toml", "credentials.json", "run"]);
    let credentials: Value =
        serde_json::from_slice(&fs::read(root.join("credentials.json")).unwrap()).unwrap();
    assert_eq!(credentials, bootstrap("tetral_ck_padded"));
    let config = fs::read_to_string(root.join("config.toml")).unwrap();
    assert_eq!(
        config,
        "app = \"app_one\"\nserver_url = \"https://engine.example.com\"\ninstance_id = \"inst_one\"\ndrives_enabled = false\n"
    );
    case.observe("stdout is {} and a newline; the root and run/ have mode 0700, config.toml, credentials.json and run/app-server.lock 0600; credentials.json holds the bootstrap without the unknown member");
    case.pass();
}

#[test]
fn each_failure_code_of_configure_leaves_the_root_unchanged() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let valid = bytes(&bootstrap("tetral_ck_second"));

    let mut case = record.case("configure while the root's lock is held fails with server_running");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let lock = fs::File::options()
        .write(true)
        .open(root.join("run/app-server.lock"))
        .unwrap();
    rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    fails_and_changes_nothing(
        &mut case,
        &root,
        "configure while the test holds run/app-server.lock",
        configure(&root, valid.clone()),
        "server_running",
    );
    drop(lock);
    case.pass();

    let mut case = record
        .case("configure with a config.toml that is not TOML fails with invalid_configuration");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    fs::write(root.join("config.toml"), "app = [not toml").unwrap();
    fails_and_changes_nothing(
        &mut case,
        &root,
        "configure with config.toml not TOML",
        configure(&root, valid.clone()),
        "invalid_configuration",
    );
    case.pass();

    let mut case = record.case("configure on a root whose run/ has mode 0500 fails with io_failed");
    if case.runs_as_non_root("root may write into a directory of mode 0500") {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fs::create_dir_all(root.join("run")).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(root.join("run"), fs::Permissions::from_mode(0o500)).unwrap();
        let run = fails_and_changes_nothing(
            &mut case,
            &root,
            "configure with run/ of mode 0500",
            configure(&root, valid.clone()),
            "io_failed",
        );
        assert!(
            run.message()
                .contains(root.join("run/app-server.lock").to_str().unwrap()),
            "{}",
            run.message()
        );
        fs::set_permissions(root.join("run"), fs::Permissions::from_mode(0o700)).unwrap();
        case.pass();
    }

    let mut case = record
        .case("configure under a directory of mode 0500 fails with io_failed naming the new root");
    if case.runs_as_non_root("root may write into a directory of mode 0500") {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("read-only");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();
        let root = parent.join("root");
        let run = fails_and_changes_nothing(
            &mut case,
            &parent,
            "configure with a new root under a directory of mode 0500",
            configure(&root, valid.clone()),
            "io_failed",
        );
        assert!(
            run.message().starts_with(&format!("{}: ", root.display())),
            "{}",
            run.message()
        );
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        case.pass();
    }
}

#[test]
fn configure_installs_over_a_root_that_another_run_left() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut replacement = bootstrap("tetral_ck_second");
    replacement["server_url"] = json!("https://other.example.com");
    replacement["instance_id"] = json!("inst_two");

    for (name, break_root) in [
        (
            "configure replaces a deleted credentials.json",
            (|root: &Path| fs::remove_file(root.join("credentials.json")).unwrap()) as fn(&Path),
        ),
        (
            "configure replaces a pair whose config.toml names another instance_id",
            |root: &Path| {
                let config = fs::read_to_string(root.join("config.toml")).unwrap();
                fs::write(
                    root.join("config.toml"),
                    config.replace("inst_one", "inst_other"),
                )
                .unwrap();
            },
        ),
        (
            "configure reads a config.toml with a member that this version does not know",
            |root: &Path| {
                let config = fs::read_to_string(root.join("config.toml")).unwrap();
                fs::write(
                    root.join("config.toml"),
                    format!("future_option = true\n{config}"),
                )
                .unwrap();
            },
        ),
        (
            "configure removes the temporary files of interrupted replacements",
            |root: &Path| {
                fs::write(root.join(".replace-a1b2c3"), "partial").unwrap();
                symlink("/nonexistent", root.join(".replace-d4e5f6")).unwrap();
            },
        ),
    ] {
        let mut case = record.case(name);
        let directory = tempfile::tempdir().unwrap();
        let root = configured_root(&mut case, directory.path());
        break_root(&root);
        let run = configure(&root, bytes(&replacement)).run();
        case.command(
            "tetral configure --app app_one --from-stdin < another bootstrap",
            &run.exit(),
        );
        assert_eq!(run.status.code(), Some(0), "{}", run.exit());
        assert_eq!(run.object(), json!({}));
        let credentials: Value =
            serde_json::from_slice(&fs::read(root.join("credentials.json")).unwrap()).unwrap();
        assert_eq!(credentials, replacement);
        let config = fs::read_to_string(root.join("config.toml")).unwrap();
        assert!(
            config.contains("server_url = \"https://other.example.com\""),
            "{config}"
        );
        assert!(config.contains("instance_id = \"inst_two\""), "{config}");
        let mut names: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["config.toml", "credentials.json", "run"]);
        case.observe("both files hold the new bootstrap's server_url and instance_id; the root holds only config.toml, credentials.json and run/");
        case.pass();
    }

    let mut case = record.case("configure keeps the local options of the existing config.toml");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let config = fs::read_to_string(root.join("config.toml")).unwrap();
    fs::write(
        root.join("config.toml"),
        format!(
            "{}\n[computer_use]\nenabled = true\ndriver = \"/opt/driver\"\n",
            config.replace("drives_enabled = false", "drives_enabled = true")
        ),
    )
    .unwrap();
    let run = configure(&root, bytes(&replacement)).run();
    case.command(
        "tetral configure --app app_one --from-stdin < another bootstrap",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), json!({}));
    let config = fs::read_to_string(root.join("config.toml")).unwrap();
    assert!(config.contains("drives_enabled = true"), "{config}");
    assert!(
        config.contains("[computer_use]\nenabled = true\ndriver = \"/opt/driver\""),
        "{config}"
    );
    assert!(config.contains("instance_id = \"inst_two\""), "{config}");
    case.observe("config.toml holds the new instance_id with drives_enabled = true and the [computer_use] table kept");
    case.pass();
}

#[test]
fn no_key_reaches_the_output_or_the_argument_list() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a canary key reaches neither stdout, stderr nor the argument list");
    let directory = tempfile::tempdir().unwrap();
    let seams = Seams::serve(directory.path(), Duration::from_secs(1));
    let canary = format!("tetral_ck_CANARY{}", std::process::id());
    let root = directory.path().join("root");
    seams.hold("configure.locked");
    let child = configure(&root, bytes(&bootstrap(&canary)))
        .env("TETRAL_LOG_LEVEL", "debug")
        .seams(&seams)
        .spawn();
    seams.wait_until_waiting("configure.locked", 1);
    let arguments = argument_list(child.id());
    assert!(!arguments.is_empty());
    assert!(!arguments.contains(&canary), "{arguments}");
    seams.release("configure.locked");
    let run = run::finish(child);
    case.command("TETRAL_LOG_LEVEL=debug tetral configure --app app_one --from-stdin < bootstrap with a canary key, held at configure.locked", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), json!({}));
    for (stream, bytes) in [("stdout", &run.stdout), ("stderr", &run.stderr)] {
        assert!(
            !String::from_utf8_lossy(bytes).contains(&canary),
            "{stream} holds the key"
        );
    }
    case.observe(format!(
        "argument list while held: {arguments:?}; stdout and stderr hold no canary"
    ));
    case.pass();
}

/// The argument list of a running process, as the operating system shows it.
fn argument_list(pid: u32) -> String {
    if cfg!(target_os = "linux") {
        let raw = fs::read(format!("/proc/{pid}/cmdline")).unwrap();
        String::from_utf8_lossy(&raw).replace('\0', " ")
    } else {
        let output = std::process::Command::new("ps")
            .args(["-ww", "-o", "args=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }
}

/// A reference control server that knows each `(instance, credential, key,
/// state)`.
fn engine(credentials: &[(&str, &str, &str, &str)]) -> ReferenceServer {
    let credentials: Vec<Value> = credentials
        .iter()
        .map(|(instance, credential, key, state)| {
            json!({
                "instance_id": instance,
                "credential_id": credential,
                "connection_key": key,
                "expires_at": "2099-01-01T00:00:00Z",
                "state": state,
            })
        })
        .collect();
    ReferenceServer::start(&json!({
        "credentials": credentials,
        "clock_start": "2030-01-01T00:00:00Z",
    }))
}

fn engine_bootstrap(
    engine: &ReferenceServer,
    instance: &str,
    credential: &str,
    key: &str,
) -> Value {
    json!({
        "server_url": engine.url,
        "instance_id": instance,
        "credential_id": credential,
        "connection_key": key,
        "expires_at": "2099-01-01T00:00:00Z",
    })
}

fn server_run(root: &Path, seams: &Seams) -> Tetral {
    Tetral::new(["server", "run", "--app", APP])
        .home(root)
        .seams(seams)
}

/// Waits until the App Server's connection is in `state` and gives the
/// status then.
fn wait_for_state(app_server: &AppServer, state: &str) -> Value {
    let mut seen = Value::Null;
    wait_until(&format!("connection_state {state}"), || {
        seen = app_server.status();
        seen["connection_state"] == state
    });
    seen
}

fn request_count(engine: &ReferenceServer) -> usize {
    engine.state()["requests"].as_array().map_or(0, Vec::len)
}

#[test]
fn server_run_after_an_interrupted_configure_authenticates_with_one_whole_credential() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let engine = engine(&[
        ("inst_one", "ccred_old", "tetral_ck_old", "active"),
        ("inst_two", "ccred_new", "tetral_ck_new", "active"),
    ]);
    let old = engine_bootstrap(&engine, "inst_one", "ccred_old", "tetral_ck_old");
    let new = engine_bootstrap(&engine, "inst_two", "ccred_new", "tetral_ck_new");
    for (barrier, authenticates) in [
        ("configure.locked", Some(("inst_one", "ccred_old"))),
        ("configure.credentials_replaced", None),
        ("configure.config_replaced", Some(("inst_two", "ccred_new"))),
    ] {
        let mut case = record.case(&format!("configure killed at {barrier}, then server run"));
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        let run = configure(&root, bytes(&old)).run();
        case.command(
            "tetral configure --app app_one --from-stdin (old bootstrap)",
            &run.exit(),
        );
        assert_eq!(run.status.code(), Some(0), "{}", run.exit());
        assert_eq!(run.object(), json!({}));
        let seams = Seams::serve(directory.path(), Duration::from_secs(1));
        seams.hold(barrier);
        let mut child = configure(&root, bytes(&new)).seams(&seams).spawn();
        seams.wait_until_waiting(barrier, 1);
        child.kill().unwrap();
        let _ = child.wait();
        case.command(
            &format!(
                "tetral configure --app app_one --from-stdin (new bootstrap), killed at {barrier}"
            ),
            "killed",
        );
        let before = engine.requests("events").len();
        let requests_before = request_count(&engine);
        match authenticates {
            None => {
                let run = server_run(&root, &seams).run();
                case.command("tetral server run --app app_one", &run.exit());
                assert_eq!(run.error_code(), "invalid_configuration");
                assert_eq!(request_count(&engine), requests_before);
                case.observe("server run exited with invalid_configuration before the reference server recorded any request");
            }
            Some((instance, credential)) => {
                let app_server = AppServer::start(server_run(&root, &seams), &root);
                case.command("tetral server run --app app_one", "running");
                wait_for_state(&app_server, "online");
                let events = engine.requests("events");
                assert_eq!(events.len(), before + 1);
                assert_eq!(events[before]["status"], 200);
                assert_eq!(events[before]["query"]["instance_id"], instance);
                assert_eq!(events[before]["query"]["credential_id"], credential);
                case.observe(format!(
                    "server run authenticated as {instance} with {credential} and is online"
                ));
            }
        }
        case.pass();
    }
}

#[test]
fn configure_beside_an_online_app_server_leaves_its_connection_alone() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("configure while an App Server is online");
    let engine = engine(&[("inst_one", "ccred_one", "tetral_ck_one", "active")]);
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root");
    let bootstrap = engine_bootstrap(&engine, "inst_one", "ccred_one", "tetral_ck_one");
    let run = configure(&root, bytes(&bootstrap)).run();
    case.command("tetral configure --app app_one --from-stdin", &run.exit());
    let seams = Seams::serve(directory.path(), Duration::from_secs(1));
    let app_server = AppServer::start(server_run(&root, &seams), &root);
    case.command("tetral server run --app app_one", "running");
    let online = wait_for_state(&app_server, "online");
    let events = engine.requests("events").len();

    let mut replacement = bootstrap.clone();
    replacement["credential_id"] = json!("ccred_other");
    replacement["connection_key"] = json!("tetral_ck_other");
    let run = configure(&root, bytes(&replacement)).run();
    case.command(
        "tetral configure --app app_one --from-stdin (another credential)",
        &run.exit(),
    );
    assert_eq!(run.error_code(), "server_running");
    seams.wait_for_more("connection.checked", 2);
    assert_eq!(engine.requests("events").len(), events);
    let state = engine.state();
    let requests = state["requests"].as_array().unwrap();
    assert!(
        requests.iter().all(|request| request["status"] != 401),
        "{requests:?}"
    );
    assert_eq!(app_server.status(), online);
    case.observe("configure failed with server_running; the reference server recorded no new events request and no 401, and the App Server is online on the same connection");
    case.pass();
}

#[test]
fn a_broken_configuration_never_reaches_engine() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let engine = engine(&[
        (
            "inst_deleted",
            "ccred_deleted",
            "tetral_ck_deleted",
            "active",
        ),
        (
            "inst_mismatch",
            "ccred_mismatch",
            "tetral_ck_mismatch",
            "active",
        ),
        (
            "inst_not_toml",
            "ccred_not_toml",
            "tetral_ck_not_toml",
            "active",
        ),
    ]);
    for (name, suffix, break_root, repairable) in [
        (
            "credentials.json deleted",
            "deleted",
            (|root: &Path| fs::remove_file(root.join("credentials.json")).unwrap()) as fn(&Path),
            true,
        ),
        (
            "config.toml naming another instance_id",
            "mismatch",
            |root: &Path| {
                let config = fs::read_to_string(root.join("config.toml")).unwrap();
                fs::write(
                    root.join("config.toml"),
                    config.replace("inst_mismatch", "inst_other"),
                )
                .unwrap();
            },
            true,
        ),
        (
            "config.toml that is not TOML",
            "not_toml",
            |root: &Path| fs::write(root.join("config.toml"), "app = [not toml").unwrap(),
            false,
        ),
    ] {
        let mut case = record.case(&format!("server run with {name}"));
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        let bootstrap = engine_bootstrap(
            &engine,
            &format!("inst_{suffix}"),
            &format!("ccred_{suffix}"),
            &format!("tetral_ck_{suffix}"),
        );
        let run = configure(&root, bytes(&bootstrap)).run();
        case.command("tetral configure --app app_one --from-stdin", &run.exit());
        break_root(&root);
        let before = request_count(&engine);
        let seams = Seams::serve(directory.path(), Duration::from_secs(1));
        let run = server_run(&root, &seams).run();
        case.command("tetral server run --app app_one", &run.exit());
        assert_eq!(run.error_code(), "invalid_configuration");
        assert_eq!(request_count(&engine), before);
        case.observe("invalid_configuration; the reference server recorded no request");
        if !repairable {
            case.pass();
            continue;
        }
        let run = configure(&root, bytes(&bootstrap)).run();
        case.command(
            "tetral configure --app app_one --from-stdin (again)",
            &run.exit(),
        );
        assert_eq!(run.status.code(), Some(0), "{}", run.exit());
        assert_eq!(run.object(), json!({}));
        let app_server = AppServer::start(server_run(&root, &seams), &root);
        case.command("tetral server run --app app_one", "running");
        wait_for_state(&app_server, "online");
        case.observe("after configure replaced the configuration, server run reached online");
        case.pass();
    }
}

#[test]
fn no_key_reaches_the_output_log_or_arguments_of_a_server_run_that_connects() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a server run that connects, gets a 401 and is shut down");
    let canary = format!("tetral_ck_CANARY_RUN{}", std::process::id());
    let engine = engine(&[("inst_one", "ccred_one", &canary, "revoked")]);
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root");
    let run = configure(
        &root,
        bytes(&engine_bootstrap(&engine, "inst_one", "ccred_one", &canary)),
    )
    .run();
    case.command(
        "tetral configure --app app_one --from-stdin < bootstrap with a canary key",
        &run.exit(),
    );
    let seams = Seams::serve(directory.path(), Duration::from_secs(1));
    let app_server = AppServer::start(
        server_run(&root, &seams).env("TETRAL_LOG_LEVEL", "debug"),
        &root,
    );
    case.command(
        "TETRAL_LOG_LEVEL=debug tetral server run --app app_one",
        "running",
    );
    let status = wait_for_state(&app_server, "blocked");
    assert_eq!(status["last_error"]["code"], "credential_revoked");
    assert!(!status.to_string().contains(&canary));
    let arguments = argument_list(app_server.pid());
    assert!(!arguments.is_empty());
    assert!(!arguments.contains(&canary), "{arguments}");
    let stop = Tetral::new(["server", "stop", "--app", APP])
        .home(&root)
        .run();
    case.command("tetral server stop --app app_one", &stop.exit());
    assert_eq!(stop.status.code(), Some(0), "{}", stop.exit());
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    let log = fs::read(root.join("logs/tetral.log")).unwrap();
    for (place, bytes) in [
        ("stdout", &run.stdout),
        ("stderr", &run.stderr),
        ("the log file", &log),
    ] {
        assert!(!bytes.is_empty(), "{place} is empty");
        assert!(
            !String::from_utf8_lossy(bytes).contains(&canary),
            "{place} holds the key"
        );
    }
    case.observe(format!(
        "blocked with credential_revoked; argument list {arguments:?}; status, stdout, stderr and the log file hold no canary"
    ));
    case.pass();
}
