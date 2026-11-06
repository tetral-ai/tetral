//! `tetral computer-use enable` and `disable` as separate processes, without
//! and with a running App Server: what each records in `config.toml`, failure
//! codes and their configuration effects, the wait behind
//! a held lock, `POST /v1/computer-use` refusing a malformed body or a
//! `config.toml` it cannot use, a change still waiting when the App Server
//! shuts down, enable and disable starting and stopping the test MCP
//! server while the App Server keeps its process and its connection, and the
//! driver's version that `GET /v1/status` shows.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ::mcp::CUA_DRIVER_TESTED_VERSION;
use serde_json::{Value, json};
use state::config::ComputerUse;
use state::paths::StateRoot;
use tetral_tests::app_server::{request, request_with_body, socket_path, wait_until};
use tetral_tests::machine::{APP, CREDENTIAL, INSTANCE, KEY, bootstrap, seconds};
use tetral_tests::mcp::{
    self, Setup, TestDriver, alive, disable, enable, group_alive, page, record_before_start,
    reports, tool, wait_for_reports,
};
use tetral_tests::record::Case;
use tetral_tests::run::{self, Run, Tetral};
use tetral_tests::seams::Seams;
use tetral_tests::snapshot::snapshot;
use tetral_tests::socket_directory;

const TEST_SERVER: &str = env!("CARGO_BIN_EXE_mcp-test-server");
/// A `--driver` for a root no App Server owns, where the command neither
/// checks nor starts it.
const DRIVER: &str = "/opt/cua/cua-driver";
const SEARCH_CHECKED: &str = "computer_use.search_checked";

/// A root that `configure` installed in `directory`.
fn configured_root(case: &mut Case, directory: &Path) -> PathBuf {
    let root = directory.join("root");
    let bootstrap = bootstrap("https://engine.example.com", INSTANCE, CREDENTIAL, KEY);
    let run = Tetral::new(["configure", "--app", APP, "--from-stdin"])
        .home(&root)
        .stdin(serde_json::to_vec(&bootstrap).unwrap())
        .run();
    case.command("tetral configure --app app_one --from-stdin", &run.exit());
    assert_eq!(run.object(), json!({}));
    root
}

/// `tetral computer-use <arguments> --app app_one` on `root`.
fn computer_use<S: AsRef<OsStr>>(root: &Path, arguments: &[S]) -> Tetral {
    let mut line: Vec<OsString> = vec!["computer-use".into()];
    line.extend(
        arguments
            .iter()
            .map(|argument| argument.as_ref().to_owned()),
    );
    line.extend(["--app".into(), APP.into()]);
    Tetral::new(line).home(root)
}

/// The `[computer_use]` that `config.toml` of `root` records.
fn recorded(root: &Path) -> Option<ComputerUse> {
    state::config::read(&StateRoot::new(root.as_os_str()).unwrap())
        .unwrap()
        .expect("config.toml exists")
        .computer_use
}

fn computer_use_record(enabled: bool, driver: &str) -> Option<ComputerUse> {
    Some(ComputerUse {
        enabled,
        driver: Some(PathBuf::from(driver)),
    })
}

/// Checks that `run` succeeded with `{}`.
fn succeeded(case: &mut Case, command: &str, run: &Run) {
    case.command(command, &run.exit());
    assert_eq!(run.status.code(), Some(0), "{command}: {}", run.exit());
    assert_eq!(run.object(), json!({}), "{command}");
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
    case.observe(format!(
        "{command}: {code}: {}; the root is unchanged",
        run.message()
    ));
    run
}

/// Each command's arguments on a valid call.
const COMMANDS: [(&str, &[&str]); 2] = [
    ("enable", &["enable", "--driver", DRIVER]),
    ("disable", &["disable"]),
];

/// Holds `run/app-server.lock` of `root` as a `configure` would.
fn hold_lock(root: &Path) -> fs::File {
    let lock = fs::File::options()
        .write(true)
        .open(root.join("run/app-server.lock"))
        .unwrap();
    rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    lock
}

#[test]
fn without_an_app_server_each_command_records_computer_use() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("enable and disable on a root no App Server owns");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());

    let run = computer_use(&root, &["enable", "--driver", DRIVER]).run();
    succeeded(
        &mut case,
        "tetral computer-use enable --driver /opt/cua/cua-driver",
        &run,
    );
    assert_eq!(recorded(&root), computer_use_record(true, DRIVER));
    case.observe("enable wrote {} and recorded enabled = true with the driver");

    let run = computer_use(&root, &["disable"]).run();
    succeeded(&mut case, "tetral computer-use disable", &run);
    assert_eq!(recorded(&root), computer_use_record(false, DRIVER));
    case.observe("disable wrote {} and recorded enabled = false, keeping the driver");
    case.pass();
}

#[test]
fn each_failure_code_of_both_commands_leaves_the_root_unchanged() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();

    let mut case = record.case("refused --driver arguments fail with invalid_arguments");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let not_utf8 = OsStr::from_bytes(b"/opt/cua/\xff");
    let refused: [(&str, Vec<&OsStr>); 4] = [
        (
            "enable with a relative --driver",
            vec!["enable".as_ref(), "--driver".as_ref(), "rel/x".as_ref()],
        ),
        ("enable without --driver", vec!["enable".as_ref()]),
        (
            "enable with a --driver not in UTF-8",
            vec!["enable".as_ref(), "--driver".as_ref(), not_utf8],
        ),
        (
            "disable with a --driver",
            vec!["disable".as_ref(), "--driver".as_ref(), DRIVER.as_ref()],
        ),
    ];
    for (command, arguments) in refused {
        fails_and_changes_nothing(
            &mut case,
            &root,
            command,
            computer_use(&root, &arguments),
            "invalid_arguments",
        );
    }
    case.pass();

    let mut case = record.case("another --app fails with app_mismatch");
    for (name, arguments) in COMMANDS {
        let mut line = vec!["computer-use"];
        line.extend(arguments);
        line.extend(["--app", "app_two"]);
        fails_and_changes_nothing(
            &mut case,
            &root,
            &format!("{name} --app app_two"),
            Tetral::new(line).home(&root),
            "app_mismatch",
        );
    }
    case.pass();

    let mut case = record.case("a root of mode 0755 fails with unsafe_control_path");
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    for (name, arguments) in COMMANDS {
        let run = fails_and_changes_nothing(
            &mut case,
            &root,
            &format!("{name} on a root of mode 0755"),
            computer_use(&root, arguments),
            "unsafe_control_path",
        );
        assert!(
            run.message().contains(root.to_str().unwrap()),
            "{}",
            run.message()
        );
    }
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    case.pass();

    let mut case = record.case("a root without config.toml fails with invalid_configuration");
    let unconfigured = tempfile::tempdir().unwrap();
    let bare = configured_root(&mut case, unconfigured.path());
    fs::remove_file(bare.join("config.toml")).unwrap();
    for (name, arguments) in COMMANDS {
        fails_and_changes_nothing(
            &mut case,
            &bare,
            &format!("{name} on a root without config.toml"),
            computer_use(&bare, arguments),
            "invalid_configuration",
        );
    }
    case.pass();

    let mut case = record.case("a root of mode 0500 fails with io_failed");
    if case.runs_as_non_root("root may write into a directory of mode 0500") {
        fs::set_permissions(&root, fs::Permissions::from_mode(0o500)).unwrap();
        for (name, arguments) in COMMANDS {
            let run = fails_and_changes_nothing(
                &mut case,
                &root,
                &format!("{name} on a root of mode 0500"),
                computer_use(&root, arguments),
                "io_failed",
            );
            assert!(
                run.message().contains(root.to_str().unwrap()),
                "{}",
                run.message()
            );
        }
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        case.pass();
    }

    let mut case = record.case(
        "a lock held with no App Server answering fails with server_running at 10 seconds, not at 9.95",
    );
    let lock = hold_lock(&root);
    for (name, arguments) in COMMANDS {
        let clock = directory.path().join(name);
        fs::create_dir(&clock).unwrap();
        let seams = Seams::serve(&clock, Duration::ZERO);
        let before = snapshot(&root);
        let mut child = computer_use(&root, arguments).seams(&seams).spawn();
        seams.wait_for_count(SEARCH_CHECKED, 1);
        seams.advance_within(Duration::from_millis(9_950), SEARCH_CHECKED);
        assert!(
            child.try_wait().unwrap().is_none(),
            "{name} ended before 10 seconds"
        );
        seams.advance(Duration::from_millis(50));
        let run = run::finish(child);
        case.command(
            &format!("{name} while the test holds run/app-server.lock"),
            &run.exit(),
        );
        assert_eq!(
            run.error_code(),
            "server_running",
            "{name}: {}",
            run.message()
        );
        assert_eq!(snapshot(&root), before, "{name} changed the root");
        case.observe(format!(
            "{name}: still waiting at 9.95 seconds; server_running at 10 seconds; the root is unchanged"
        ));
    }
    drop(lock);
    case.pass();
}

#[test]
fn a_lock_released_while_the_command_waits_lets_it_record() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("enable behind a lock the test releases at 3 seconds");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let seams = Seams::serve(directory.path(), Duration::ZERO);
    let lock = hold_lock(&root);
    let child = computer_use(&root, &["enable", "--driver", DRIVER])
        .seams(&seams)
        .spawn();
    seams.wait_for_count(SEARCH_CHECKED, 1);
    seams.advance_within(Duration::from_secs(3), SEARCH_CHECKED);
    assert_eq!(recorded(&root), None);
    drop(lock);
    seams.advance(Duration::from_millis(50));
    let run = run::finish(child);
    succeeded(
        &mut case,
        "tetral computer-use enable --driver /opt/cua/cua-driver, the lock released at 3 seconds",
        &run,
    );
    assert_eq!(recorded(&root), computer_use_record(true, DRIVER));
    case.observe("the command took the released lock, wrote {} and recorded enabled = true");
    case.pass();
}

/// A test server that lists the tool `a`.
fn listing_a() -> Value {
    json!({ "pages": [page(None, vec![tool("a")], None)] })
}

/// The process IDs of the test server's processes started since `seen`.
fn started_since(driver: &TestDriver, seen: usize) -> Vec<u32> {
    driver.wait_for_processes(seen + 1)[seen..].to_vec()
}

/// Waits until the report at `index`, counting from 0, has arrived and
/// checks that it is the disabled one.
fn wait_for_disabled_report(setup: &Setup, index: usize) {
    let report = wait_for_reports(&setup.engine, index + 1)[index].clone();
    assert_eq!(report["tools"], json!([]), "{report}");
    assert_eq!(report["server_info"], Value::Null, "{report}");
    assert_eq!(report["error"], Value::Null, "{report}");
}

#[test]
fn enable_and_disable_on_a_running_app_server_keep_its_process_and_connection() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("enable, a second enable and disable on a running App Server");
    let setup = mcp::online_with(&mut case, Path::new(TEST_SERVER), listing_a(), |_, _| {});
    let root = setup.machine.root.clone();
    let pid = setup.machine.app_server.pid();
    let connection_id = setup.machine.status()["connection_id"].clone();
    let program = setup.driver.driver().to_owned();

    let run = enable(&setup.machine, &program);
    succeeded(
        &mut case,
        "tetral computer-use enable --driver <test server>",
        &run,
    );
    assert_eq!(recorded(&root), computer_use_record(true, &program));
    let first = started_since(&setup.driver, 0)[0];
    wait_for_reports(&setup.engine, 1);
    case.observe("enable wrote {}, recorded enabled = true and started the test server");

    let run = enable(&setup.machine, &program);
    succeeded(
        &mut case,
        "tetral computer-use enable --driver <test server> (again)",
        &run,
    );
    let second = started_since(&setup.driver, 1)[0];
    assert!(!group_alive(first), "the first server's group remains");
    assert!(alive(second));
    wait_for_reports(&setup.engine, 2);
    case.observe("a second enable stopped the first server's process group and started a new one");

    let run = disable(&setup.machine);
    succeeded(&mut case, "tetral computer-use disable", &run);
    assert_eq!(recorded(&root), computer_use_record(false, &program));
    assert!(
        !group_alive(second),
        "the server's group remains after disable"
    );
    wait_for_disabled_report(&setup, 2);
    case.observe("disable wrote {}, recorded enabled = false keeping the driver, left no test server process and reported the disabled list");
    case.pass();

    let mut case = record.case("disable on a test server that ignores SIGTERM");
    let stubborn = TestDriver::ignoring_sigterm(
        &setup.directory.path().join("driver"),
        Path::new(TEST_SERVER),
        listing_a(),
    );
    let run = enable(&setup.machine, stubborn.driver());
    succeeded(
        &mut case,
        "tetral computer-use enable --driver <test server ignoring SIGTERM>",
        &run,
    );
    let third = started_since(&setup.driver, 2)[0];
    wait_for_reports(&setup.engine, 4);
    let checked = setup.machine.seams.count("mcp.stop_checked");
    let child = Tetral::new(["computer-use", "disable", "--app", APP])
        .home(&root)
        .seams(&setup.machine.seams)
        .spawn();
    setup
        .machine
        .seams
        .wait_for_count("mcp.stop_checked", checked + 1);
    setup
        .machine
        .seams
        .advance_within(seconds(1.999), "mcp.stop_checked");
    assert!(alive(third), "the server ended before its grace");
    setup.machine.seams.advance(seconds(0.001));
    let run = run::finish(child);
    succeeded(&mut case, "tetral computer-use disable", &run);
    assert!(
        !group_alive(third),
        "the server's group remains after SIGKILL"
    );
    wait_for_disabled_report(&setup, 4);
    case.observe("the server outlived SIGTERM at 1.999 seconds and was gone once disable answered at 2 seconds");
    case.pass();

    let mut case =
        record.case("enable whose server never answers initialize fails with driver_failed");
    TestDriver::new(
        &setup.directory.path().join("driver"),
        Path::new(TEST_SERVER),
        json!({ "initialize": "never" }),
    );
    let child = Tetral::new(["computer-use", "enable", "--app", APP, "--driver", &program])
        .home(&root)
        .seams(&setup.machine.seams)
        .spawn();
    let fourth = started_since(&setup.driver, 3)[0];
    setup.driver.wait_for_received(fourth, "initialize", 1);
    setup
        .machine
        .seams
        .advance_within(seconds(29.999), "mcp.start_checked");
    assert_eq!(reports(&setup.engine).len(), 5);
    setup.machine.seams.advance(seconds(0.001));
    let run = run::finish(child);
    case.command(
        "tetral computer-use enable --driver <test server never answering initialize>",
        &run.exit(),
    );
    assert_eq!(run.error_code(), "driver_failed");
    assert_eq!(run.message(), "no initialize result within 30 seconds");
    assert_eq!(recorded(&root), computer_use_record(true, &program));
    let report = wait_for_reports(&setup.engine, 6)[5].clone();
    assert_eq!(
        report["error"],
        json!({ "code": "mcp_start_failed", "message": "no initialize result within 30 seconds" })
    );
    case.observe("driver_failed at 30 seconds; enabled = true stays recorded and the tool list reports mcp_start_failed");

    assert_eq!(setup.machine.app_server.pid(), pid);
    assert_eq!(setup.machine.status()["connection_id"], connection_id);
    case.observe("the App Server kept its process ID and connection_id throughout");
    case.pass();
}

/// The records warning that the driver's version is not the tested one.
fn untested_warnings(root: &Path) -> Vec<Value> {
    tetral_tests::sessions::records(root, "mcp.driver_version_untested")
}

/// Waits until `count` such records exist and checks that the last names
/// only the tested version.
fn wait_for_unread_warning(root: &Path, count: usize) {
    wait_until("the unread version's warning", || {
        untested_warnings(root).len() >= count
    });
    let warnings = untested_warnings(root);
    assert_eq!(warnings.len(), count, "{warnings:#?}");
    let last = &warnings[count - 1];
    assert_eq!(last.get("cua_driver.version"), None, "{last}");
    assert_eq!(last["cua_driver.tested_version"], CUA_DRIVER_TESTED_VERSION);
}

/// [`listing_a`] whose `--version` follows `version`.
fn listing_a_with_version(version: Value) -> Value {
    let mut scenario = listing_a();
    scenario["version"] = version;
    scenario
}

/// The status member `cua_driver` of an enabled Computer Use.
fn cua_driver(version: Option<&str>) -> Value {
    json!({ "version": version, "tested_version": CUA_DRIVER_TESTED_VERSION })
}

#[test]
fn the_drivers_version_shows_in_status_and_an_untested_one_only_warns() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "server run with Computer Use recorded enabled and a test server answering --version with the tested version",
    );
    let tested = json!({ "line": format!("cua-driver {CUA_DRIVER_TESTED_VERSION}") });
    let setup = mcp::online_with(
        &mut case,
        Path::new(TEST_SERVER),
        listing_a_with_version(tested),
        |root, driver| record_before_start(root, &["enable", "--driver", driver.driver()]),
    );
    let root = setup.machine.root.clone();
    let program = setup.driver.driver().to_owned();
    let report = wait_for_reports(&setup.engine, 1).remove(0);
    assert_eq!(report["tools"][0]["name"], "a", "{report}");
    assert_eq!(
        setup.machine.status()["cua_driver"],
        cua_driver(Some(CUA_DRIVER_TESTED_VERSION))
    );
    case.observe(format!(
        "after cua's first tool-list report status cua_driver {{version: {CUA_DRIVER_TESTED_VERSION}, tested_version: {CUA_DRIVER_TESTED_VERSION}}}"
    ));
    case.pass();

    let mut case =
        record.case("enable with a test server answering --version with cua-driver 0.0.1");
    setup.driver.set_scenario(listing_a_with_version(
        json!({ "line": "cua-driver 0.0.1" }),
    ));
    let run = enable(&setup.machine, &program);
    succeeded(
        &mut case,
        "tetral computer-use enable --driver <test server answering cua-driver 0.0.1>",
        &run,
    );
    assert_eq!(
        setup.machine.status()["cua_driver"],
        cua_driver(Some("0.0.1"))
    );
    let report = wait_for_reports(&setup.engine, 2).remove(1);
    assert_eq!(report["error"], Value::Null, "{report}");
    assert_eq!(report["tools"][0]["name"], "a", "{report}");
    wait_until("the untested version's warning", || {
        !untested_warnings(&root).is_empty()
    });
    // The first record of the run: the tested version's start wrote none.
    let warnings = untested_warnings(&root);
    assert_eq!(warnings.len(), 1, "{warnings:#?}");
    assert_eq!(warnings[0]["level"], "WARN");
    assert_eq!(warnings[0]["cua_driver.version"], "0.0.1");
    assert_eq!(
        warnings[0]["cua_driver.tested_version"],
        CUA_DRIVER_TESTED_VERSION
    );
    case.observe(format!(
        "status cua_driver {{version: 0.0.1, tested_version: {CUA_DRIVER_TESTED_VERSION}}}; the run's first warn record, naming both; cua reported its tool a"
    ));
    case.pass();

    let rows = [
        ("nothing and exit status 1", listing_a()),
        (
            "cua-driver 0.0.1 and exit status 1",
            listing_a_with_version(json!({ "line": "cua-driver 0.0.1", "exit": 1 })),
        ),
    ];
    for (index, (answer, scenario)) in rows.into_iter().enumerate() {
        let mut case = record.case(&format!(
            "enable with a test server answering --version with {answer}"
        ));
        setup.driver.set_scenario(scenario);
        let run = enable(&setup.machine, &program);
        succeeded(
            &mut case,
            &format!("tetral computer-use enable --driver <test server answering {answer}>"),
            &run,
        );
        assert_eq!(setup.machine.status()["cua_driver"], cua_driver(None));
        wait_for_reports(&setup.engine, 3 + index);
        wait_for_unread_warning(&root, 2 + index);
        case.observe("cua started with status cua_driver {version: null, tested_version} and one more warn record, naming only the tested version");
        case.pass();
    }

    let mut case = record
        .case("enable with a test server whose --version never exits starts cua at 5 seconds");
    setup
        .driver
        .set_scenario(listing_a_with_version(json!("never")));
    let started = setup.driver.processes().len();
    let seams = &setup.machine.seams;
    let checked = seams.count("mcp.version_checked");
    let child = Tetral::new(["computer-use", "enable", "--app", APP, "--driver", &program])
        .home(&root)
        .seams(seams)
        .spawn();
    seams.wait_for_count("mcp.version_checked", checked + 1);
    let version_process = setup.driver.wait_for_version_pid();
    seams.advance_within(seconds(4.999), "mcp.version_checked");
    assert_eq!(
        setup.driver.processes().len(),
        started,
        "<driver> mcp started before the bound"
    );
    seams.advance(seconds(0.001));
    let run = run::finish(child);
    succeeded(
        &mut case,
        "tetral computer-use enable --driver <test server whose --version never exits>",
        &run,
    );
    wait_until("the --version process's end", || !alive(version_process));
    assert_eq!(setup.machine.status()["cua_driver"], cua_driver(None));
    wait_for_reports(&setup.engine, 5);
    wait_for_unread_warning(&root, 4);
    case.observe("at 4.999 seconds no <driver> mcp had started; at 5 seconds enable wrote {} and the --version process ended, status cua_driver was {version: null, tested_version}, cua reported its tools and one more warn record named only the tested version");
    case.pass();

    let mut case = record.case("disable");
    let run = disable(&setup.machine);
    succeeded(&mut case, "tetral computer-use disable", &run);
    assert_eq!(setup.machine.status()["cua_driver"], Value::Null);
    case.observe("after disable status cua_driver was null");
    case.pass();
}

#[test]
fn a_malformed_body_is_refused_and_changes_nothing() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("POST /v1/computer-use with malformed bodies");
    let setup = mcp::online_with(&mut case, Path::new(TEST_SERVER), listing_a(), |_, _| {});
    let root = setup.machine.root.clone();
    let run = enable(&setup.machine, setup.driver.driver());
    succeeded(
        &mut case,
        "tetral computer-use enable --driver <test server>",
        &run,
    );
    let config = fs::read(root.join("config.toml")).unwrap();
    let processes = setup.driver.wait_for_processes(1);

    let bodies = [
        json!({ "enabled": "yes", "driver": null }),
        json!({ "enabled": true, "driver": "rel/x" }),
        json!({ "enabled": true }),
        json!({ "enabled": false, "driver": null, "reason": "extra" }),
    ];
    for body in bodies {
        let response = request_with_body(
            &socket_path(&root),
            "POST",
            "/v1/computer-use",
            &serde_json::to_vec(&body).unwrap(),
        )
        .expect("an answer");
        assert_eq!(response.status, 400, "{body}");
        let answer = response.json();
        assert_eq!(answer["code"], "invalid_arguments", "{body}: {answer}");
        assert!(
            answer["message"]
                .as_str()
                .is_some_and(|message| !message.is_empty())
        );
        assert_eq!(
            fs::read(root.join("config.toml")).unwrap(),
            config,
            "{body}"
        );
        case.observe(format!(
            "{body}: 400 invalid_arguments; config.toml unchanged"
        ));
    }

    let client = sdk::Client::with_test_socket_context(
        &StateRoot::new(root.as_os_str()).unwrap(),
        socket_directory::context().paths(),
        sdk::RESPONSE_MAX_BYTES,
    )
    .unwrap();
    let answer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(client.set_computer_use(true, Some("rel/x")));
    let Err(sdk::Error::Http { status, body }) = answer else {
        panic!("set_computer_use with a relative driver: {answer:?}");
    };
    assert_eq!(status, 400);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["code"], "invalid_arguments");
    assert_eq!(fs::read(root.join("config.toml")).unwrap(), config);
    assert_eq!(setup.driver.processes(), processes);
    case.observe("the SDK's set_computer_use with a relative driver returned Http 400 with invalid_arguments; config.toml unchanged and no test server process started");
    case.pass();
}

#[test]
fn a_config_toml_the_app_server_cannot_use_fails_the_change_and_changes_nothing() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("enable on a running App Server whose config.toml does not parse");
    let setup = mcp::online_with(&mut case, Path::new(TEST_SERVER), listing_a(), |_, _| {});
    let root = setup.machine.root.clone();
    let config = root.join("config.toml");
    let configured = fs::read(&config).unwrap();
    fs::write(&config, "not = [toml").unwrap();
    let run = fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral computer-use enable --driver <test server>",
        computer_use(&root, &["enable", "--driver", setup.driver.driver()])
            .seams(&setup.machine.seams),
        "invalid_configuration",
    );
    assert_eq!(
        run.message(),
        format!(
            "{} does not follow the configuration format",
            config.display()
        )
    );
    let body = json!({ "enabled": true, "driver": setup.driver.driver() });
    let response = request_with_body(
        &socket_path(&root),
        "POST",
        "/v1/computer-use",
        &serde_json::to_vec(&body).unwrap(),
    )
    .expect("an answer");
    assert_eq!(
        (response.status, response.json()),
        (
            500,
            json!({ "code": "invalid_configuration", "message": run.message() })
        )
    );
    assert_eq!(setup.driver.processes(), Vec::<u32>::new());
    case.observe("POST /v1/computer-use answered 500 with the same code and message, and no test server process started");
    case.pass();

    let mut case =
        record.case("disable, with cua running, on an App Server whose config.toml cannot be read");
    if !case.runs_as_non_root("root reads a file of mode 0000") {
        return;
    }
    fs::write(&config, &configured).unwrap();
    let run = enable(&setup.machine, setup.driver.driver());
    succeeded(
        &mut case,
        "tetral computer-use enable --driver <test server>",
        &run,
    );
    let server = started_since(&setup.driver, 0)[0];
    wait_for_reports(&setup.engine, 1);
    let configured = fs::read(&config).unwrap();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o000)).unwrap();
    let run = fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral computer-use disable",
        computer_use(&root, &["disable"]).seams(&setup.machine.seams),
        "io_failed",
    );
    assert!(
        run.message()
            .starts_with(&format!("{}: ", config.display())),
        "{}",
        run.message()
    );
    let response = request_with_body(
        &socket_path(&root),
        "POST",
        "/v1/computer-use",
        br#"{"enabled": false, "driver": null}"#,
    )
    .expect("an answer");
    let answer = response.json();
    assert_eq!(
        (response.status, &answer["code"]),
        (500, &json!("io_failed")),
        "{answer}"
    );
    // cua still runs on its connection: a list change it sends is listed
    // and reported.
    setup.driver.control(
        server,
        json!({ "send": { "jsonrpc": "2.0", "method": "notifications/tools/list_changed" } }),
    );
    let report = &wait_for_reports(&setup.engine, 2)[1];
    assert_eq!(report["error"], Value::Null, "{report}");
    fs::set_permissions(&config, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(fs::read(&config).unwrap(), configured);
    case.observe("POST /v1/computer-use disabling answered 500 io_failed; cua kept running: a list change its server sent afterwards was reported");
    case.pass();
}

const SHUTDOWN_MESSAGE: &str = "The App Server is shutting down; retry after it has stopped or restarted. The configuration may already have changed.";

#[test]
fn an_already_written_setting_gets_an_explicit_shutdown_error_from_the_local_api() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("direct API enable already written while initialize waits, then shutdown");
    let setup = mcp::online_with(
        &mut case,
        Path::new(TEST_SERVER),
        json!({"initialize":"never"}),
        |_, _| {},
    );
    let root = &setup.machine.root;
    let socket = socket_path(root);
    let program = setup.driver.driver().to_owned();
    let body = serde_json::to_vec(&json!({"enabled":true,"driver":program})).unwrap();
    let caller =
        std::thread::spawn(move || request_with_body(&socket, "POST", "/v1/computer-use", &body));
    let pid = setup.driver.wait_for_processes(1)[0];
    setup.driver.wait_for_received(pid, "initialize", 1);
    assert_eq!(recorded(root), computer_use_record(true, &program));
    // Keep the original inode allocated: two incorrect atomic replacements
    // must not be able to recycle its number and evade the preservation check.
    let written_config = fs::File::open(root.join("config.toml")).unwrap();
    let written = written_config.metadata().unwrap().ino();
    assert_eq!(
        request(&socket_path(root), "POST", "/v1/shutdown")
            .unwrap()
            .status,
        202
    );
    let response = caller
        .join()
        .unwrap()
        .expect("the interrupted request must receive an HTTP response");
    assert_eq!(response.status, 500);
    assert_eq!(
        response.json(),
        json!({"code":"io_failed","message":SHUTDOWN_MESSAGE})
    );
    let root = root.clone();
    setup.machine.app_server.exited();
    assert_eq!(recorded(&root), computer_use_record(true, &program));
    assert_eq!(
        fs::metadata(root.join("config.toml")).unwrap().ino(),
        written
    );
    case.observe("500 io_failed carried the exact retry and possibly-changed message; server exited within its unchanged clock deadline; the already-written configuration and inode stayed");
    case.pass();
}

#[test]
fn written_and_queued_commands_fail_on_shutdown_and_only_explicit_reapplication_changes_settings() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("enable already written, disable admitted behind it, then shutdown and explicit reapplication after restart");
    let setup = mcp::online_with(
        &mut case,
        Path::new(TEST_SERVER),
        json!({"initialize":"never"}),
        |_, _| {},
    );
    let root = setup.machine.root.clone();
    let program = setup.driver.driver().to_owned();
    let enable = computer_use(&root, &["enable", "--driver", &program])
        .seams(&setup.machine.seams)
        .spawn();
    let pid = setup.driver.wait_for_processes(1)[0];
    setup.driver.wait_for_received(pid, "initialize", 1);
    assert_eq!(recorded(&root), computer_use_record(true, &program));
    // Keep the original inode allocated: two incorrect atomic replacements
    // must not be able to recycle its number and evade the preservation check.
    let written_config = fs::File::open(root.join("config.toml")).unwrap();
    let written = written_config.metadata().unwrap().ino();
    let queued = setup.machine.seams.count("mcp.setting_queued");
    let disable = computer_use(&root, &["disable"])
        .seams(&setup.machine.seams)
        .spawn();
    setup
        .machine
        .seams
        .wait_for_count("mcp.setting_queued", queued + 1);
    assert_eq!(
        request(&socket_path(&root), "POST", "/v1/shutdown")
            .unwrap()
            .status,
        202
    );
    let mcp::Setup {
        machine,
        driver,
        engine: _engine,
        directory: _directory,
    } = setup;
    let mut searched = machine.seams.count(SEARCH_CHECKED);
    machine.app_server.exited();
    // Drive an observed legacy search timer so restoring automatic completion
    // reaches the configuration/inode assertions instead of hanging the fixture.
    // The current commands exit on the HTTP error without entering this search.
    machine.seams.advance(Duration::from_millis(50));
    let mut children = [("enable", enable), ("disable", disable)];
    wait_until("both interrupted setting commands exit", || {
        let mut ended = true;
        for (_, child) in &mut children {
            ended &= child.try_wait().unwrap().is_some();
        }
        let checked = machine.seams.count(SEARCH_CHECKED);
        if !ended && checked > searched {
            searched = checked;
            machine.seams.advance(Duration::from_millis(50));
        }
        ended
    });
    let runs = children.map(|(command, child)| {
        let run = run::finish(child);
        case.command(
            &format!("tetral computer-use {command} interrupted by shutdown"),
            &run.exit(),
        );
        run
    });
    assert_eq!(
        (
            recorded(&root),
            fs::metadata(root.join("config.toml")).unwrap().ino()
        ),
        (computer_use_record(true, &program), written),
        "interrupted settings must preserve the configuration and its inode"
    );
    for run in runs {
        assert_eq!(run.status.code(), Some(1));
        assert_eq!(run.error_code(), "io_failed");
        assert_eq!(run.message(), SHUTDOWN_MESSAGE);
    }
    assert!(!socket_path(&root).exists(), "shutdown removed its socket");
    drop(hold_lock(&root));
    driver.set_scenario(listing_a());
    let (name, value) = driver.environment();
    let server = tetral_tests::app_server::AppServer::start(
        Tetral::new(["server", "run", "--app", APP])
            .home(&root)
            .seams(&machine.seams)
            .env(name, value),
        &root,
    );
    let machine = tetral_tests::machine::Machine {
        app_server: server,
        seams: machine.seams,
        root: root.clone(),
    };
    for enabled in [false, true] {
        let run = if enabled {
            mcp::enable(&machine, &program)
        } else {
            mcp::disable(&machine)
        };
        succeeded(
            &mut case,
            "explicit computer-use reapplication after restart",
            &run,
        );
        assert_eq!(recorded(&root), computer_use_record(enabled, &program));
        assert_eq!(machine.status()["cua_driver"].is_null(), !enabled);
    }
    case.observe("both admitted commands exited 1/io_failed with the exact truthful message; queued disable wrote nothing and neither command replaced the pre-shutdown inode; cleanup released socket/lock; explicit disable and enable after restart succeeded with matching config/status");
    case.pass();
}

#[test]
fn a_post_connect_eof_fails_without_recording_even_when_the_root_lock_is_available() {
    use std::io::{BufRead, Read};
    use std::os::unix::net::UnixListener;
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "socket accepts and reads a setting then closes without an answer, root lock available",
    );
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let socket_dir = socket_directory::socket_dir();
    fs::create_dir_all(&socket_dir).unwrap();
    fs::set_permissions(&socket_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let socket = socket_path(&root);
    let listener = UnixListener::bind(&socket).unwrap();
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
    let before = fs::read(root.join("config.toml")).unwrap();
    let inode = fs::metadata(root.join("config.toml")).unwrap().ino();
    let child = computer_use(&root, &["enable", "--driver", DRIVER]).spawn();
    let (stream, _) = listener.accept().unwrap();
    let mut reader = std::io::BufReader::new(stream);
    let mut header = String::new();
    let mut length = 0;
    loop {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).unwrap() > 0);
        if line == "\r\n" {
            break;
        }
        if let Some(n) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
            length = n.trim().parse::<usize>().unwrap();
        }
        header.push_str(&line);
    }
    assert!(header.starts_with("POST /v1/computer-use HTTP/1.1"));
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"enabled":true,"driver":DRIVER})
    );
    drop(reader);
    drop(listener);
    let run = run::finish(child);
    case.command(
        "tetral computer-use enable after transport EOF",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(1));
    assert_eq!(run.error_code(), "io_failed");
    assert_eq!(fs::read(root.join("config.toml")).unwrap(), before);
    assert_eq!(fs::metadata(root.join("config.toml")).unwrap().ino(), inode);
    // The same socket path now has no listener. A new caller may use the
    // existing offline path after an actual connection refusal.
    assert_eq!(
        std::os::unix::net::UnixStream::connect(&socket)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::ConnectionRefused
    );
    let offline = computer_use(&root, &["enable", "--driver", DRIVER]).run();
    succeeded(
        &mut case,
        "tetral computer-use enable after connection refusal",
        &offline,
    );
    assert_eq!(recorded(&root), computer_use_record(true, DRIVER));
    fs::remove_file(socket).unwrap();
    case.observe("post-connect EOF exited 1/io_failed and did not write the available root; a new command after verified ConnectionRefused succeeded and recorded its setting through the offline root-lock path");
    case.pass();
}
