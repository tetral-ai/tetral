//! Real restricted processes on Linux. Each case runs commands through the
//! isolation's availability check and launch, as the task supervisor does,
//! by starting `tetral` as its isolation harness, or as tool calls of a
//! running App Server, which meet its own state root, control socket, task
//! directories and configuration, and observes from outside every sandbox
//! what the commands could reach.

#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;
use sha2::{Digest, Sha256};
use tetral_tests::binaries;
use tetral_tests::isolation::{
    self, Fails, SYSTEM_PATH, Served, Succeeds, World, harness, policy, quoted, read_write,
};
use tetral_tests::mcp::{self, TestDriver};
use tetral_tests::other_user::RootDirectory;
use tetral_tests::record::Case;
use tetral_tests::seams::Seams;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{BINDING, SESSION, TaskMachine, policy as task_policy};

/// The test MCP server, which starts as a driver's `<driver> mcp`.
const TEST_SERVER: &str = env!("CARGO_BIN_EXE_mcp-test-server");

fn host_bwrap() -> PathBuf {
    let path = SYSTEM_PATH
        .split(':')
        .map(|directory| Path::new(directory).join("bwrap"))
        .find(|candidate| candidate.is_file())
        .expect("the host has a bwrap on /usr/bin or /bin");
    path.canonicalize().unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn every_grant_reaches_only_what_its_rules_allow_and_never_the_protected_paths() {
    let mut case = tetral_tests::record!()
        .case("every grant reaches only what its rules allow and never the protected paths");
    isolation::grants_reach_only_what_their_rules_allow(&mut case, &World::new());
    case.pass();
}

#[test]
fn a_missing_deny_path_under_a_directory_of_another_user_stays_uncreatable_and_the_command_runs() {
    let mut case = tetral_tests::record!().case(
        "a missing deny path under a directory of another user stays uncreatable and the command runs",
    );
    if !case.runs_as_non_root("root may create entries in /") {
        return;
    }
    isolation::a_missing_deny_path_stays_uncreatable(&mut case, &World::new());
    case.pass();
}

#[test]
fn a_rule_for_a_regular_file_and_a_deny_path_under_one_leave_the_command_running() {
    let mut case = tetral_tests::record!().case(
        "a read_write rule for a regular file, and a deny path under a regular file, in a writable project",
    );
    isolation::regular_file_rules_leave_the_command_running(&mut case, &World::new());
    case.pass();
}

#[test]
fn the_probes_run_again_as_calls_of_an_app_server_against_its_own_paths() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "the isolation probes as exec_command calls of an App Server started from <p>/bin/tetral, and those that read or write one file as view_image and edit calls, against its own state root, control socket and task directories",
    );
    let served = Served::start(&mut case);
    isolation::grants_reach_only_what_their_rules_allow(&mut case, &served);
    isolation::regular_file_rules_leave_the_command_running(&mut case, &served);
    isolation::file_probes(&mut case, &served);
    case.pass();
}

#[test]
fn a_missing_deny_path_stays_uncreatable_as_an_exec_command_call() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "a missing deny path under a directory of another user, as an exec_command call of an App Server",
    );
    if !case.runs_as_non_root("root may create entries in /") {
        return;
    }
    let served = Served::start(&mut case);
    isolation::a_missing_deny_path_stays_uncreatable(&mut case, &served);
    case.pass();
}

/// Runs, in `tasks`'s binding `binding`, a command that writes a file
/// beside the driver in the project's `bin` and then writes and replaces
/// the driver, under a rule that makes the project writable; asserts that
/// only the file beside it was written.
fn driver_stays_read_only(tasks: &TaskMachine, binding: &str, bin: &Path, contents: &[u8]) {
    let rules = json!([
        {"path": "/", "access": "read"},
        {"path": tasks.project, "access": "read_write"},
    ]);
    let task = format!("task_{binding}");
    tasks.execute_with(
        SESSION,
        binding,
        &task,
        json!({"cmd": "printf y > bin/other; printf x > bin/cua-driver; printf z > r && mv -f r bin/cua-driver"}),
        policy(rules, "disabled"),
        None,
    );
    let terminal = tasks.terminal(&task);
    assert_ne!(
        terminal["result"]["result"]["exit_code"],
        json!(0),
        "{terminal}"
    );
    assert_eq!(fs::read_to_string(bin.join("other")).unwrap(), "y");
    assert!(
        fs::read(bin.join("cua-driver")).unwrap() == contents,
        "the driver changed"
    );
}

#[test]
fn the_cua_driver_that_config_toml_records_stays_read_only_under_a_writable_rule() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a cua-driver recorded in config.toml through a symbolic link to a directory of the read_write project: write a file beside it, then write and replace the driver",
    );
    let directory = tempfile::tempdir().unwrap();
    let mut tasks = TaskMachine::start(&mut case, directory.path());
    let bin = tasks.project.join("bin");
    fs::create_dir(&bin).unwrap();
    let driver = bin.join("cua-driver");
    fs::write(&driver, "driver\n").unwrap();
    // Recorded through a symbolic link, as a driver on a linked directory is.
    let link = directory.path().join("link");
    symlink(&bin, &link).unwrap();
    let recorded_driver = link.join("cua-driver");
    let config = tasks.root.join("config.toml");
    tasks.kill_and_restart(|_| {
        let recorded = format!(
            "{}\n[computer_use]\nenabled = false\ndriver = \"{}\"\n",
            fs::read_to_string(&config).unwrap(),
            recorded_driver.display()
        );
        fs::write(&config, recorded).unwrap();
    });
    let issued = tasks.prepare(SESSION, "bind_recorded", &tasks.project, None);
    tasks.wait_prepared(&issued);
    driver_stays_read_only(&tasks, "bind_recorded", &bin, b"driver\n");
    case.observe("the file beside the driver holds y; the write and the replacement of the recorded driver failed, which still holds driver");
    case.pass();
    drop(tasks);

    let mut case = record.case(
        "a cua-driver in the read_write project recorded by computer-use enable on the running App Server, then a task of the binding prepared before it: write a file beside it, then write and replace the driver",
    );
    let directory = tempfile::tempdir().unwrap();
    let scenario = directory.path().join("driver");
    let test_driver = TestDriver::new(&scenario, Path::new(TEST_SERVER), json!({}));
    let tasks = TaskMachine::start_with_environment(
        &mut case,
        directory.path(),
        &[test_driver.environment()],
    );
    let bin = tasks.project.join("bin");
    fs::create_dir(&bin).unwrap();
    let driver = bin.join("cua-driver");
    fs::copy(TEST_SERVER, &driver).unwrap();
    let run = mcp::enable(tasks.machine(), driver.to_str().unwrap());
    case.command(
        "tetral computer-use enable --app app_one --driver <project>/bin/cua-driver",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    // The binding was prepared before the enable; its task comes after.
    driver_stays_read_only(&tasks, BINDING, &bin, &fs::read(TEST_SERVER).unwrap());
    case.observe("the enable answered {}; in the binding prepared before it, the file beside the driver holds y; the write and the replacement of the driver failed, which is still the test server");
    case.pass();
}

#[test]
fn disabled_blocks_tcp_and_udp_and_direct_reaches_them_and_local_services() {
    let mut case = tetral_tests::record!()
        .case("disabled blocks TCP and UDP, and direct reaches them and local services");
    isolation::network_reach(&mut case);
    case.pass();
}

#[test]
fn a_disabled_task_leaves_the_connection_its_heartbeats_and_a_direct_tasks_connect_working() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "a disabled task running across 20 seconds of the clock while a direct task connects to a TCP listener on the host's LAN address",
    );
    isolation::disabled_task_beside_direct(&mut case);
    case.pass();
}

/// A directory holding only an executable `bwrap` script with `body`.
fn fake_bwrap(directory: &Path, body: &str) -> PathBuf {
    fs::create_dir_all(directory).unwrap();
    let bwrap = directory.join("bwrap");
    fs::write(&bwrap, format!("#!/bin/sh\n{body}\n")).unwrap();
    fs::set_permissions(&bwrap, fs::Permissions::from_mode(0o755)).unwrap();
    directory.to_path_buf()
}

/// One way of offering a `bwrap` to the isolation: the `tetral` build, the
/// App Server's `PATH`, the bytes of a `bwrap` beside `tetral`, and the
/// command's own `PATH`, applied only at the isolated command exec.
#[derive(Clone)]
struct Offer<'a> {
    name: &'a str,
    tetral: &'a Path,
    path: &'a str,
    beside: Option<Vec<u8>>,
    command_path: &'a str,
    expected: Expected<'a>,
}

#[derive(Clone, Copy)]
enum Expected<'a> {
    /// The command runs and writes its marker; a planted `bwrap` that
    /// comes later on `PATH` never runs.
    Runs { ignored: Option<&'a Path> },
    /// The check rejects the call `sandbox_unavailable` and nothing starts;
    /// for a planted `bwrap`, the message names it and it never runs.
    Unavailable { planted: Option<&'a Path> },
}

/// The marker that a planted `bwrap` writes when it runs.
fn ran_marker(planted: &Path) -> PathBuf {
    planted.with_file_name("ran")
}

/// How an offer's command is started: by the isolation harness, or as a
/// `tool.execute` of an App Server run from the offer's `tetral` with its
/// `PATH`, the command's `PATH` in the call's `env`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Via {
    Harness,
    ToolExecute,
}

/// Checks `offer` by the harness and as a `tool.execute`.
fn check_offer_both_ways(case: &mut Case, offer: Offer) {
    check_offer(case, offer.clone(), Via::Harness);
    check_offer(case, offer, Via::ToolExecute);
}

/// Runs a command that writes a marker through `offer` `via` the harness or
/// a call, and checks what it expects.
fn check_offer(case: &mut Case, offer: Offer, via: Via) {
    let Offer {
        name,
        tetral,
        path,
        beside,
        command_path,
        expected,
    } = offer;
    let world = World::with_tetral(tetral);
    if let Some(bytes) = beside {
        let bwrap = world.tetral.with_file_name("bwrap");
        fs::write(&bwrap, bytes).unwrap();
        fs::set_permissions(&bwrap, fs::Permissions::from_mode(0o755)).unwrap();
    }
    if via == Via::ToolExecute {
        check_offered_call(case, &world, name, path, command_path, expected);
        return;
    }
    let marker = world.project.join("marker");
    let request = world.request(
        &read_write(&world.project),
        &world.project,
        &["/bin/sh", "-c", &format!("echo > {}", quoted(&marker))],
        command_path,
    );
    let seams = Seams::serve(&world.base, Duration::ZERO);
    let outcome = harness(&world.tetral, &request, path, Some(&seams));
    let spawned = seams.count("process.spawn");
    case.observe(format!(
        "{name}: {outcome}; process.spawn {spawned}; marker {}",
        if marker.exists() { "written" } else { "absent" }
    ));
    let names = |text: &str, planted: &Path| {
        assert!(text.contains(planted.to_str().unwrap()), "{name}: {text}");
        assert!(
            !ran_marker(planted).exists(),
            "{name}: the planted bwrap ran"
        );
    };
    match expected {
        Expected::Runs { ignored } => {
            assert_eq!(outcome["exit_code"], 0, "{name}: {outcome}");
            assert_eq!((spawned, marker.exists()), (1, true), "{name}");
            if let Some(ignored) = ignored {
                assert!(
                    !ran_marker(ignored).exists(),
                    "{name}: the planted bwrap ran"
                );
            }
        }
        Expected::Unavailable { planted } => {
            assert_eq!(
                outcome["rejected"]["code"], "sandbox_unavailable",
                "{name}: {outcome}"
            );
            assert_eq!((spawned, marker.exists()), (0, false), "{name}");
            if let Some(planted) = planted {
                names(outcome["rejected"]["message"].as_str().unwrap(), planted);
            }
        }
    }
}

/// [`check_offer`] as a `tool.execute` of an App Server run from the
/// world's `tetral` with `path` as its `PATH`.
fn check_offered_call(
    case: &mut Case,
    world: &World,
    name: &str,
    path: &str,
    command_path: &str,
    expected: Expected,
) {
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start_from(case, directory.path(), &world.tetral, path);
    let marker = tasks.project.join("marker");
    let spawned_before = tasks.machine().seams.count("process.spawn");
    tasks.execute(
        "task_offer",
        json!({"cmd": format!("echo > {}", quoted(&marker)), "env": {"PATH": command_path}}),
        task_policy(&tasks.project, "direct"),
    );
    let (phase, report) = match expected {
        Expected::Runs { .. } => ("terminal", tasks.terminal("task_offer")),
        _ => ("rejected", tasks.report("task_offer", "rejected")),
    };
    let spawned = tasks.machine().seams.count("process.spawn") - spawned_before;
    case.observe(format!(
        "{name}, as a tool.execute: {phase} {}; process.spawn {spawned}; marker {}",
        if phase == "terminal" {
            report["result"]["result"]["exit_code"].to_string()
        } else {
            report["error"].to_string()
        },
        if marker.exists() { "written" } else { "absent" }
    ));
    match expected {
        Expected::Runs { ignored } => {
            assert_eq!(
                report["result"]["result"]["exit_code"], 0,
                "{name}: {report}"
            );
            assert_eq!((spawned, marker.exists()), (1, true), "{name}");
            if let Some(ignored) = ignored {
                assert!(
                    !ran_marker(ignored).exists(),
                    "{name}: the planted bwrap ran"
                );
            }
        }
        Expected::Unavailable { planted } => {
            assert_eq!(
                report["error"]["code"], "sandbox_unavailable",
                "{name}: {report}"
            );
            assert_eq!((spawned, marker.exists()), (0, false), "{name}");
            if let Some(planted) = planted {
                let message = report["error"]["message"].as_str().unwrap();
                assert!(
                    message.contains(planted.to_str().unwrap()),
                    "{name}: {message}"
                );
                assert!(
                    !ran_marker(planted).exists(),
                    "{name}: the planted bwrap ran"
                );
            }
        }
    }
}

#[test]
fn each_bwrap_offer_runs_the_command_rejects_the_call_or_has_the_helper_refuse_it() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "each bwrap offer: the command runs, the call is rejected sandbox_unavailable before any process starts, or tetral as codex-linux-sandbox refuses a planted bwrap before running it",
    );
    let host_bwrap_bytes = fs::read(host_bwrap()).unwrap();
    let digest_build = binaries::tetral_with_bwrap_digest(&sha256_hex(&host_bwrap_bytes));
    let scratch = tempfile::tempdir().unwrap();
    let scratch = scratch.path().canonicalize().unwrap();
    let empty = scratch.join("empty");
    fs::create_dir_all(&empty).unwrap();
    let empty = empty.to_str().unwrap();
    let no_namespaces =
        b"#!/bin/sh\necho 'bwrap: No permissions to create a new namespace' >&2\nexit 1\n".to_vec();

    // A copy of the host's bwrap that the test user owns, in a PATH directory
    // ahead of the system one that the call's profile cannot write, as an
    // earlier call with a broader grant could have left it there.
    let planted_directory = scratch.join("planted");
    fs::create_dir_all(&planted_directory).unwrap();
    let copy = planted_directory.join("bwrap-copy");
    fs::copy(host_bwrap(), &copy).unwrap();
    let planted = planted_directory.join("bwrap");
    fake_bwrap(
        &planted_directory,
        &format!(
            "echo > {}\nexec {} \"$@\"",
            quoted(&ran_marker(&planted)),
            quoted(&copy)
        ),
    );
    let planted_path = format!("{}:{SYSTEM_PATH}", planted_directory.display());
    let planted_last = format!("{SYSTEM_PATH}:{}", planted_directory.display());
    // A symbolic link to the host's bwrap: root owns the file, but the test
    // user can replace the link.
    let link_directory = scratch.join("link");
    fs::create_dir_all(&link_directory).unwrap();
    let link = link_directory.join("bwrap");
    symlink(host_bwrap(), &link).unwrap();
    let link_path = format!("{}:{SYSTEM_PATH}", link_directory.display());

    let rows = [
        Offer {
            name: "no bwrap on PATH and none beside tetral",
            tetral: binaries::tetral(),
            path: empty,
            beside: None,
            command_path: SYSTEM_PATH,
            expected: Expected::Unavailable { planted: None },
        },
        Offer {
            name: "a bwrap beside a tetral built with a digest that its bytes do not match",
            tetral: digest_build,
            path: empty,
            beside: Some([host_bwrap_bytes.as_slice(), b"\0"].concat()),
            command_path: SYSTEM_PATH,
            expected: Expected::Unavailable { planted: None },
        },
        Offer {
            name: "no bwrap on PATH and, beside a tetral built without a digest, a bwrap that cannot create a namespace",
            tetral: binaries::tetral(),
            path: empty,
            beside: Some(no_namespaces),
            command_path: SYSTEM_PATH,
            expected: Expected::Unavailable { planted: None },
        },
        Offer {
            name: "the host's bwrap on PATH",
            tetral: binaries::tetral(),
            path: SYSTEM_PATH,
            beside: None,
            command_path: SYSTEM_PATH,
            expected: Expected::Runs { ignored: None },
        },
        Offer {
            name: "a copy of the host's bwrap that the test user owns, ahead of the system one on PATH, in a directory the call cannot write",
            tetral: binaries::tetral(),
            path: &planted_path,
            beside: None,
            command_path: SYSTEM_PATH,
            expected: Expected::Unavailable {
                planted: Some(&planted),
            },
        },
        Offer {
            name: "that copy after the system bwrap on PATH",
            tetral: binaries::tetral(),
            path: &planted_last,
            beside: None,
            command_path: &planted_last,
            expected: Expected::Runs {
                ignored: Some(&planted),
            },
        },
        Offer {
            name: "a symbolic link that the test user owns, ahead of the system bwrap on PATH, to the host's bwrap",
            tetral: binaries::tetral(),
            path: &link_path,
            beside: None,
            command_path: SYSTEM_PATH,
            expected: Expected::Unavailable {
                planted: Some(&link),
            },
        },
        Offer {
            name: "a task-only PATH cannot change the sandbox launcher selection",
            tetral: binaries::tetral(),
            path: SYSTEM_PATH,
            beside: None,
            command_path: &planted_path,
            expected: Expected::Runs {
                ignored: Some(&planted),
            },
        },
    ];
    for offer in rows {
        check_offer_both_ways(&mut case, offer);
    }
    case.pass();

    let mut case = record.case(
        "bwrap files in directories that root owns: one root owns whose --help lacks --as-pid-1, alone on PATH, and one the test user owns, ahead of the system bwrap",
    );
    if !case.runs_as_non_root("root can modify every file, so it trusts no system bwrap") {
        return;
    }
    let old_help = fake_bwrap(
        &scratch.join("old-help"),
        "echo 'usage: bwrap [OPTIONS...] [--] COMMAND [ARGS...]\n    --perms OCTAL'",
    )
    .join("bwrap");
    let user = (
        rustix::process::getuid().as_raw(),
        rustix::process::getgid().as_raw(),
    );
    let made = (|| {
        let help_directory = RootDirectory::new("tetral-old-help-bwrap")?;
        help_directory.install(&old_help, "bwrap", (0, 0))?;
        let user_directory = RootDirectory::new("tetral-user-bwrap")?;
        let user_bwrap = user_directory.install(&host_bwrap(), "bwrap", user)?;
        Ok::<_, String>((help_directory, user_directory, user_bwrap))
    })();
    let (help_directory, user_directory, user_bwrap) = match made {
        Ok(made) => made,
        Err(reason) => {
            case.not_run(reason);
            return;
        }
    };
    let user_path = format!("{}:{SYSTEM_PATH}", user_directory.path().display());
    for offer in [
        Offer {
            name: "the only bwrap on PATH, owned by root, lacks --as-pid-1",
            tetral: binaries::tetral(),
            path: help_directory.path().to_str().unwrap(),
            beside: None,
            command_path: SYSTEM_PATH,
            expected: Expected::Unavailable { planted: None },
        },
        Offer {
            name: "a copy of the host's bwrap that the test user owns, in a directory root owns, ahead of the system bwrap on PATH",
            tetral: binaries::tetral(),
            path: &user_path,
            beside: None,
            command_path: SYSTEM_PATH,
            expected: Expected::Unavailable {
                planted: Some(&user_bwrap),
            },
        },
    ] {
        check_offer_both_ways(&mut case, offer);
    }
    case.pass();
}

#[test]
fn the_bundled_bwrap_with_its_digest_runs_tools_where_no_system_bwrap_exists() {
    let mut case = tetral_tests::record!()
        .case("the bundled bwrap with its digest runs tools where no system bwrap exists");
    let host_bwrap = host_bwrap();
    let bytes = fs::read(&host_bwrap).unwrap();
    let world = World::with_tetral(binaries::tetral_with_bwrap_digest(&sha256_hex(&bytes)));
    let bwrap = world.tetral.with_file_name("bwrap");
    fs::write(&bwrap, &bytes).unwrap();
    fs::set_permissions(&bwrap, fs::Permissions::from_mode(0o755)).unwrap();

    // The App Server's PATH and the commands' hold the tools but no bwrap.
    let tools = world.base.join("tools");
    fs::create_dir_all(&tools).unwrap();
    for tool in ["bash", "cat", "cp", "mv", "python3"] {
        let target = Path::new("/usr/bin").join(tool).canonicalize().unwrap();
        symlink(target, tools.join(tool)).unwrap();
    }
    let path = tools.to_str().unwrap();
    let p = &world.project;
    // Outside every grant, and removed even when a probe wrongly writes it.
    let outside = tempfile::TempPath::try_from_path(format!(
        "/tmp/tetral-isolation-probe-{}-bundled",
        std::process::id()
    ))
    .unwrap();
    let probes = [
        (
            "read /etc/hostname",
            "cat /etc/hostname".to_owned(),
            Succeeds,
        ),
        (
            "write <p>/a",
            format!("printf x > {}", quoted(&p.join("a"))),
            Succeeds,
        ),
        (
            "write a file in the task's temporary directory",
            format!("printf x > {}", quoted(&world.task_temporary.join("f"))),
            Succeeds,
        ),
        (
            "write /tmp",
            format!("printf x > {}", quoted(&outside)),
            Fails,
        ),
        (
            "write <p>/.git/config",
            format!("printf x >> {}", quoted(&p.join(".git/config"))),
            Fails,
        ),
        (
            "rename <p>/.git",
            format!("mv {} {}", quoted(&p.join(".git")), quoted(&p.join("g"))),
            Fails,
        ),
        (
            "replace the bwrap beside tetral",
            format!(
                "cp /bin/true {r} && mv -f {r} {b}",
                r = quoted(&p.join("replacement")),
                b = quoted(&bwrap)
            ),
            Fails,
        ),
    ];
    let policy = read_write(p);
    let request = world.request(
        &policy,
        p,
        &["/bin/bash", "-c", &isolation::script(&probes)],
        path,
    );
    let outcome = harness(&world.tetral, &request, path, None);
    isolation::check_probes(&mut case, &policy, &outcome, &probes);
    assert!(!outside.exists(), "{} was written", outside.display());
    case.pass();
}
