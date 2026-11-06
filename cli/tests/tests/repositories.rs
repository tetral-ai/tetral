//! A cloud computer's repositories in `tetral server run` against the
//! reference control server and a loopback Git smart-HTTP server over HTTPS
//! that stands in for git-proxy, as separate processes with real `git`: the
//! path checks, the git ticket, the clone, checkout and reuse of each
//! repository with its local configuration only, the cause and message of
//! each failure, and a clone that finishes after its connection ended.
//!
//! Besides the counters that `tetral_tests::machine` names, the App Server
//! reports the test seam counter `git.spawn` for each `git` it spawns.

use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::git_server::{Fault, GitServer, Remote};
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, SYSTEM_PATH, answer_fault, bootstrap, credential, engine,
    one_credential_engine,
};
use tetral_tests::record::Case;
use tetral_tests::reference::ReferenceServer;
use tetral_tests::sessions::{
    closed, failed, new_connection, new_connection_after, online_machine, outcome, prepare,
    reports_for,
};
use tetral_tests::socket_directory;

const GIT_SPAWN: &str = "git.spawn";

/// A cloud computer's `session.prepare` payload for `repositories`.
fn payload(session: &str, binding: &str, cwd: &Path, repositories: Value) -> Value {
    json!({
        "session_id": session,
        "binding_id": binding,
        "cwd": cwd,
        "create_cwd": true,
        "drives": [],
        "drive_exclude": [],
        "repositories": repositories,
    })
}

fn repository(url: &str, path: &Path, checkout: Value) -> Value {
    json!({
        "url": url,
        "checkout": checkout,
        "path": path,
        "git_identity": { "name": "Agent", "email": "agent@example.com" },
    })
}

/// Waits for the report that answers `message_id` and gives it.
fn report(engine: &ReferenceServer, message_id: &str) -> Value {
    let mut reports = Vec::new();
    wait_until(&format!("the report answering {message_id}"), || {
        reports = reports_for(engine, message_id);
        !reports.is_empty()
    });
    reports.remove(0)
}

/// The `[path, cause]` of each repository failure of a `prepared` report.
fn failures(report: &Value) -> Vec<(String, String)> {
    assert_eq!(report["status"], "prepared", "{report}");
    report["repository_failures"]
        .as_array()
        .map(|failures| {
            failures
                .iter()
                .map(|failure| {
                    (
                        failure["path"].as_str().unwrap().to_owned(),
                        failure["cause"].as_str().unwrap().to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn text(path: &Path) -> String {
    path.to_str().unwrap().to_owned()
}

/// The paths `repositories.json` of `session` lists.
fn listed(machine: &Machine, session: &str) -> Vec<String> {
    fs::read(
        machine
            .root
            .join("sessions")
            .join(session)
            .join("repositories.json"),
    )
    .map(|bytes| serde_json::from_slice(&bytes).unwrap())
    .unwrap_or_default()
}

/// Every path under `directory` but the App Server's log, sorted.
fn tree(directory: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.starts_with(directory.join("logs")) {
                continue;
            }
            if path.is_dir() && !path.is_symlink() {
                pending.push(path.clone());
            }
            paths.push(path);
        }
    }
    paths.sort();
    paths
}

/// A machine with `git` on its `PATH`, its own `HOME` and
/// `XDG_CONFIG_HOME` under `base`, and the Git server's certificate trusted
/// through `GIT_SSL_CAINFO`.
struct GitMachine {
    machine: Machine,
    environment: Vec<(&'static str, String)>,
}

impl GitMachine {
    fn start(case: &mut Case, engine: &ReferenceServer, git: &GitServer, base: &Path) -> Self {
        Self::start_with_args(case, engine, git, base, &[])
    }

    fn start_with_args(
        case: &mut Case,
        engine: &ReferenceServer,
        git: &GitServer,
        base: &Path,
        extra: &[&str],
    ) -> Self {
        Self::start_with_inheritance(case, engine, git, base, extra, false, None)
    }

    fn start_with_inheritance(
        case: &mut Case,
        engine: &ReferenceServer,
        git: &GitServer,
        base: &Path,
        extra: &[&str],
        private_descriptor: bool,
        low_speed_seconds: Option<u64>,
    ) -> Self {
        let home = base.join("home");
        let xdg = base.join("xdg");
        fs::create_dir_all(xdg.join("git")).unwrap();
        fs::create_dir_all(&home).unwrap();
        fs::write(home.join(".gitconfig"), "[core]\n\tautocrlf = false\n").unwrap();
        fs::write(xdg.join("git/config"), "[color]\n\tui = never\n").unwrap();
        let environment = vec![
            ("PATH", SYSTEM_PATH.to_owned()),
            ("HOME", text(&home)),
            ("XDG_CONFIG_HOME", text(&xdg)),
            ("GIT_SSL_CAINFO", text(&git.ca_file)),
            ("GIT_HTTP_LOW_SPEED_LIMIT", "99999999".to_owned()),
            ("GIT_HTTP_LOW_SPEED_TIME", "1".to_owned()),
        ];
        let mut args = vec!["server", "run", "--app", tetral_tests::machine::APP];
        args.extend_from_slice(extra);
        let mut command = if private_descriptor {
            // Shell redirection deliberately leaves a non-CLOEXEC descriptor
            // in the App Server. The protected launch must close it in Git.
            let inherited = base.join("machine/root/credentials/inherited-probe");
            let wrapper = format!(
                "printf dummy-descriptor > '{}'; exec 9<'{}'; exec '{}' \"$@\"",
                inherited.display(),
                inherited.display(),
                tetral_tests::binaries::tetral().display()
            );
            let mut wrapped = vec!["-c", &wrapper, "preparation-parent"];
            wrapped.extend(args);
            tetral_tests::run::Tetral::at(Path::new("/bin/sh"), wrapped)
        } else {
            tetral_tests::run::Tetral::new(args)
        }
        .current_dir(&tetral_tests::workspace_dir().join("tests"))
        .env(
            tetral_tests::machine::TOOL_EXECUTION_VARIABLE,
            tetral_tests::machine::tool_execution_fragment(),
        )
        .env("TETRAL_DUMMY_CONTROL", "private-control-value");
        if let Some(seconds) = low_speed_seconds {
            command = command.env("TETRAL_TEST_GIT_LOW_SPEED_SECONDS", seconds.to_string());
        }
        for (key, value) in &environment {
            command = command.env(key, value);
        }
        let machine = Machine::start_as(
            case,
            &base.join("machine"),
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            command,
        );
        machine.wait_for("online");
        GitMachine {
            machine,
            environment,
        }
    }

    /// The bytes of the global Git configuration files.
    fn global_configuration(&self) -> Vec<Vec<u8>> {
        let variable = |name: &str| {
            self.environment
                .iter()
                .find(|(each, _)| *each == name)
                .map(|(_, value)| PathBuf::from(value))
                .unwrap()
        };
        vec![
            fs::read(variable("HOME").join(".gitconfig")).unwrap(),
            fs::read(variable("XDG_CONFIG_HOME").join("git/config")).unwrap(),
        ]
    }

    /// Runs `git` in the App Server's environment and gives its trimmed
    /// stdout, or its stderr when it fails.
    fn git(&self, arguments: &[&str]) -> Result<String, String> {
        let output = Command::new("git")
            .args(arguments)
            .env_clear()
            .envs(self.environment.iter().map(|(name, value)| (name, value)))
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap();
        if output.status.success() {
            Ok(String::from_utf8(output.stdout).unwrap().trim().to_owned())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).into_owned())
        }
    }

    fn in_repository(&self, path: &Path, arguments: &[&str]) -> String {
        let mut all = vec!["-C", path.to_str().unwrap()];
        all.extend_from_slice(arguments);
        self.git(&all).unwrap()
    }
}

/// The Git server and a reference control server that names it as
/// `git_proxy_url`, and whose current tickets it accepts.
fn servers(base: &Path) -> (GitServer, ReferenceServer) {
    let git = GitServer::start(&base.join("git"));
    let engine = engine(
        &[credential(INSTANCE, CREDENTIAL, KEY, "active")],
        json!({ "git_proxy_url": git.url }),
    );
    git.check_tickets(engine.current_tickets());
    (git, engine)
}

/// The current ticket of `session`'s computer.
fn current_ticket(engine: &ReferenceServer, session: &str) -> String {
    let state = engine.state();
    let tickets = state["tickets"][format!("{INSTANCE}/{session}")]
        .as_array()
        .unwrap();
    tickets.last().unwrap().as_str().unwrap().to_owned()
}

#[test]
fn a_repository_path_in_the_state_root_lexically_or_through_a_link_fails_before_any_ticket() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let engine = one_credential_engine();
    let mut case =
        record.case("a repository path inside the state root, directly and through a link");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    let link = base.join("link");
    symlink(&machine.root, &link).unwrap();
    let before = tree(&machine.root);
    for (session, path) in [
        ("sess_lexical", machine.root.join("clones/../repo")),
        ("sess_linked", link.join("repo")),
    ] {
        let issued = prepare(
            &engine,
            &payload(
                session,
                "bind_1",
                &base.join("work").join(session),
                json!([repository("https://github.com/acme/a", &path, Value::Null)]),
            ),
        );
        let report = report(&engine, &issued);
        assert_eq!(
            (&report["status"], &report["error"]["code"]),
            (&json!("failed"), &json!("repository_path_forbidden")),
            "{session}"
        );
    }
    assert!(engine.requests("git-ticket").is_empty());
    let after: Vec<PathBuf> = tree(&machine.root)
        .into_iter()
        .filter(|path| !path.starts_with(machine.root.join("sessions")))
        .collect();
    let before: Vec<PathBuf> = before
        .into_iter()
        .filter(|path| !path.starts_with(machine.root.join("sessions")))
        .collect();
    assert_eq!(after, before);
    assert!(!machine.root.join("repo").exists());
    assert_eq!(machine.seams.count(GIT_SPAWN), 0);
    case.observe("both preparations failed with repository_path_forbidden; no git ticket was fetched, no git ran and nothing new is under the state root outside the Session records");
    case.pass();
}

#[test]
fn a_refused_or_empty_ticket_fails_every_repository_with_cause_clone_and_clones_nothing() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let mut case = record.case(
        "a ticket fetch refused with 409 unknown_instruction, then one whose ticket is empty",
    );
    let machine = GitMachine::start(&mut case, &engine, &git, base);
    let remote = git.repository("alpha");
    let work = base.join("work");
    let paths = [work.join("one"), work.join("two")];
    let repositories = json!([
        repository(&remote.url, &paths[0], Value::Null),
        repository(&remote.url, &paths[1], Value::Null),
    ]);
    let expected: Vec<(String, String)> = paths
        .iter()
        .map(|path| (text(path), "clone".to_owned()))
        .collect();
    answer_fault(
        &engine,
        json!({ "endpoint": "git-ticket", "action": "error", "code": "unknown_instruction" }),
    );
    let refused = prepare(
        &engine,
        &payload("sess_a", "bind_1", &work.join("a"), repositories.clone()),
    );
    let refused = report(&engine, &refused);
    assert_eq!(failures(&refused), expected);
    assert_eq!(engine.requests("git-ticket").len(), 1);
    answer_fault(
        &engine,
        json!({
            "endpoint": "git-ticket", "action": "schema_fail",
            "body": { "git_proxy_url": git.url, "ticket": "" },
        }),
    );
    let empty = prepare(
        &engine,
        &payload("sess_b", "bind_1", &work.join("b"), repositories),
    );
    let empty = report(&engine, &empty);
    assert_eq!(failures(&empty), expected);
    // The refusal's own message, and the schema failure's description.
    for (report, message) in [
        (&refused, "injected by the answer control"),
        (&empty, "the git ticket answer does not match its schema"),
    ] {
        let messages: Vec<&Value> = report["repository_failures"]
            .as_array()
            .unwrap()
            .iter()
            .map(|failure| &failure["message"])
            .collect();
        assert_eq!(messages, [message, message]);
    }
    for path in &paths {
        assert!(!path.exists(), "{}", path.display());
    }
    assert!(git.requests().is_empty());
    assert_eq!(machine.machine.seams.count(GIT_SPAWN), 0);
    case.observe("both prepared with both repositories failed with cause clone, each with the refusal's message or the schema failure's description; the refused fetch was sent once; nothing was cloned and no git ran");
    case.pass();
}

#[test]
fn repositories_clone_check_out_and_are_reused_with_the_new_ticket_in_their_own_configuration_only()
{
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let mut case = record.case("a default branch, a branch, a commit and a missing branch, cloned");
    let machine = GitMachine::start(&mut case, &engine, &git, base);
    let global = machine.global_configuration();
    let remote: Remote = git.repository("alpha");
    let work = base.join("work");
    let [default, branch, commit, missing] =
        ["default", "branch", "commit", "missing"].map(|name| work.join(name));
    let mut repositories = json!([
        repository(&remote.url, &default, Value::Null),
        repository(
            &remote.url,
            &branch,
            json!({ "type": "branch", "name": "feature" })
        ),
        repository(
            &remote.url,
            &commit,
            json!({ "type": "commit", "sha": remote.first })
        ),
        repository(
            &remote.url,
            &missing,
            json!({ "type": "branch", "name": "absent" })
        ),
    ]);
    let trace = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let issued = engine.control(
        "prepare",
        json!({
            "instance_id": INSTANCE, "traceparent": trace,
            "payload": payload("sess_a", "bind_1", &work.join("cwd"), repositories.clone()),
        }),
    )["message_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let first = report(&engine, &issued);
    assert_eq!(failures(&first), [(text(&missing), "checkout".to_owned())]);
    let ticket = current_ticket(&engine, "sess_a");
    let at = |path: &Path, arguments: &[&str]| machine.in_repository(path, arguments);
    assert_eq!(at(&default, &["rev-parse", "HEAD"]), remote.main);
    assert_eq!(at(&default, &["branch", "--show-current"]), "main");
    assert_eq!(at(&branch, &["rev-parse", "HEAD"]), remote.feature);
    assert_eq!(at(&branch, &["branch", "--show-current"]), "feature");
    assert_eq!(at(&commit, &["rev-parse", "HEAD"]), remote.first);
    assert_eq!(at(&commit, &["branch", "--show-current"]), "");
    let proxy = git.url.as_str();
    for path in [&default, &branch, &commit, &missing] {
        assert_eq!(
            at(path, &["config", "--local", "remote.origin.url"]),
            remote.url
        );
        let local = |key: &str| at(path, &["config", "--local", "--get-all", key]);
        assert_eq!(
            local(&format!("url.{proxy}/github.com/.insteadOf")),
            "https://github.com/"
        );
        assert_eq!(
            local(&format!("http.{proxy}/.extraHeader")),
            format!("X-Tetral-Git-Ticket: {ticket}")
        );
        assert_eq!(local("user.name"), "Agent");
        assert_eq!(local("user.email"), "agent@example.com");
    }
    let requests = git.requests();
    assert!(!requests.is_empty());
    for request in &requests {
        assert_eq!(
            (request.ticket.as_deref(), request.status),
            (Some(ticket.as_str()), 200),
            "{}",
            request.path
        );
        assert!(request.path.starts_with("/github.com/acme/alpha/"));
    }
    assert_eq!(engine.requests("git-ticket").len(), 1);
    assert_eq!(engine.requests("git-ticket")[0]["traceparent"], trace);
    case.observe("the default branch, the branch and the detached commit are checked out, the missing branch failed with cause checkout; each repository's origin is its github.com URL and its local configuration holds the four keys; every request to the Git server carried the ticket in its header");
    case.pass();

    let mut case = record.case("an existing folder this Session did not clone");
    let existing = work.join("existing");
    fs::create_dir_all(&existing).unwrap();
    fs::write(existing.join("kept"), "foreign").unwrap();
    let fetches = engine.requests("git-ticket").len();
    let issued = prepare(
        &engine,
        &payload(
            "sess_b",
            "bind_1",
            &work.join("cwd_b"),
            json!([repository(&remote.url, &existing, Value::Null)]),
        ),
    );
    let failed = report(&engine, &issued);
    assert_eq!(
        (&failed["status"], &failed["error"]["code"]),
        (&json!("failed"), &json!("repository_path_exists"))
    );
    assert_eq!(fs::read_dir(&existing).unwrap().count(), 1);
    assert_eq!(fs::read(existing.join("kept")).unwrap(), b"foreign");
    assert_eq!(engine.requests("git-ticket").len(), fetches);
    case.observe("failed with repository_path_exists before any ticket fetch; the folder holds only its own file, unchanged");
    case.pass();

    let mut case =
        record.case("a second preparation of the cloned repositories with a new Git identity");
    for repository in repositories.as_array_mut().unwrap() {
        repository["git_identity"] = json!({"name":"Renewed Agent", "email":"renewed@example.org"});
    }
    // The local keys represent an earlier proxy endpoint. User values can
    // share its keys, and must survive the next preparation.
    let old_proxy = "https://retired.example:9443";
    for path in [&default, &branch, &commit, &missing] {
        for key in [
            format!("url.{proxy}/github.com/.insteadOf"),
            format!("http.{proxy}/.extraHeader"),
        ] {
            at(path, &["config", "--local", "--unset-all", &key]);
        }
        for (key, value) in [
            (
                format!("url.{old_proxy}/github.com/.insteadOf"),
                "https://github.com/".to_owned(),
            ),
            (
                format!("url.{old_proxy}/github.com/.insteadOf"),
                "https://user.example/".to_owned(),
            ),
            (
                format!("http.{old_proxy}/.extraHeader"),
                format!("X-Tetral-Git-Ticket: {ticket}"),
            ),
            (
                format!("http.{old_proxy}/.extraHeader"),
                "X-User: old-proxy".to_owned(),
            ),
            (
                format!("http.{proxy}/.extraHeader"),
                "X-User: current-proxy".to_owned(),
            ),
            (
                "url.https://unrelated.example/.insteadOf".to_owned(),
                "git@unrelated.example:".to_owned(),
            ),
        ] {
            at(path, &["config", "--local", "--add", &key, &value]);
        }
    }
    new_connection(&engine, &machine.machine);
    let served = git.requests().len();
    let issued = prepare(
        &engine,
        &payload("sess_a", "bind_2", &work.join("cwd"), repositories),
    );
    let second = report(&engine, &issued);
    assert_eq!(failures(&second), [], "{second:#}");
    let renewed = current_ticket(&engine, "sess_a");
    assert_ne!(renewed, ticket);
    assert_eq!(git.requests().len(), served);
    for path in [&default, &branch, &commit, &missing] {
        assert_eq!(
            at(
                path,
                &[
                    "config",
                    "--local",
                    "--get-all",
                    &format!("url.{old_proxy}/github.com/.insteadOf")
                ]
            ),
            "https://user.example/"
        );
        assert_eq!(
            at(
                path,
                &[
                    "config",
                    "--local",
                    "--get-all",
                    &format!("http.{old_proxy}/.extraHeader")
                ]
            ),
            "X-User: old-proxy"
        );
        assert_eq!(
            at(
                path,
                &[
                    "config",
                    "--local",
                    "--get-all",
                    "url.https://unrelated.example/.insteadOf"
                ]
            ),
            "git@unrelated.example:"
        );
        assert_eq!(
            at(path, &["config", "--local", "user.name"]),
            "Renewed Agent"
        );
        assert_eq!(
            at(path, &["config", "--local", "user.email"]),
            "renewed@example.org"
        );
        assert_eq!(
            at(
                path,
                &[
                    "config",
                    "--local",
                    "--get-all",
                    &format!("http.{proxy}/.extraHeader")
                ]
            ),
            format!("X-User: current-proxy\nX-Tetral-Git-Ticket: {renewed}")
        );
    }
    at(&branch, &["fetch", "--quiet", "origin"]);
    let last = git.requests().pop().unwrap();
    assert_eq!(
        (last.ticket.as_deref(), last.status),
        (Some(renewed.as_str()), 200)
    );
    assert_eq!(
        engine.requests("git-ticket").last().unwrap()["traceparent"],
        Value::Null
    );
    assert_eq!(machine.global_configuration(), global);
    case.observe("prepared without another clone; the independent failed checkout retained its existing reuse behavior and each repository uses the new identity and ticket, old proxy ownership is removed, user values sharing those keys survive and a git fetch reaches the current proxy; global Git configuration files are byte-identical");
    case.pass();
}

#[test]
fn each_failed_clone_carries_the_cause_its_git_error_names_and_a_bounded_message() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let mut case =
        record.case("clones answered with 401, 403, 404, 424, 500 and a 2,000-byte error line");
    let machine = GitMachine::start(&mut case, &engine, &git, base);
    let remote = git.repository("alpha");
    let work = base.join("work");
    // `fatal: remote error: ` and the message make a last line of 2,000
    // bytes.
    let long = format!(
        "{}界{}",
        "x".repeat(1_023 - "fatal: remote error: ".len()),
        "z".repeat(974)
    );
    let rows = [
        (Fault::Status(401), "authentication"),
        (Fault::Status(403), "forbidden"),
        (Fault::Status(404), "not_found"),
        (Fault::Status(424), "authentication"),
        (Fault::Status(500), "clone"),
        (Fault::RemoteError(long), "clone"),
    ];
    let mut repositories = Vec::new();
    let mut expected = Vec::new();
    for (index, (fault, cause)) in rows.into_iter().enumerate() {
        git.fault(fault);
        let path = work.join(format!("r{index}"));
        repositories.push(repository(&remote.url, &path, Value::Null));
        expected.push((text(&path), cause.to_owned()));
    }
    let issued = prepare(
        &engine,
        &payload("sess_a", "bind_1", &work.join("cwd"), json!(repositories)),
    );
    let report = report(&engine, &issued);
    assert_eq!(failures(&report), expected);
    let messages: Vec<&str> = report["repository_failures"]
        .as_array()
        .unwrap()
        .iter()
        .map(|failure| failure["message"].as_str().unwrap())
        .collect();
    assert_eq!(messages[5].len(), 1_023);
    assert_eq!(
        messages[5],
        format!(
            "fatal: remote error: {}",
            "x".repeat(1_023 - "fatal: remote error: ".len())
        )
    );
    assert!(messages[5].starts_with("fatal: remote error: x"));
    assert!(messages[..5].iter().all(|message| !message.is_empty()));
    let ticket = current_ticket(&engine, "sess_a");
    let log = fs::read_to_string(machine.machine.root.join("logs/tetral.log")).unwrap();
    assert!(!log.contains(&ticket));
    assert!(!report.to_string().contains(&ticket));
    case.observe(format!(
        "causes {:?}; the 2,000-byte line became a 1,024-byte message; the ticket is in neither the log nor the report",
        expected.iter().map(|(_, cause)| cause.as_str()).collect::<Vec<_>>(),
    ));
    case.pass();
}

#[test]
fn a_user_computer_preparation_fetches_no_ticket_and_spawns_no_git() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let engine = one_credential_engine();
    let mut case = record.case("a user computer's preparation");
    let machine = online_machine(&mut case, &engine, &base.join("machine"));
    let project = base.join("project");
    fs::create_dir_all(&project).unwrap();
    let issued = prepare(
        &engine,
        &json!({
            "session_id": "sess_a", "binding_id": "bind_1", "cwd": project,
            "drives": [], "drive_exclude": [], "repositories": [],
        }),
    );
    assert_eq!(report(&engine, &issued)["status"], "prepared");
    assert!(engine.requests("git-ticket").is_empty());
    assert_eq!(machine.seams.count(GIT_SPAWN), 0);
    assert!(listed(&machine, "sess_a").is_empty());
    case.observe("prepared with no git ticket fetch and git.spawn 0");
    case.pass();
}

#[test]
fn a_clone_held_past_its_connection_is_recorded_and_reused_by_the_next_preparation() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let mut case = record.case("a branch clone whose pack is held until the connection ended");
    let machine = GitMachine::start(&mut case, &engine, &git, base);
    let remote = git.repository("alpha");
    let work = base.join("work");
    let path = work.join("held");
    let second = git.repository("second");
    let second_path = work.join("never_started");
    let mut repositories = json!([
        repository(
            &remote.url,
            &path,
            json!({ "type": "branch", "name": "feature" })
        ),
        repository(&second.url, &second_path, Value::Null)
    ]);
    git.hold_pack();
    let issued = prepare(
        &engine,
        &payload("sess_a", "bind_1", &work.join("cwd"), repositories.clone()),
    );
    git.wait_until_held();
    new_connection(&engine, &machine.machine);
    let early = prepare(
        &engine,
        &payload("sess_a", "bind_2", &work.join("cwd"), repositories.clone()),
    );
    assert_eq!(outcome(&engine, &early), failed("cleanup_pending"));
    git.release();
    machine
        .machine
        .seams
        .wait_for_count("repository.completed", 1);
    assert_eq!(listed(&machine.machine, "sess_a"), [text(&path)]);
    assert!(path.is_dir());
    assert!(!second_path.exists());
    assert!(
        git.requests()
            .iter()
            .all(|request| !request.path.contains("/second/"))
    );
    assert!(reports_for(&engine, &issued).is_empty());
    assert_eq!(
        machine.in_repository(&path, &["branch", "--show-current"]),
        "feature"
    );
    assert_eq!(
        machine.in_repository(&path, &["rev-parse", "HEAD"]),
        remote.feature
    );
    repositories.as_array_mut().unwrap().pop();
    let served = git.requests().len();
    let issued = prepare(
        &engine,
        &payload("sess_a", "bind_3", &work.join("cwd"), repositories.clone()),
    );
    assert_eq!(failures(&report(&engine, &issued)), []);
    assert_eq!(git.requests().len(), served);
    case.observe("a preparation while the clone still ran failed with cleanup_pending; after the next connection the first clone is listed with feature checked out, the second repository never starts; the next preparation reused the first without a clone or repository_path_exists");
    case.pass();

    let mut case = record.case("a listed clone that was deleted");
    fs::remove_dir_all(&path).unwrap();
    // The second reconnect's backoff base is 2 seconds.
    new_connection_after(&engine, &machine.machine, 2.4);
    let issued = prepare(
        &engine,
        &payload("sess_a", "bind_4", &work.join("cwd"), repositories),
    );
    assert_eq!(failures(&report(&engine, &issued)), []);
    assert!(git.requests().len() > served);
    assert_eq!(
        machine.in_repository(&path, &["branch", "--show-current"]),
        "feature"
    );
    case.observe("the next preparation cloned it again");
    case.pass();

    let mut case = record.case("close deletes the recorded clone and retries its report");
    assert!(path.join(".git").is_dir());
    let attempts = engine.requests("session-status").len();
    let answered = machine.machine.seams.count("outbox.answered");
    answer_fault(
        &engine,
        json!({"endpoint":"session-status", "count":1, "action":"drop"}),
    );
    let issued = engine.control(
        "close",
        json!({"instance_id":INSTANCE,
        "payload":{"session_id":"sess_a","binding_id":"bind_4"}}),
    );
    let closing = issued["message_id"].as_str().unwrap();
    wait_until("the first closed report attempt", || {
        engine.requests("session-status").len() == attempts + 1
    });
    machine
        .machine
        .seams
        .wait_for_count("outbox.answered", answered + 1);
    assert!(
        !path.exists(),
        "the clone must be deleted before reporting closed"
    );
    assert!(!machine.machine.root.join("sessions/sess_a").exists());
    machine
        .machine
        .seams
        .advance(std::time::Duration::from_secs(2));
    wait_until("the closed report retry", || {
        engine.requests("session-status").len() == attempts + 2
    });
    assert_eq!(outcome(&engine, closing), closed());
    let requests = engine.requests("session-status");
    assert_eq!(requests[attempts]["body"], requests[attempts + 1]["body"]);
    case.observe("the recorded clone and Session directory were absent before the first closed report; a dropped response caused an identical report retry");
    case.pass();
}

#[test]
fn a_late_clone_whose_checkout_fails_leaves_no_folder_and_a_close_kills_a_running_clone() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let mut case = record.case("a clone of a missing branch held until the connection ended");
    let machine = GitMachine::start(&mut case, &engine, &git, base);
    let remote = git.repository("alpha");
    let work = base.join("work");
    let cwd = work.join("cwd");
    let late = work.join("late");
    git.hold_pack();
    prepare(
        &engine,
        &payload(
            "sess_a",
            "bind_1",
            &cwd,
            json!([repository(
                &remote.url,
                &late,
                json!({ "type": "branch", "name": "missing" })
            )]),
        ),
    );
    git.wait_until_held();
    new_connection(&engine, &machine.machine);
    git.release();
    // The Session prepares again once the late clone has ended.
    let mut attempt = 1;
    let prepared = loop {
        attempt += 1;
        let issued = prepare(
            &engine,
            &payload("sess_a", &format!("bind_{attempt}"), &cwd, json!([])),
        );
        let answer = outcome(&engine, &issued);
        if answer != failed("cleanup_pending") || attempt == 50 {
            break answer;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    };
    assert_eq!(prepared.0, "prepared");
    assert!(!late.exists());
    assert!(listed(&machine.machine, "sess_a").is_empty());
    case.observe("the clone finished after its connection, its checkout failed and its folder was removed unlisted");
    case.pass();

    let mut case = record.case("a session.close while the first of two clones is held");
    git.hold_pack();
    prepare(
        &engine,
        &payload(
            "sess_b",
            "bind_1",
            &cwd,
            json!([
                repository(&remote.url, &work.join("first"), Value::Null),
                repository(&remote.url, &work.join("second"), Value::Null),
            ]),
        ),
    );
    git.wait_until_held();
    let spawned = machine.machine.seams.count(GIT_SPAWN);
    let issued = engine.control(
        "close",
        json!({
            "instance_id": INSTANCE,
            "payload": { "session_id": "sess_b", "binding_id": "bind_1" },
        }),
    );
    let closing = issued["message_id"].as_str().unwrap().to_owned();
    assert_eq!(outcome(&engine, &closing), closed());
    git.release();
    assert_eq!(machine.machine.seams.count(GIT_SPAWN), spawned);
    assert!(!work.join("second").exists());
    case.observe("closed was reported while the clone's pack was still held; no git was spawned after the close and the second repository was never cloned");
    case.pass();

    let mut case = record.case("a repository path equal to a cwd that does not exist yet");
    let together = work.join("together");
    let issued = prepare(
        &engine,
        &payload(
            "sess_c",
            "bind_1",
            &together,
            json!([repository(&remote.url, &together, Value::Null)]),
        ),
    );
    assert_eq!(failures(&report(&engine, &issued)), []);
    assert_eq!(
        machine.in_repository(&together, &["rev-parse", "HEAD"]),
        remote.main
    );
    case.observe("prepared with the repository published before the final cwd probe");
    case.pass();
}

#[test]
fn resources_arrive_before_cwd_and_a_failed_required_checkout_never_becomes_reusable() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let mut case =
        record.case("cwd inside a clone, then repeated dependent checkout and ticket failures");
    let machine = GitMachine::start(&mut case, &engine, &git, base);
    let remote = git.repository("ordered");
    let target = base.join("work/repository");
    let cwd = target.join("nested");
    git.hold_pack();
    let issued = prepare(
        &engine,
        &payload(
            "sess_order",
            "bind_1",
            &cwd,
            json!([repository(&remote.url, &target, Value::Null)]),
        ),
    );
    wait_until("held pack or premature preparation result", || {
        git.pack_held() || !reports_for(&engine, &issued).is_empty()
    });
    assert!(
        reports_for(&engine, &issued).is_empty(),
        "preparation ended before resources arrived: {:?}",
        reports_for(&engine, &issued)
    );
    assert!(
        !target.exists(),
        "validation must not create clone target or cwd"
    );
    assert!(listed(&machine.machine, "sess_order").is_empty());
    git.release();
    let prepared = report(&engine, &issued);
    assert_eq!(failures(&prepared), []);
    assert_eq!(prepared["cwd"], text(&cwd.canonicalize().unwrap()));
    assert_eq!(fs::read(cwd.join("content")).unwrap(), b"from clone");

    let failed_target = base.join("work/failed");
    for (binding, branch) in [
        ("bind_missing_1", "absent"),
        ("bind_missing_2", "absent"),
        ("bind_file", "first"),
    ] {
        let issued = prepare(
            &engine,
            &payload(
                "sess_failed",
                binding,
                &failed_target,
                json!([repository(
                    &remote.url,
                    &failed_target,
                    json!({"type":"branch", "name":branch})
                )]),
            ),
        );
        let failure = report(&engine, &issued);
        assert_eq!(failure["status"], "failed", "{failure}");
        assert_eq!(failure["error"]["code"], "repository_preparation_failed");
        let message = failure["error"]["message"].as_str().unwrap();
        assert!(
            message.contains(failed_target.to_str().unwrap()) && message.contains("checkout"),
            "{message}"
        );
        assert!(
            message.contains(if branch == "absent" {
                "absent"
            } else {
                "first"
            }),
            "the Git failure cause is retained: {message}"
        );
        assert!(!failed_target.exists());
        assert!(listed(&machine.machine, "sess_failed").is_empty());
        assert!(
            fs::read_dir(failed_target.parent().unwrap())
                .unwrap()
                .all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".tetral-clone-")
                })
        );
    }
    // An escaped control character takes six bytes of the message and one of
    // the path, so the message passes its 1,024 bytes while the path and Git's
    // files under it stay within macOS's 1,024-byte `PATH_MAX`.
    let mut diagnostic_target =
        base.join(format!("work/failed\n\u{1b}[31m{}", "\u{1b}".repeat(40)));
    for _ in 0..5 {
        diagnostic_target.push("界".repeat(50));
    }
    let issued = prepare(
        &engine,
        &payload(
            "sess_diagnostic",
            "bind_diagnostic",
            &diagnostic_target,
            json!([repository(
                &remote.url,
                &diagnostic_target,
                json!({"type":"branch", "name":"absent"})
            )]),
        ),
    );
    let diagnostic = report(&engine, &issued);
    assert_eq!(diagnostic["error"]["code"], "repository_preparation_failed");
    let message = diagnostic["error"]["message"].as_str().unwrap();
    assert!(message.contains("checkout"), "{message}");
    assert!(message.contains("failed\\n\\u{1b}[31m"), "{message}");
    assert!(
        message.contains('界'),
        "the retained path stays valid UTF-8"
    );
    // Cut at 1,024 bytes, back to the start of a three-byte character.
    assert!(
        (1_022..=1_024).contains(&message.len()),
        "{} bytes",
        message.len()
    );
    assert!(!message.chars().any(char::is_control));
    assert!(!diagnostic_target.exists());
    answer_fault(
        &engine,
        json!({"endpoint":"git-ticket", "action":"error", "code":"unknown_instruction"}),
    );
    let issued = prepare(
        &engine,
        &payload(
            "sess_ticket",
            "bind_ticket",
            &failed_target,
            json!([repository(&remote.url, &failed_target, Value::Null)]),
        ),
    );
    assert_eq!(
        report(&engine, &issued)["error"]["code"],
        "repository_preparation_failed"
    );
    assert!(!failed_target.exists());
    case.observe("the held clone left cwd absent; its tracked subdirectory became cwd only after publication; both missing-branch attempts and the dependent ticket failure decoded as repository_preparation_failed without a reusable clone or staging residue");
    case.pass();
}

#[test]
fn preparation_holds_idle_until_actual_git_completion_but_not_report_ack() {
    use std::time::Duration;
    use tetral_tests::machine::wait_for_received;
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    for end_connection in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path();
        let (git, engine) = servers(base);
        let mut case = record.case(&format!(
            "held clone and withheld ACKs; connection ended: {end_connection}"
        ));
        let machine =
            GitMachine::start_with_args(&mut case, &engine, &git, base, &["--idle-exit", "1m"]);
        let seams = &machine.machine.seams;
        seams.hold("idle.uploading");
        let remote = git.repository("busy");
        let target = base.join("work/clone");
        let request = payload(
            "sess_busy",
            "bind_busy",
            &target,
            json!([repository(&remote.url, &target, Value::Null)]),
        );
        git.hold_pack();
        let first = prepare(&engine, &request);
        git.wait_until_held();
        let duplicate = prepare(&engine, &request);
        let other = prepare(
            &engine,
            &payload("sess_other", "bind_other", base, json!([])),
        );
        assert_eq!(report(&engine, &other)["status"], "prepared");
        let advance = |count| {
            for _ in 0..count {
                let beats = engine.received("heartbeat").len();
                let grants = seams.count("connection.granted");
                engine.advance(Duration::from_secs(15));
                wait_for_received(&engine, "heartbeat", beats + 1);
                seams.wait_for_count("connection.granted", grants + 1);
                seams.advance(Duration::from_secs(15));
            }
        };
        if end_connection {
            new_connection(&engine, &machine.machine);
        }
        advance(5);
        assert_eq!(machine.machine.status()["server_state"], "running");
        assert_eq!(seams.waiting("idle.uploading"), 0);
        answer_fault(
            &engine,
            json!({"endpoint":"session-status", "count":100, "action":"withhold"}),
        );
        let checked = seams.count("idle.checked");
        git.release();
        if end_connection {
            seams.wait_for_count("repository.completed", 1);
            assert!(reports_for(&engine, &first).is_empty());
            assert!(reports_for(&engine, &duplicate).is_empty());
            assert!(target.join(".git").is_dir());
        } else {
            assert_eq!(report(&engine, &first)["status"], "prepared");
            assert_eq!(report(&engine, &duplicate)["status"], "prepared");
        }
        seams.wait_for_count("idle.checked", checked + 1);
        if !end_connection {
            assert_eq!(
                tetral_tests::sessions::state_of(&machine.machine, "sess_busy"),
                "prepared_pending_ack"
            );
        }
        advance(3);
        assert_eq!(seams.waiting("idle.uploading"), 0);
        advance(1);
        seams.wait_until_waiting("idle.uploading", 1);
        if !end_connection {
            assert_eq!(
                tetral_tests::sessions::state_of(&machine.machine, "sess_busy"),
                "prepared_pending_ack"
            );
        }
        seams.release("idle.uploading");
        let (run, _) = machine.machine.app_server.exited();
        assert_eq!(run.status.code(), Some(0), "{}", run.exit());
        case.observe(format!("the held clone kept idle exit inactive past 75 seconds with connection ended={end_connection}; actual Git completion started a new full idle minute, independently of report ACKs"));
        case.pass();
    }
}

#[test]
fn git_low_speed_window_overrides_inherited_settings_without_a_total_clone_deadline() {
    use std::time::{Duration, Instant};
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let mut case = record
        .case("real paced pack beyond the constructed low-speed window and a stalled response");
    assert_eq!(
        session::Times::default().git_low_speed_window,
        Duration::from_secs(60)
    );
    let window = Duration::from_secs(2);
    let machine = GitMachine::start_with_inheritance(
        &mut case,
        &engine,
        &git,
        base,
        &[],
        false,
        Some(window.as_secs()),
    );
    let remote = git.repository("paced");
    let wait = |message: &str| {
        let until = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(report) = reports_for(&engine, message).into_iter().next() {
                break report;
            }
            assert!(
                Instant::now() < until,
                "repository report did not arrive within 20 seconds"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    git.pace_pack(Duration::from_millis(100));
    let target = base.join("paced-target");
    let started = Instant::now();
    let issued = prepare(
        &engine,
        &payload(
            "sess_paced",
            "bind_paced",
            &target,
            json!([repository(&remote.url, &target, Value::Null)]),
        ),
    );
    assert_eq!(failures(&wait(&issued)), []);
    assert!(started.elapsed() > window);
    assert_eq!(
        machine.in_repository(&target, &["rev-parse", "HEAD"]),
        remote.main
    );
    git.hold_pack();
    let stalled = base.join("stalled-target");
    let started = Instant::now();
    let issued = prepare(
        &engine,
        &payload(
            "sess_stalled",
            "bind_stalled",
            base,
            json!([repository(&remote.url, &stalled, Value::Null)]),
        ),
    );
    git.wait_until_held();
    let failure = wait(&issued);
    let elapsed = started.elapsed();
    assert_eq!(failures(&failure), [(text(&stalled), "clone".to_owned())]);
    assert!(
        (window..Duration::from_secs(12)).contains(&elapsed),
        "{elapsed:?}"
    );
    assert!(!stalled.exists());
    git.release();
    let log = fs::read_to_string(machine.machine.root.join("logs/tetral.log")).unwrap();
    let progress: Vec<Value> = log
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|entry| entry["event"] == "repository.progress")
        .collect();
    assert!(!progress.is_empty(), "no parsed progress records: {log}");
    for entry in progress {
        assert_eq!(entry["session.id"], "sess_paced", "{entry}");
        assert_eq!(entry["binding.id"], "bind_paced", "{entry}");
        assert!(entry["repository.completed"].is_u64(), "{entry}");
        assert!(entry["repository.total"].is_u64(), "{entry}");
        assert!(entry["elapsed_ms"].is_u64(), "{entry}");
    }
    assert!(!log.contains(&current_ticket(&engine, "sess_paced")));
    assert!(!log.contains(KEY));
    case.observe(format!("a continuously transferring clone exceeded its constructed {window:?} low-speed window despite inherited 1-second/high-speed settings; a held response failed after {elapsed:?}; parsed stage/count/elapsed records contained no credential"));
    case.pass();
}

/// Executes a genuine admitted tool on the existing repository binding.
#[cfg(target_os = "linux")]
fn configure_with_tool(engine: &ReferenceServer, script: &str) {
    engine.control("execute", json!({
        "instance_id": INSTANCE,
        "payload": {
            "session_id":"sess_hooks", "binding_id":"bind_first",
            "tool_use_event_id":"call_configuration", "task_id":"task_configuration",
            "tool":"exec_command", "input":{"cmd":script,"wait_ms":0},
            "sandbox_policy":{"schema_version":1,"filesystem":{"rules":[{"path":"/","access":"read_write"}]},"network":{"mode":"direct"}},
        },
    }));
    let mut terminal = Value::Null;
    wait_until("the configuration tool's terminal report", || {
        terminal = engine
            .received("tool-status")
            .into_iter()
            .map(|row| row["body"].clone())
            .find(|body| body["task_id"] == "task_configuration" && body["phase"] == "terminal")
            .unwrap_or(Value::Null);
        !terminal.is_null()
    });
    assert_eq!(terminal["result"]["result"]["exit_code"], 0, "{terminal}");
}

// Reads the descriptors of the App Server and of the hooks through `/proc`.
#[cfg(target_os = "linux")]
#[test]
fn tool_written_git_hooks_filters_and_helpers_keep_ordinary_access_and_deny_private_state() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a tool configures Git and a later real preparation runs restricted hooks, filters and credential helpers");
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let machine =
        GitMachine::start_with_inheritance(&mut case, &engine, &git, base, &[], true, None);
    let remote = git.repository("configured");
    let target = base.join("repository");
    let repos = json!([repository(
        &remote.url,
        &target,
        json!({"type":"branch","name":"feature"})
    )]);
    let issued = prepare(
        &engine,
        &payload("sess_hooks", "bind_first", base, repos.clone()),
    );
    assert_eq!(failures(&report(&engine, &issued)), []);
    let root = &machine.machine.root;
    let inherited = root.join("credentials/inherited-probe");
    assert_eq!(fs::read_to_string(&inherited).unwrap(), "dummy-descriptor");
    assert_eq!(
        fs::read_link(format!("/proc/{}/fd/9", machine.machine.app_server.pid())).unwrap(),
        inherited
    );
    let ordinary = root.join("ordinary-state");
    fs::write(&ordinary, "readable-state").unwrap();
    let temporary_credential = root.join("credentials/replacement.tmp");
    fs::write(&temporary_credential, "replacement-secret").unwrap();
    let run_private = root.join("run/private-proof");
    fs::write(&run_private, "private-control").unwrap();
    let task_private = execution::store::task_parent(root).join("other-task");
    state::files::create_private_dirs(&task_private).unwrap();
    fs::write(task_private.join("secret"), "private-task").unwrap();
    let output = base.join("observed");
    let probe = base.join("probe.py");
    let settings = base.join("probe.json");
    let hooks = base.join("hooks");
    let config = json!({
        "ordinary":ordinary, "credentials":[root.join("credentials/credentials.json"),temporary_credential,run_private,task_private.join("secret"),inherited],
        "socket":machine.machine.app_server.socket,
        "trusted":tetral_tests::binaries::tetral(), "output":output,
        "late":root.join("credentials/late-replacement.tmp"), "release":base.join("release-hook"),
    });
    let probe_text = r#"import json, os, pathlib, socket, subprocess, sys
cfg = json.loads(pathlib.Path(__file__).with_suffix('.json').read_text())
label = sys.argv[1]
import traceback
pathlib.Path(cfg['output']).mkdir(exist_ok=True)
def failed(kind, value, tb):
    pathlib.Path(cfg['output'], label + '-error').write_text(''.join(traceback.format_exception(kind, value, tb)))
sys.excepthook = failed
assert pathlib.Path(cfg['ordinary']).read_text() == 'readable-state'
for path in cfg['credentials']:
    try:
        with open(path, 'rb') as stream: stream.read(1)
    except OSError:
        pass
    else:
        raise AssertionError('private file readable: ' + path)
for path in [cfg['ordinary'], cfg['trusted']]:
    try:
        with open(path, 'r+b'): pass
    except OSError:
        pass
    else:
        raise AssertionError('protected file writable: ' + path)
connection = socket.socket(socket.AF_UNIX)
try:
    connection.connect(cfg['socket'])
except OSError:
    pass
else:
    raise AssertionError('private control socket reachable')
finally:
    connection.close()
assert not any(name.startswith('TETRAL_') for name in os.environ)
for number in range(3, 64):
    try: os.fstat(number)
    except OSError: continue
    target = os.readlink('/proc/self/fd/' + str(number))
    assert 'tetral-task-environment' not in target, target
    assert target not in cfg['credentials'], target
    assert target != cfg['socket'], target
if label == 'descendant':
    import time
    pathlib.Path(cfg['output'], 'waiting').write_text('ready')
    while not pathlib.Path(cfg['release']).exists(): time.sleep(0.01)
    for path in [cfg['late'], cfg['credentials'][0]]:
        try: pathlib.Path(path).read_bytes()
        except OSError: pass
        else: raise AssertionError('replacement credential readable: ' + path)
if label == 'hook':
    pathlib.Path('.git/preparation-proof').write_text('metadata remains writable')
    subprocess.run([sys.executable, __file__, 'descendant'], check=True)
pathlib.Path(cfg['output']).mkdir(exist_ok=True)
pathlib.Path(cfg['output'], label).write_text('passed')
"#;
    let hook = format!("#!/bin/sh\nexec python3 '{}' hook\n", probe.display());
    let attributes = base.join("attributes");
    let gitconfig = format!(
        "[core]\n hooksPath = {}\n attributesFile = {}\n[filter \"checked\"]\n smudge = python3 {} filter && cat\n clean = cat\n required = true\n[credential]\n helper = !python3 {} helper\n",
        hooks.display(),
        attributes.display(),
        probe.display(),
        probe.display()
    );
    let files = json!({
        probe.to_str().unwrap(): probe_text,
        settings.to_str().unwrap(): config.to_string(),
        hooks.join("post-checkout").to_str().unwrap(): hook,
        attributes.to_str().unwrap(): "* filter=checked\n",
        base.join("home/.gitconfig").to_str().unwrap():gitconfig,
    });
    let install = format!(
        "python3 - <<'PYINSTALL'\nimport json, pathlib\nfiles=json.loads({})\nfor name, text in files.items():\n p=pathlib.Path(name); p.parent.mkdir(parents=True,exist_ok=True); p.write_text(text)\npathlib.Path({}).chmod(0o755)\nPYINSTALL",
        serde_json::to_string(&files.to_string()).unwrap(),
        serde_json::to_string(&hooks.join("post-checkout")).unwrap()
    );
    configure_with_tool(&engine, &install);
    assert!(probe.exists());
    // The owned clone disappeared between preparations; ordinary reuse
    // alone does not execute clone/checkout hooks.
    fs::remove_dir_all(&target).unwrap();
    new_connection(&engine, &machine.machine);
    let issued = prepare(&engine, &payload("sess_hooks", "bind_second", base, repos));
    wait_until("the live hook descendant or a failed clone report", || {
        output.join("waiting").exists() || !reports_for(&engine, &issued).is_empty()
    });
    if output.join("waiting").exists() {
        let credential = root.join("credentials/credentials.json");
        let bytes = fs::read(&credential).unwrap();
        fs::write(
            root.join("credentials/late-replacement.tmp"),
            "later-private-bytes",
        )
        .unwrap();
        state::files::replace(&credential, &bytes).unwrap();
        assert_eq!(fs::read(&credential).unwrap(), bytes);
        fs::write(base.join("release-hook"), "continue").unwrap();
    }
    let answer = report(&engine, &issued);
    let diagnostics: Vec<_> = fs::read_dir(&output)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| {
            (
                entry.file_name(),
                fs::read_to_string(entry.path()).unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(failures(&answer), [], "{answer}; {diagnostics:?}");
    for label in ["hook", "descendant", "filter"] {
        assert_eq!(fs::read_to_string(output.join(label)).unwrap(), "passed");
    }
    assert_eq!(
        fs::read_to_string(target.join(".git/preparation-proof")).unwrap(),
        "metadata remains writable"
    );
    git.fault(Fault::Status(401));
    let helper_target = base.join("helper-repository");
    let issued = prepare(
        &engine,
        &payload(
            "sess_helper",
            "bind_helper",
            base,
            json!([repository(&remote.url, &helper_target, Value::Null)]),
        ),
    );
    let answer = report(&engine, &issued);
    assert_eq!(
        failures(&answer),
        [(text(&helper_target), "authentication".into())],
        "{answer}"
    );
    assert_eq!(fs::read_to_string(output.join("helper")).unwrap(), "passed");
    assert_eq!(fs::read_to_string(ordinary).unwrap(), "readable-state");
    case.observe("an admitted tool installed ordinary global Git configuration; a later clone ran the hook, its child and a required filter, and a 401 invoked the credential helper. All read ordinary state and wrote ordinary files; every private read/control connection/protected write was refused, command environment and inherited descriptors were clean, and checkout Git metadata remained writable");
    case.pass();
}

#[cfg(target_os = "linux")]
#[test]
fn preparation_shutdown_and_parent_death_end_the_entire_git_namespace() {
    use std::os::unix::fs::PermissionsExt;
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    for ending in ["close", "stop", "parent_death"] {
        let mut case = record.case(&format!("{ending} ends Git and a detached hook descendant"));
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path();
        let (git, engine) = servers(base);
        let machine = GitMachine::start(&mut case, &engine, &git, base);
        let remote = git.repository("hanging");
        let hooks = base.join("hooks");
        fs::create_dir(&hooks).unwrap();
        let observed = base.join("namespace.json");
        let child_ready = base.join("child-ready");
        let hook = format!(
            r#"#!/usr/bin/python3
import json, os, pathlib, signal, subprocess, sys, time
child = subprocess.Popen([sys.executable, '-c', 'import pathlib,signal; signal.signal(signal.SIGTERM,signal.SIG_IGN); pathlib.Path({child_ready:?}).write_text("ready"); signal.pause()'], start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
while not pathlib.Path({child_ready:?}).exists(): time.sleep(0.01)
# Renamed into place, so the test never reads a partial file.
observed = pathlib.Path({observed:?})
partial = observed.with_name(observed.name + '.partial')
partial.write_text(json.dumps({{'namespace':os.readlink('/proc/self/ns/pid'), 'scratch':os.getcwd()}}))
os.replace(partial, observed)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child.wait()
"#,
            child_ready = text(&child_ready),
            observed = text(&observed)
        );
        fs::write(hooks.join("post-checkout"), hook).unwrap();
        fs::set_permissions(
            hooks.join("post-checkout"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::write(
            base.join("home/.gitconfig"),
            format!("[core]\n hooksPath = {}\n", hooks.display()),
        )
        .unwrap();
        let target = base.join("repository");
        let second = base.join("second");
        prepare(
            &engine,
            &payload(
                "sess_tree",
                "bind_tree",
                base,
                json!([
                    repository(&remote.url, &target, Value::Null),
                    repository(&remote.url, &second, Value::Null),
                ]),
            ),
        );
        wait_until("the running hook and detached child", || observed.exists());
        let observation: Value = serde_json::from_slice(&fs::read(&observed).unwrap()).unwrap();
        let namespace = observation["namespace"].as_str().unwrap();
        let processes = || -> Vec<PathBuf> {
            fs::read_dir("/proc")
                .unwrap()
                .flatten()
                .filter_map(|entry| {
                    let path = entry.path().join("ns/pid");
                    (fs::read_link(&path).ok().as_deref() == Some(Path::new(namespace)))
                        .then_some(path)
                })
                .collect()
        };
        let running = processes();
        assert!(
            running.len() >= 3,
            "the namespace must contain Git and its descendants: {running:?}"
        );
        // Hold its identity until the final absence check, excluding reuse.
        let _namespace = fs::File::open(&running[0]).unwrap();
        assert_ne!(
            fs::read_link("/proc/self/ns/pid").unwrap(),
            Path::new(namespace)
        );
        let scratch = PathBuf::from(observation["scratch"].as_str().unwrap());
        assert!(scratch.join(".git").exists());
        assert!(!target.exists());
        match ending {
            "close" => {
                let issued = engine.control("close", json!({"instance_id":INSTANCE, "payload":{"session_id":"sess_tree","binding_id":"bind_tree"}}));
                assert_eq!(
                    outcome(&engine, issued["message_id"].as_str().unwrap()),
                    closed()
                );
                assert!(!scratch.exists());
                assert!(
                    processes().is_empty(),
                    "close reported before Git descendants ended"
                );
            }
            "stop" => {
                let stop = tetral_tests::run::Tetral::new([
                    "server",
                    "stop",
                    "--app",
                    tetral_tests::machine::APP,
                ])
                .home(&machine.machine.root)
                .run();
                assert!(stop.status.success(), "{}", stop.exit());
                let (run, _) = machine.machine.app_server.exited();
                assert!(run.status.success(), "{}", run.exit());
                assert!(!scratch.exists());
                assert!(
                    processes().is_empty(),
                    "server exit preceded descendant cleanup"
                );
            }
            "parent_death" => {
                machine.machine.app_server.kill();
                wait_until("all Git descendants after parent death", || {
                    processes().is_empty()
                });
            }
            _ => unreachable!(),
        }
        assert!(!target.exists());
        assert!(!second.exists());
        let task_parent =
            execution::store::task_parent(&machine.machine.root.canonicalize().unwrap());
        assert!(
            task_parent.is_dir(),
            "{ending} left the task parent as a non-directory: {}",
            task_parent.display()
        );
        case.observe("a real Git hook started a detached child that ignored SIGTERM; the complete PID namespace disappeared, the next repository never started, and ordinary close/stop removed owned staging before reporting completion");
        case.pass();
    }
}

#[test]
fn restart_after_recording_a_clone_replaces_only_its_reserved_staging() {
    use std::time::Duration;
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("parent death between repository record and publication, followed by retry");
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let machine = GitMachine::start(&mut case, &engine, &git, base);
    let remote = git.repository("recorded");
    let target = base.join("repository");
    let repositories = json!([repository(
        &remote.url,
        &target,
        json!({"type":"branch","name":"feature"})
    )]);
    machine.machine.seams.hold("repository.recorded");
    prepare(
        &engine,
        &payload("sess_recorded", "bind_first", base, repositories.clone()),
    );
    machine
        .machine
        .seams
        .wait_until_waiting("repository.recorded", 1);
    assert_eq!(listed(&machine.machine, "sess_recorded"), [text(&target)]);
    assert!(!target.exists());
    let scratch = fs::read_dir(base)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".tetral-clone-sess_recorded-")
        })
        .unwrap();
    let sibling = base.join(".tetral-clone-sess_recorded-unrelated");
    fs::create_dir(&sibling).unwrap();
    fs::write(sibling.join("keep"), "not owned staging").unwrap();
    fs::write(scratch.join("orphan"), "old staging").unwrap();
    let GitMachine {
        machine,
        environment,
    } = machine;
    let Machine {
        app_server,
        seams,
        root,
    } = machine;
    app_server.kill();
    seams.release("repository.recorded");
    engine.advance(Duration::from_secs(46));
    let mut command =
        tetral_tests::run::Tetral::new(["server", "run", "--app", tetral_tests::machine::APP])
            .home(&root)
            .seams(&seams)
            .current_dir(&tetral_tests::workspace_dir().join("tests"));
    for (key, value) in &environment {
        command = command.env(key, value);
    }
    let app_server = tetral_tests::app_server::AppServer::start(command, &root);
    let restarted = GitMachine {
        machine: Machine {
            app_server,
            seams,
            root,
        },
        environment,
    };
    restarted.machine.wait_for("online");
    let issued = prepare(
        &engine,
        &payload("sess_recorded", "bind_retry", base, repositories),
    );
    assert_eq!(failures(&report(&engine, &issued)), []);
    assert_eq!(
        restarted.in_repository(&target, &["rev-parse", "HEAD"]),
        remote.feature
    );
    assert!(!target.join("orphan").exists());
    assert!(!scratch.exists());
    assert_eq!(
        fs::read_to_string(sibling.join("keep")).unwrap(),
        "not owned staging"
    );
    assert_eq!(listed(&restarted.machine, "sess_recorded"), [text(&target)]);
    case.observe("the committed record survived an absent final path; restart removed the exact stale staging path, cloned and checked out again, published one owned target, and retained a similarly named unowned sibling");
    case.pass();
}

// macOS isolates with the system's `/usr/bin/sandbox-exec`, which no `PATH`
// hides.
#[cfg(target_os = "linux")]
#[test]
fn unavailable_preparation_isolation_never_falls_back_to_host_git() {
    use std::os::unix::fs::PermissionsExt;
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("missing isolation refuses preparation before a host Git wrapper can run");
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let bin = base.join("bin");
    fs::create_dir(&bin).unwrap();
    let sentinel = base.join("unrestricted-git-started");
    let wrapper = bin.join("git");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\necho unsafe > '{}'\nexit 1\n",
            sentinel.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let machine = Machine::start_with(
        &mut case,
        &base.join("machine"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        base,
        &[("PATH", text(&bin))],
    );
    machine.wait_for("online");
    let target = base.join("repository");
    let remote = git.repository("unavailable");
    let issued = prepare(
        &engine,
        &payload(
            "sess_unavailable",
            "bind_unavailable",
            base,
            json!([repository(&remote.url, &target, Value::Null)]),
        ),
    );
    let answer = report(&engine, &issued);
    assert_eq!(failures(&answer), [(text(&target), "clone".into())]);
    assert!(
        answer["repository_failures"][0]["message"]
            .as_str()
            .unwrap()
            .contains("preparation isolation unavailable"),
        "{answer}"
    );
    assert!(!sentinel.exists());
    assert!(!target.exists());
    assert_eq!(machine.seams.count(GIT_SPAWN), 0);
    case.observe("the valid ticket reached preparation, but missing bwrap produced an actionable clone failure; neither the host wrapper nor an unrestricted Git fallback ran");
    case.pass();
}

/// Detach this fixture's mounts before the App Server is dropped, including
/// after an assertion failure while an initial namespace request is waiting.
#[cfg(target_os = "linux")]
struct MountedDrives(PathBuf);

#[cfg(target_os = "linux")]
impl Drop for MountedDrives {
    fn drop(&mut self) {
        let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        for point in mounts.lines().filter_map(|line| line.split(' ').nth(4)) {
            if Path::new(point).starts_with(&self.0) {
                let _ = Command::new("fusermount3")
                    .args(["-u", "-z", point])
                    .status();
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn preparation_hooks_respect_writable_read_only_and_failed_drive_mounts() {
    use std::os::unix::fs::PermissionsExt;
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("Git hooks use only the current healthy writable Drive grants");
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path();
    let (git, engine) = servers(base);
    let machine = GitMachine::start(&mut case, &engine, &git, base);
    let _mounts = MountedDrives(machine.machine.root.clone());
    answer_fault(
        &engine,
        json!({"endpoint":"session-status", "count":100, "action":"withhold"}),
    );
    let remote = git.repository("drives");
    let hooks = base.join("hooks");
    fs::create_dir(&hooks).unwrap();
    fs::write(
        base.join("home/.gitconfig"),
        format!("[core]\n hooksPath = {}\n", hooks.display()),
    )
    .unwrap();
    for phase in ["fresh", "retained", "failed"] {
        let failed = phase == "failed";
        let writable_ready = phase == "retained";
        let session_id = format!("sess_{phase}");
        let session = machine.machine.root.join("sessions").join(&session_id);
        let drives = session.join("drives");
        let writable = drives.join("drive_write");
        let readonly = drives.join("drive_read");
        if writable_ready {
            state::files::create_private_dirs(&session).unwrap();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let mut state = drive::state::SyncState::create(&session.join("sync-state.db"))
                    .await
                    .unwrap();
                for drive in ["drive_write", "drive_read"] {
                    let write = drive::catchup::finish_listing(state.tables(), drive, 0, 1, 1);
                    state.apply(write).await.unwrap();
                }
            });
        }
        if failed {
            state::files::create_private_dirs(&session).unwrap();
            fs::write(session.join("sync-state.db"), "broken state").unwrap();
            fs::create_dir_all(&writable).unwrap();
            fs::create_dir_all(&readonly).unwrap();
            fs::write(writable.join("ordinary"), "retained").unwrap();
        }
        let observed = base.join(format!("{session_id}-observed"));
        let hook = format!(
            r#"#!/usr/bin/python3
import pathlib
writable = pathlib.Path({writable:?})
readonly = pathlib.Path({readonly:?})
failed = {failed}
for path, allowed in [(writable / 'created', {writable_ready}), (readonly / 'created', False)]:
    try: path.write_text('hook data')
    except OSError as error: assert not allowed, ('write refused', str(path), str(error))
    else:
        assert allowed, ('write permitted', str(path))
        assert path.read_text() == 'hook data'
if failed: assert (writable / 'ordinary').read_text() == 'retained'
pathlib.Path({observed:?}).write_text('passed')
"#,
            writable = text(&writable),
            readonly = text(&readonly),
            failed = if failed { "True" } else { "False" },
            writable_ready = if writable_ready { "True" } else { "False" },
            observed = text(&observed)
        );
        fs::write(hooks.join("post-checkout"), hook).unwrap();
        fs::set_permissions(
            hooks.join("post-checkout"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let target = base.join(&session_id);
        let mut payload = payload(
            &session_id,
            &format!("bind_{phase}"),
            base,
            json!([repository(&remote.url, &target, Value::Null)]),
        );
        payload["drives"] = json!([{"drive_id":"drive_write","access":"read_write"},{"drive_id":"drive_read","access":"read_only"}]);
        let issued = prepare(&engine, &payload);
        let answer = report(&engine, &issued);
        assert_eq!(failures(&answer), [], "{answer}");
        assert_eq!(fs::read_to_string(&observed).unwrap(), "passed");
        if failed {
            assert_eq!(answer["drive_mount_failures"].as_array().unwrap().len(), 2);
            assert!(!writable.join("created").exists());
        } else {
            assert!(answer.get("drive_mount_failures").is_none(), "{answer}");
            if writable_ready {
                assert_eq!(
                    fs::read_to_string(writable.join("created")).unwrap(),
                    "hook data"
                );
            } else {
                assert!(!writable.join("created").exists());
            }
        }
        assert!(!readonly.join("created").exists());
    }
    case.observe("a real hook wrote through the current healthy read-write FUSE mount and was denied through the read-only mount; with failed sync state, residual files stayed readable and neither configured drive received write access");
    case.pass();
}
