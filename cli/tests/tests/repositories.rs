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
        ];
        let machine = Machine::start_with(
            case,
            &base.join("machine"),
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            &tetral_tests::workspace_dir().join("tests"),
            &environment,
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
    for path in &paths {
        assert!(!path.exists(), "{}", path.display());
    }
    assert!(git.requests().is_empty());
    assert_eq!(machine.machine.seams.count(GIT_SPAWN), 0);
    case.observe(format!(
        "both prepared with both repositories failed with cause clone ({:?}, {:?}); the refused fetch was sent once; nothing was cloned and no git ran",
        refused["repository_failures"][0]["message"], empty["repository_failures"][0]["message"],
    ));
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
    let repositories = json!([
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
    let issued = prepare(
        &engine,
        &payload("sess_a", "bind_1", &work.join("cwd"), repositories.clone()),
    );
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

    let mut case = record.case("a second preparation of the cloned repositories");
    new_connection(&engine, &machine.machine);
    let spawned = machine.machine.seams.count(GIT_SPAWN);
    let served = git.requests().len();
    let issued = prepare(
        &engine,
        &payload("sess_a", "bind_2", &work.join("cwd"), repositories),
    );
    let second = report(&engine, &issued);
    assert_eq!(failures(&second), []);
    let renewed = current_ticket(&engine, "sess_a");
    assert_ne!(renewed, ticket);
    assert_eq!(git.requests().len(), served);
    // Four `config --local --replace-all` for each of the four paths.
    assert_eq!(machine.machine.seams.count(GIT_SPAWN), spawned + 16);
    for path in [&default, &branch, &commit, &missing] {
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
            format!("X-Tetral-Git-Ticket: {renewed}")
        );
    }
    at(&branch, &["fetch", "--quiet", "origin"]);
    let last = git.requests().pop().unwrap();
    assert_eq!(
        (last.ticket.as_deref(), last.status),
        (Some(renewed.as_str()), 200)
    );
    assert_eq!(machine.global_configuration(), global);
    case.observe("prepared without a clone, each repository's header holds the new ticket and a git fetch reached the Git server with it; the global Git configuration files are byte-identical");
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
    let long = "x".repeat(2_000 - "fatal: remote error: ".len());
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
    assert_eq!(messages[5].len(), 1_024);
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
    let repositories = json!([repository(
        &remote.url,
        &path,
        json!({ "type": "branch", "name": "feature" })
    )]);
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
    wait_until("the held clone in repositories.json", || {
        listed(&machine.machine, "sess_a") == [text(&path)]
    });
    assert!(reports_for(&engine, &issued).is_empty());
    assert_eq!(
        machine.in_repository(&path, &["branch", "--show-current"]),
        "feature"
    );
    assert_eq!(
        machine.in_repository(&path, &["rev-parse", "HEAD"]),
        remote.feature
    );
    let served = git.requests().len();
    let issued = prepare(
        &engine,
        &payload("sess_a", "bind_3", &work.join("cwd"), repositories.clone()),
    );
    assert_eq!(failures(&report(&engine, &issued)), []);
    assert_eq!(git.requests().len(), served);
    case.observe("a preparation while the clone still ran failed with cleanup_pending; after the next connection the clone is listed with feature checked out; the next preparation reused it without a clone or repository_path_exists");
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
    case.observe("prepared with the repository cloned into the cwd the probe created");
    case.pass();
}
