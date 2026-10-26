//! Real restricted processes on Linux. Each case runs commands through the
//! isolation's availability check and launch, as the task supervisor does,
//! by starting `tetral` as its isolation harness, or as tool calls of a
//! running App Server, which meet its own state root, control socket, task
//! directories and configuration, and observes from outside every sandbox
//! what the commands could reach.

#![cfg(target_os = "linux")]

use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{IpAddr, TcpListener, UdpSocket};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tetral_tests::binaries;
use tetral_tests::machine::{INSTANCE, seconds};
use tetral_tests::other_user::RootDirectory;
use tetral_tests::record::Case;
use tetral_tests::run::Tetral;
use tetral_tests::seams::Seams;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{
    BINDING, SESSION, TaskMachine, WAIT_CHECKED, millis, policy as task_policy,
};

/// The `argv[0]` under which `tetral` built with `test-seams` reads one
/// request on stdin, runs its command through the check and the launch, and
/// prints the outcome.
const HARNESS: &str = "tetral-isolation-harness";

/// The `PATH` of the harness and of the commands, where the host's `bwrap`
/// lies.
const SYSTEM_PATH: &str = "/usr/bin:/bin";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    Succeeds,
    Fails,
}
use Expect::{Fails, Succeeds};

/// The directories of one case, all canonical: a state root `R` holding the
/// credential, this Session's drive and a second Session's planted files;
/// the socket directory `S` with a listening control socket; the task
/// directories `T` with this task's private temporary directory; a project
/// `p` with a `.git` directory, the `tetral` under test and a symbolic link
/// to `R`; and a project `q` without `.git`.
struct World {
    _base: tempfile::TempDir,
    base: PathBuf,
    root: PathBuf,
    sockets: PathBuf,
    socket: PathBuf,
    _listener: UnixListener,
    tasks: PathBuf,
    task_temporary: PathBuf,
    drives: PathBuf,
    mount: PathBuf,
    project: PathBuf,
    bare: PathBuf,
    tetral: PathBuf,
}

impl World {
    fn new() -> World {
        World::with_tetral(binaries::tetral())
    }

    /// A world whose `tetral` is a link to `built`, at `p/bin/tetral`.
    fn with_tetral(built: &Path) -> World {
        let temporary = tempfile::tempdir().unwrap();
        let base = temporary.path().canonicalize().unwrap();
        let root = base.join("R");
        let sockets = base.join("S");
        let tasks = base.join("T");
        let task_temporary = tasks.join("task_1");
        let drives = root.join("sessions/S1/drives");
        let mount = drives.join("d1");
        let project = base.join("p");
        let bare = base.join("q");
        for directory in [
            &sockets,
            &task_temporary,
            &mount,
            &root.join("sessions/S2/drives/d"),
            &root.join("sessions/S2/tasks/t"),
            &project.join(".git"),
            &project.join("bin"),
            &project.join("secret"),
            &bare,
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        for (path, bytes) in [
            (root.join("credentials.json"), "{\"key\":\"secret\"}"),
            (root.join("sessions/S2/drives/d/f"), "drive"),
            (root.join("sessions/S2/tasks/t/stdout.log"), "log"),
            (mount.join("f"), "mount"),
            (project.join(".git/config"), "[core]\n"),
            (project.join("a"), "a"),
            (project.join("secret/f"), "secret"),
        ] {
            fs::write(path, bytes).unwrap();
        }
        symlink(&root, project.join("l")).unwrap();
        let socket = sockets.join("h.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let tetral = project.join("bin/tetral");
        link_or_copy(built, &tetral);
        World {
            _base: temporary,
            base,
            root,
            sockets,
            socket,
            _listener: listener,
            tasks,
            task_temporary,
            drives,
            mount,
            project,
            bare,
            tetral,
        }
    }

    fn request(&self, policy: &Value, cwd: &Path, argv: &[&str], path: &str) -> Value {
        json!({
            "policy": policy,
            "state_root": self.root,
            "socket_directory": self.sockets,
            "task_directories": self.tasks,
            "task_temporary_directory": self.task_temporary,
            "drives": {
                "directory": self.drives,
                "mounts": [{"path": self.mount, "access": "read_only"}],
            },
            "cwd": cwd,
            "argv": argv,
            "env": {"PATH": path, "HOME": self.task_temporary},
        })
    }
}

/// What the isolation probes name, as a command sees it: the state root `R`, the
/// control socket, the project `p` with a `.git` directory and the project
/// `q` without one, both in `base` beside `R`, the `tetral` executable at
/// `<p>/bin/tetral`, and the task's private temporary directory as a shell
/// word.
struct Targets {
    /// Tells this runner's paths outside every grant from another's.
    label: &'static str,
    base: PathBuf,
    root: PathBuf,
    socket: PathBuf,
    project: PathBuf,
    bare: PathBuf,
    tetral: PathBuf,
    temporary: String,
}

/// How a case runs its commands: through the isolation harness of a world,
/// or as calls of a running App Server.
trait Runner {
    fn targets(&self) -> Targets;

    /// Runs `argv`, a shell with `-c` and a script, under `policy` in `cwd`
    /// and gives its `exit_code`, `stdout` and `stderr`.
    fn run(&self, policy: &Value, cwd: &Path, argv: &[&str]) -> Value;

    /// Runs `probes` in one `bash` command and asserts each one's outcome.
    fn probe(
        &self,
        case: &mut Case,
        policy: &Value,
        cwd: &Path,
        probes: &[(&str, String, Expect)],
    ) {
        let outcome = self.run(policy, cwd, &["/bin/bash", "-c", &script(probes)]);
        check_probes(case, policy, &outcome, probes);
    }
}

impl Runner for World {
    fn targets(&self) -> Targets {
        Targets {
            label: "harness",
            base: self.base.clone(),
            root: self.root.clone(),
            socket: self.socket.clone(),
            project: self.project.clone(),
            bare: self.bare.clone(),
            tetral: self.tetral.clone(),
            temporary: quoted(&self.task_temporary),
        }
    }

    /// Runs one command through the harness of the world's `tetral`.
    fn run(&self, policy: &Value, cwd: &Path, argv: &[&str]) -> Value {
        harness(
            &self.tetral,
            &self.request(policy, cwd, argv, SYSTEM_PATH),
            SYSTEM_PATH,
            None,
        )
    }
}

/// An App Server started from a link of the test build at `<p>/bin/tetral`,
/// with `q` beside `p` and a second Session's files planted in its state
/// root, whose `exec_command`, `edit` and `view_image` calls meet its own
/// state root, control socket and task directories.
struct Served {
    tasks: TaskMachine,
    base: PathBuf,
    calls: AtomicU64,
    _directory: tempfile::TempDir,
}

impl Served {
    fn start(case: &mut Case) -> Served {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let bin = base.join("project/bin");
        fs::create_dir_all(&bin).unwrap();
        link_or_copy(binaries::tetral(), &bin.join("tetral"));
        let tasks = TaskMachine::start_from(case, &base, &bin.join("tetral"), SYSTEM_PATH);
        let (p, root) = (&tasks.project, &tasks.root);
        for directory in [
            &p.join(".git"),
            &p.join("secret"),
            &base.join("q"),
            &root.join("sessions/S2/drives/d"),
            &root.join("sessions/S2/tasks/t"),
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        for (path, bytes) in [
            (root.join("sessions/S2/drives/d/f"), "drive"),
            (root.join("sessions/S2/tasks/t/stdout.log"), "log"),
            (p.join(".git/config"), "[core]\n"),
            (p.join("a"), "a"),
            (p.join("secret/f"), "secret"),
        ] {
            fs::write(path, bytes).unwrap();
        }
        symlink(root, p.join("l")).unwrap();
        assert!(root.join("credentials.json").is_file());
        Served {
            tasks,
            base,
            calls: AtomicU64::new(0),
            _directory: directory,
        }
    }

    fn next_task(&self) -> String {
        format!("task_probe_{}", self.calls.fetch_add(1, Ordering::SeqCst))
    }

    /// Runs `tool` with `input` under `policy` and gives its terminal
    /// envelope: its status, error and result.
    fn file_tool(&self, tool: &str, input: Value, policy: &Value) -> Value {
        let task = self.next_task();
        self.tasks.engine.control(
            "execute",
            json!({"instance_id": INSTANCE, "payload": {
                "session_id": SESSION, "binding_id": BINDING,
                "tool_use_event_id": format!("call_{task}"), "task_id": task,
                "tool": tool, "input": input, "sandbox_policy": policy,
            }}),
        );
        self.tasks.terminal(&task)["result"].clone()
    }
}

impl Runner for Served {
    fn targets(&self) -> Targets {
        let project = self.tasks.project.clone();
        Targets {
            label: "exec_command",
            base: self.base.clone(),
            root: self.tasks.root.clone(),
            socket: self
                .tasks
                .machine()
                .app_server
                .socket
                .canonicalize()
                .unwrap(),
            bare: self.base.join("q"),
            tetral: project.join("bin/tetral"),
            project,
            temporary: "\"$TMPDIR\"".to_owned(),
        }
    }

    /// Runs the script of `argv` as an `exec_command` call, whose shell is
    /// `bash`.
    fn run(&self, policy: &Value, cwd: &Path, argv: &[&str]) -> Value {
        let [_, "-c", script] = argv else {
            panic!("a shell with -c and a script: {argv:?}");
        };
        let task = self.next_task();
        self.tasks
            .execute(&task, json!({"cmd": script, "cwd": cwd}), policy.clone());
        let result = &self.tasks.terminal(&task)["result"]["result"];
        json!({
            "exit_code": result["exit_code"],
            "stdout": result["stdout"]["text"],
            "stderr": result["stderr"]["text"],
        })
    }
}

/// Links `built` to `link`, or copies it where a link cannot cross
/// filesystems. A link keeps the build's inode, so a write that went through
/// would fail with the running executable busy rather than change the build.
fn link_or_copy(built: &Path, link: &Path) {
    if fs::hard_link(built, link).is_err() {
        fs::copy(built, link).unwrap();
    }
}

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

fn policy(rules: Value, network: &str) -> Value {
    json!({"schema_version": 1, "filesystem": {"rules": rules}, "network": {"mode": network}})
}

fn read_write(project: &Path) -> Value {
    policy(
        json!([{"path": "/", "access": "read"}, {"path": project, "access": "read_write"}]),
        "direct",
    )
}

fn quoted(path: &Path) -> String {
    format!("'{}'", path.display())
}

/// Starts `tetral` at `program` as the isolation harness with `request` on
/// stdin and `PATH` set to `path`, and gives the harness's answer.
fn harness(program: &Path, request: &Value, path: &str, seams: Option<&Seams>) -> Value {
    let mut command = Tetral::at(program, [] as [&str; 0])
        .arg0(HARNESS)
        .env("PATH", path)
        .stdin(request.to_string());
    if let Some(seams) = seams {
        command = command.seams(seams);
    }
    let run = command.run();
    assert!(
        run.status.success(),
        "the harness failed: {}; stderr: {}",
        run.exit(),
        String::from_utf8_lossy(&run.stderr)
    );
    run.object()
}

/// One `bash` script that runs each probe in turn and prints, per probe, its
/// index and `ok`, or `fail` and what it printed on stderr.
fn script(probes: &[(&str, String, Expect)]) -> String {
    let mut script = String::new();
    for (index, (_, probe, _)) in probes.iter().enumerate() {
        script.push_str(&format!(
            "if err=$( {{ {probe} ; }} 2>&1 >/dev/null ); then echo '{index} ok'; else echo \"{index} fail ${{err//$'\\n'/ }}\"; fi\n"
        ));
    }
    script
}

fn check_probes(
    case: &mut Case,
    policy: &Value,
    outcome: &Value,
    probes: &[(&str, String, Expect)],
) {
    assert_eq!(outcome["exit_code"], 0, "the probes ran: {outcome}");
    let lines: Vec<&str> = outcome["stdout"].as_str().unwrap().lines().collect();
    assert_eq!(lines.len(), probes.len(), "one line per probe: {outcome}");
    let rules = &policy["filesystem"]["rules"];
    let network = &policy["network"]["mode"];
    for (index, ((name, _, expected), line)) in probes.iter().zip(&lines).enumerate() {
        let (number, rest) = line.split_once(' ').unwrap();
        assert_eq!(number, index.to_string());
        let actual = if rest == "ok" { Succeeds } else { Fails };
        case.observe(format!("rules {rules}, {network}: {name}: {rest}"));
        assert_eq!(actual, *expected, "{name}: {rest}");
    }
}

/// The probes that every grant must fail: the credential, the control
/// socket, the second Session's planted files and the `tetral` executable.
fn protected_probes(targets: &Targets) -> Vec<(&'static str, String, Expect)> {
    let replacement = targets.project.join("replacement");
    vec![
        (
            "read R/credentials.json",
            format!("cat {}", quoted(&targets.root.join("credentials.json"))),
            Fails,
        ),
        (
            "connect to the control socket in S",
            format!(
                "python3 -c 'import socket, sys; socket.socket(socket.AF_UNIX).connect(sys.argv[1])' {}",
                quoted(&targets.socket)
            ),
            Fails,
        ),
        (
            "read the second Session's drive file",
            format!(
                "cat {}",
                quoted(&targets.root.join("sessions/S2/drives/d/f"))
            ),
            Fails,
        ),
        (
            "read the second Session's task log",
            format!(
                "cat {}",
                quoted(&targets.root.join("sessions/S2/tasks/t/stdout.log"))
            ),
            Fails,
        ),
        (
            "replace the tetral executable",
            format!(
                "cp /bin/true {r} && mv -f {r} {t}",
                r = quoted(&replacement),
                t = quoted(&targets.tetral)
            ),
            Fails,
        ),
    ]
}

#[test]
fn every_grant_reaches_only_what_its_rules_allow_and_never_the_protected_paths() {
    let mut case = tetral_tests::record!()
        .case("every grant reaches only what its rules allow and never the protected paths");
    grants_reach_only_what_their_rules_allow(&mut case, &World::new());
}

/// The probes under `/` read with `<p>` read_write, of a missing
/// top-level `.git`, of the descriptors, under `/` read_write, under no rule
/// and under a deny rule inside `<p>`.
fn grants_reach_only_what_their_rules_allow(case: &mut Case, runner: &impl Runner) {
    let targets = runner.targets();
    let p = &targets.project;
    // Outside every grant, and removed even when a probe wrongly writes it.
    let outside = tempfile::TempPath::try_from_path(format!(
        "/tmp/tetral-isolation-probe-{}-{}",
        std::process::id(),
        targets.label
    ))
    .unwrap();
    let tetral_inode = fs::metadata(&targets.tetral).unwrap().ino();

    let mut probes = vec![
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
            format!("printf x > {}/f", targets.temporary),
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
    ];
    for name in [".codex", ".agents", ".aws"] {
        probes.push((
            "create a file under a writable metadata name",
            format!(
                "mkdir -p {d} && printf x > {d}/x",
                d = quoted(&p.join(name))
            ),
            Succeeds,
        ));
    }
    probes.extend(protected_probes(&targets));
    probes.push((
        "read R/credentials.json through the link <p>/l",
        format!("cat {}", quoted(&p.join("l/credentials.json"))),
        Fails,
    ));
    let relative = targets.root.strip_prefix(&targets.base).unwrap();
    let doubled = format!(
        "{}//{}///credentials.json",
        targets.base.display(),
        relative.display()
    );
    for (name, alias) in [
        (
            "through <p>/..",
            p.join("..").join(relative).join("credentials.json"),
        ),
        ("with repeated /", PathBuf::from(doubled)),
        (
            "through /proc/self/root",
            Path::new("/proc/self/root")
                .join(targets.root.strip_prefix("/").unwrap())
                .join("credentials.json"),
        ),
    ] {
        probes.push((name, format!("cat {}", quoted(&alias)), Fails));
    }
    runner.probe(case, &read_write(p), p, &probes);
    assert!(!outside.exists(), "{} was written", outside.display());
    assert!(p.join(".git/config").is_file() && !p.join("g").exists());
    assert_eq!(fs::metadata(&targets.tetral).unwrap().ino(), tetral_inode);

    // A missing top-level .git stays uncreatable, and the isolation leaves no
    // metadata name behind.
    let q = &targets.bare;
    runner.probe(
        case,
        &read_write(q),
        q,
        &[
            ("git init <q>", format!("git init {}", quoted(q)), Fails),
            (
                "mkdir <q>/.git",
                format!("mkdir {}", quoted(&q.join(".git"))),
                Fails,
            ),
            (
                "write <q>/f",
                format!("printf x > {}", quoted(&q.join("f"))),
                Succeeds,
            ),
        ],
    );
    for name in [".git", ".agents", ".codex", ".aws"] {
        assert!(
            !q.join(name).exists(),
            "<q>/{name} exists after the commands"
        );
    }
    case.observe("after the commands under <q> read_write: no <q>/.git, .agents, .codex or .aws");

    let descriptors = runner.run(
        &read_write(p),
        p,
        &[
            "/bin/bash",
            "-c",
            "for n in {3..1023}; do if (true >&$n) 2>/dev/null; then echo $n; fi; done",
        ],
    );
    assert_eq!(descriptors["exit_code"], 0, "{descriptors}");
    assert_eq!(
        descriptors["stdout"], "",
        "descriptors open beyond 0, 1 and 2"
    );
    case.observe("descriptors 3 to 1023: none open");

    let everything = policy(json!([{"path": "/", "access": "read_write"}]), "direct");
    let mut probes = vec![(
        "write <p>/.git/config",
        format!("printf x >> {}", quoted(&p.join(".git/config"))),
        Succeeds,
    )];
    probes.extend(protected_probes(&targets));
    runner.probe(case, &everything, p, &probes);
    assert_eq!(fs::metadata(&targets.tetral).unwrap().ino(), tetral_inode);

    // Under `disabled` the test build passes the launcher entry no seam
    // variable, which it could not use with nothing readable.
    let nothing = policy(json!([]), "disabled");
    let shell = runner.run(
        &nothing,
        p,
        &["/bin/sh", "-c", &format!("cat {}", quoted(&p.join("a")))],
    );
    let stderr = shell["stderr"].as_str().unwrap();
    assert_eq!(shell["exit_code"], 1, "{shell}");
    assert!(stderr.starts_with("cat:"), "cat ran and failed: {shell}");
    case.observe(format!(
        "rules [], `/bin/sh -c 'cat <p>/a'`: exit 1, {stderr:?}"
    ));

    let denied_inside = policy(
        json!([
            {"path": "/", "access": "read"},
            {"path": p, "access": "read_write"},
            {"path": p.join("secret"), "access": "deny"},
        ]),
        "direct",
    );
    runner.probe(
        case,
        &denied_inside,
        p,
        &[(
            "read <p>/secret/f",
            format!("cat {}", quoted(&p.join("secret/f"))),
            Fails,
        )],
    );
}

#[test]
fn a_missing_deny_path_under_a_directory_of_another_user_stays_uncreatable_and_the_command_runs() {
    let mut case = tetral_tests::record!().case(
        "a missing deny path under a directory of another user stays uncreatable and the command runs",
    );
    a_missing_deny_path_stays_uncreatable(&mut case, &World::new());
}

/// The probe of a deny rule for a path below a missing `/<m>`.
fn a_missing_deny_path_stays_uncreatable(case: &mut Case, runner: &impl Runner) {
    if !case.runs_as_non_root("root may create entries in /") {
        return;
    }
    let targets = runner.targets();
    let missing = PathBuf::from(format!(
        "/tetral-isolation-missing-{}-{}",
        std::process::id(),
        targets.label
    ));
    assert!(!missing.exists());
    let denied_missing = policy(
        json!([
            {"path": "/", "access": "read_write"},
            {"path": missing.join("x"), "access": "deny"},
        ]),
        "direct",
    );
    runner.probe(
        case,
        &denied_missing,
        &targets.project,
        &[("mkdir /<m>", format!("mkdir {}", quoted(&missing)), Fails)],
    );
    assert!(!missing.exists(), "{} exists", missing.display());
}

#[test]
fn a_rule_for_a_regular_file_and_a_deny_path_under_one_leave_the_command_running() {
    let mut case = tetral_tests::record!().case(
        "a read_write rule for a regular file, and a deny path under a regular file, in a writable project",
    );
    regular_file_rules_leave_the_command_running(&mut case, &World::new());
}

/// The probes of a `read_write` rule for a regular file and of a deny
/// rule below one.
fn regular_file_rules_leave_the_command_running(case: &mut Case, runner: &impl Runner) {
    let targets = runner.targets();
    let p = &targets.project;
    let f = p.join("f");
    fs::write(&f, "x\n").unwrap();
    let file_rule = policy(
        json!([
            {"path": "/", "access": "read"},
            {"path": p, "access": "read_write"},
            {"path": f, "access": "read_write"},
        ]),
        "direct",
    );
    runner.probe(
        case,
        &file_rule,
        p,
        &[(
            "echo y >> <p>/f",
            format!("echo y >> {}", quoted(&f)),
            Succeeds,
        )],
    );
    assert_eq!(fs::read_to_string(&f).unwrap(), "x\ny\n");
    let under_file = policy(
        json!([
            {"path": "/", "access": "read"},
            {"path": p, "access": "read_write"},
            {"path": f.join("x"), "access": "deny"},
        ]),
        "direct",
    );
    let shown = runner.run(
        &under_file,
        p,
        &["/bin/sh", "-c", &format!("cat {}", quoted(&f))],
    );
    assert_eq!(
        (&shown["exit_code"], &shown["stdout"]),
        (&json!(0), &json!("x\ny\n")),
        "{shown}"
    );
    case.observe("f ended with y; under <p>/f/x deny, cat <p>/f ran and printed f");
}

#[test]
fn the_probes_run_again_as_calls_of_an_app_server_against_its_own_paths() {
    let _socket_dir = socket_directory::shared();
    let mut case = tetral_tests::record!().case(
        "the isolation probes as exec_command calls of an App Server started from <p>/bin/tetral, and those that read or write one file as view_image and edit calls, against its own state root, control socket and task directories",
    );
    let served = Served::start(&mut case);
    grants_reach_only_what_their_rules_allow(&mut case, &served);
    regular_file_rules_leave_the_command_running(&mut case, &served);
    file_probes(&mut case, &served);
}

#[test]
fn a_missing_deny_path_stays_uncreatable_as_an_exec_command_call() {
    let _socket_dir = socket_directory::shared();
    let mut case = tetral_tests::record!().case(
        "a missing deny path under a directory of another user, as an exec_command call of an App Server",
    );
    let served = Served::start(&mut case);
    a_missing_deny_path_stays_uncreatable(&mut case, &served);
}

/// Whether a `view_image` call read its file: an image, or a file in which
/// it found no image; not when the file could not be read.
fn read_by_view_image(envelope: &Value) -> Expect {
    if envelope["status"] == "success" {
        return Succeeds;
    }
    match envelope["error"]["kind"].as_str() {
        Some("unsupported_format") => Succeeds,
        Some("not_found") => Fails,
        _ => panic!("an outcome that tells nothing of the read: {envelope}"),
    }
}

/// The probes that read or write one file, as `view_image` and `edit`
/// calls of `served`.
fn file_probes(case: &mut Case, served: &Served) {
    let targets = served.targets();
    let p = &targets.project;
    let relative = targets.root.strip_prefix(&targets.base).unwrap();
    let credentials = targets.root.join("credentials.json");
    let protected = [
        ("R/credentials.json", credentials.clone()),
        (
            "the second Session's drive file",
            targets.root.join("sessions/S2/drives/d/f"),
        ),
        (
            "the second Session's task log",
            targets.root.join("sessions/S2/tasks/t/stdout.log"),
        ),
    ];
    let mut reads = vec![("/etc/hostname", PathBuf::from("/etc/hostname"), Succeeds)];
    reads.extend(
        protected
            .iter()
            .map(|(name, path)| (*name, path.clone(), Fails)),
    );
    reads.extend([
        (
            "R/credentials.json through the link <p>/l",
            p.join("l/credentials.json"),
            Fails,
        ),
        (
            "R/credentials.json through <p>/..",
            p.join("..").join(relative).join("credentials.json"),
            Fails,
        ),
        (
            "R/credentials.json with repeated /",
            PathBuf::from(format!(
                "{}//{}///credentials.json",
                targets.base.display(),
                relative.display()
            )),
            Fails,
        ),
        (
            "R/credentials.json through /proc/self/root",
            Path::new("/proc/self/root").join(credentials.strip_prefix("/").unwrap()),
            Fails,
        ),
    ]);
    let shown = |envelope: &Value| {
        json!([
            envelope["status"],
            envelope["error"]["kind"],
            envelope["error"]["message"]
        ])
    };
    let view = |case: &mut Case, policy: &Value, name: &str, path: &Path, expected: Expect| {
        let envelope = served.file_tool("view_image", json!({"path": path}), policy);
        case.observe(format!(
            "rules {}: view_image {name}: {}",
            policy["filesystem"]["rules"],
            shown(&envelope)
        ));
        assert_eq!(
            read_by_view_image(&envelope),
            expected,
            "view_image {name}: {envelope}"
        );
    };
    let edit =
        |case: &mut Case, policy: &Value, name: &str, path: &Path, old: &str, expected: Expect| {
            let before = fs::read(path).unwrap();
            let envelope = served.file_tool(
                "edit",
                json!({"path": path, "old_string": old, "new_string": "edited"}),
                policy,
            );
            case.observe(format!(
                "rules {}: edit {name}: {}",
                policy["filesystem"]["rules"],
                shown(&envelope)
            ));
            let actual = if envelope["status"] == "success" {
                Succeeds
            } else {
                Fails
            };
            assert_eq!(actual, expected, "edit {name}: {envelope}");
            if expected == Fails {
                assert_eq!(fs::read(path).unwrap(), before, "edit {name} changed it");
            }
        };

    let grant = read_write(p);
    for (name, path, expected) in &reads {
        view(case, &grant, name, path, *expected);
    }
    let e = p.join("e");
    fs::write(&e, "old").unwrap();
    edit(case, &grant, "<p>/e", &e, "old", Succeeds);
    assert_eq!(fs::read_to_string(&e).unwrap(), "edited");
    // Outside every grant, and removed even when an edit wrongly writes it.
    let outside = tempfile::TempPath::try_from_path(format!(
        "/tmp/tetral-isolation-probe-{}-edit",
        std::process::id()
    ))
    .unwrap();
    fs::write(&outside, "o").unwrap();
    edit(case, &grant, "a file in /tmp", &outside, "o", Fails);
    let config = p.join(".git/config");
    edit(case, &grant, "<p>/.git/config", &config, "[core]", Fails);

    let everything = policy(json!([{"path": "/", "access": "read_write"}]), "direct");
    edit(
        case,
        &everything,
        "<p>/.git/config",
        &config,
        "[core]",
        Succeeds,
    );
    for (name, path) in &protected {
        view(case, &everything, name, path, Fails);
    }

    let g = p.join("g");
    fs::write(&g, "x\n").unwrap();
    fs::set_permissions(&g, fs::Permissions::from_mode(0o7640)).unwrap();
    let file_rule = policy(
        json!([
            {"path": "/", "access": "read"},
            {"path": p, "access": "read_write"},
            {"path": g, "access": "read_write"},
        ]),
        "direct",
    );
    // The rule mounts g itself, which a rename cannot replace, so the edit
    // writes it in place.
    edit(
        case,
        &file_rule,
        "<p>/g under its own rule",
        &g,
        "x",
        Succeeds,
    );
    assert_eq!(fs::read_to_string(&g).unwrap(), "edited\n");
    assert_eq!(fs::metadata(&g).unwrap().mode() & 0o7777, 0o7640);
    let under_file = policy(
        json!([
            {"path": "/", "access": "read"},
            {"path": p, "access": "read_write"},
            {"path": g.join("x"), "access": "deny"},
        ]),
        "direct",
    );
    view(
        case,
        &under_file,
        "<p>/g under a deny rule for <p>/g/x",
        &g,
        Succeeds,
    );
}

#[test]
fn the_cua_driver_that_config_toml_records_stays_read_only_under_a_writable_rule() {
    let _socket_dir = socket_directory::shared();
    let mut case = tetral_tests::record!().case(
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
    let rules = json!([
        {"path": "/", "access": "read"},
        {"path": tasks.project, "access": "read_write"},
    ]);
    tasks.execute_with(
        SESSION,
        "bind_recorded",
        "task_driver",
        json!({"cmd": "printf y > bin/other; printf x > bin/cua-driver; printf z > r && mv -f r bin/cua-driver"}),
        policy(rules, "disabled"),
        None,
    );
    let terminal = tasks.terminal("task_driver");
    assert_ne!(
        terminal["result"]["result"]["exit_code"],
        json!(0),
        "{terminal}"
    );
    assert_eq!(fs::read_to_string(bin.join("other")).unwrap(), "y");
    assert_eq!(fs::read_to_string(&driver).unwrap(), "driver\n");
    case.observe("the file beside the driver holds y; the write and the replacement of the recorded driver failed, which still holds driver");
}

/// The host's LAN address: the source address of its default route.
fn lan_address() -> IpAddr {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket
        .connect("192.0.2.1:9")
        .expect("the host has a default route");
    let address = socket.local_addr().unwrap().ip();
    assert!(!address.is_loopback(), "the host has a LAN address");
    address
}

/// A TCP listener whose pending connections a test counts without waiting.
fn pending_connections(listener: &TcpListener) -> usize {
    let mut count = 0;
    loop {
        match listener.accept() {
            Ok(_) => count += 1,
            Err(error) if error.kind() == ErrorKind::WouldBlock => return count,
            Err(error) => panic!("accept: {error}"),
        }
    }
}

fn arrived_datagrams(socket: &UdpSocket) -> usize {
    let mut count = 0;
    let mut buffer = [0_u8; 64];
    loop {
        match socket.recv(&mut buffer) {
            Ok(_) => count += 1,
            Err(error) if error.kind() == ErrorKind::WouldBlock => return count,
            Err(error) => panic!("recv: {error}"),
        }
    }
}

/// An HTTP service that answers every request with `200 OK`.
fn http_service(address: IpAddr) -> u16 {
    let listener = TcpListener::bind((address, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(mut connection) = connection else {
                continue;
            };
            let _ = connection.set_read_timeout(Some(Duration::from_secs(5)));
            let mut request = [0_u8; 1024];
            let _ = connection.read(&mut request);
            let _ = connection.write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok");
        }
    });
    port
}

#[test]
fn disabled_blocks_tcp_and_udp_and_direct_reaches_them_and_local_services() {
    let mut case = tetral_tests::record!()
        .case("disabled blocks TCP and UDP, and direct reaches them and local services");
    let world = World::new();
    let p = &world.project;
    let mut listeners = Vec::new();
    for address in ["127.0.0.1".parse().unwrap(), lan_address()] {
        let tcp = TcpListener::bind((address, 0)).unwrap();
        tcp.set_nonblocking(true).unwrap();
        let udp = UdpSocket::bind((address, 0)).unwrap();
        udp.set_nonblocking(true).unwrap();
        let http = http_service(address);
        listeners.push((address, tcp, udp, http));
    }

    for (mode, expected) in [("disabled", Fails), ("direct", Succeeds)] {
        let mut probes = Vec::new();
        for (address, tcp, udp, http) in &listeners {
            let tcp_port = tcp.local_addr().unwrap().port();
            let udp_port = udp.local_addr().unwrap().port();
            probes.push((
                "TCP connect to the listener",
                format!("exec 3<>/dev/tcp/{address}/{tcp_port}"),
                expected,
            ));
            probes.push((
                "TCP connect to the HTTP service",
                format!("exec 3<>/dev/tcp/{address}/{http}"),
                expected,
            ));
            probes.push((
                "UDP datagram to the socket",
                format!("printf x > /dev/udp/{address}/{udp_port}"),
                expected,
            ));
            if mode == "direct" {
                probes.push((
                    "HTTP request to the service",
                    format!(
                        "exec 3<>/dev/tcp/{address}/{http} && printf 'GET / HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '200 OK' <&3"
                    ),
                    Succeeds,
                ));
            }
        }
        let network = policy(
            json!([{"path": "/", "access": "read"}, {"path": p, "access": "read_write"}]),
            mode,
        );
        world.probe(&mut case, &network, p, &probes);
        // Every probe has exited, so whatever it sent sits in the queues.
        for (address, tcp, udp, _) in &listeners {
            let connections = pending_connections(tcp);
            let datagrams = arrived_datagrams(udp);
            case.observe(format!(
                "{mode}, {address}: {connections} connection(s) at the listener, {datagrams} datagram(s) at the socket"
            ));
            let reached = usize::from(mode == "direct");
            assert_eq!(connections, reached, "{mode}: connections at {address}");
            assert_eq!(datagrams, reached, "{mode}: datagrams at {address}");
        }
    }
}

#[test]
fn a_disabled_task_leaves_the_connection_its_heartbeats_and_a_direct_tasks_connect_working() {
    let _socket_dir = socket_directory::shared();
    let mut case = tetral_tests::record!().case(
        "a disabled task running across 20 seconds of the clock while a direct task connects to a TCP listener on the host's LAN address",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let address = lan_address();
    let listener = TcpListener::bind((address, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();

    let checked = seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_disabled",
        json!({"cmd": "while [ ! -e stop ]; do sleep 0.05; done", "wait_ms": 250, "on_wait_expiry": "detach"}),
        task_policy(&tasks.project, "disabled"),
    );
    tasks.wait_for_start(checked);
    seams.advance(millis(250));
    tasks.report("task_disabled", "running");
    tasks.execute(
        "task_direct",
        json!({"cmd": format!("exec 3<>/dev/tcp/{address}/{port}")}),
        task_policy(&tasks.project, "direct"),
    );
    let direct = tasks.terminal("task_direct");
    assert_eq!(direct["result"]["result"]["exit_code"], 0, "{direct:#}");
    assert_eq!(pending_connections(&listener), 1);

    // Each 15 seconds of the clock wait for Engine's heartbeat and the ACK
    // that grants the connection more time.
    let heartbeats = tasks.engine.received("heartbeat").len();
    tasks.advance(seconds(20.0));
    assert_eq!(tasks.engine.received("heartbeat").len(), heartbeats + 1);
    assert_eq!(tasks.machine().state(), "online");
    assert!(
        tasks
            .reports("task_disabled")
            .iter()
            .all(|report| report["phase"] != "terminal")
    );
    fs::write(tasks.project.join("stop"), "").unwrap();
    let disabled = tasks.terminal("task_disabled");
    assert_eq!(disabled["result"]["result"]["exit_code"], 0, "{disabled:#}");
    case.observe("the direct task's connect reached the listener while the disabled task ran; across 20 seconds of the clock the heartbeat was ACKed and the connection stayed online; the disabled task ran until stopped and exited 0");
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
/// command's own `PATH`, which the launch hands to `tetral` as
/// `codex-linux-sandbox`.
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
    /// The launch starts `tetral` as `codex-linux-sandbox`, which ends with
    /// exit status 1 naming the planted `bwrap` on stderr before it runs it
    /// or the command.
    RefusedByTheHelper { planted: &'a Path },
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

/// Checks `offer` by the harness and, unless the helper refuses it, which
/// only the harness's request can set up, as a `tool.execute`.
fn check_offer_both_ways(case: &mut Case, offer: Offer) {
    let refused = matches!(offer.expected, Expected::RefusedByTheHelper { .. });
    check_offer(case, offer.clone(), Via::Harness);
    if !refused {
        check_offer(case, offer, Via::ToolExecute);
    }
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
        Expected::RefusedByTheHelper { planted } => {
            assert_eq!(outcome["exit_code"], 1, "{name}: {outcome}");
            assert_eq!((spawned, marker.exists()), (1, false), "{name}");
            names(outcome["stderr"].as_str().unwrap(), planted);
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
        Expected::RefusedByTheHelper { .. } => unreachable!("only the harness sets it up"),
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
            command_path: SYSTEM_PATH,
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
            name: "that copy first only on the command's PATH, as if planted after the check",
            tetral: binaries::tetral(),
            path: SYSTEM_PATH,
            beside: None,
            command_path: &planted_path,
            expected: Expected::RefusedByTheHelper { planted: &planted },
        },
    ];
    for offer in rows {
        check_offer_both_ways(&mut case, offer);
    }
    drop(case);

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
    let request = world.request(&policy, p, &["/bin/bash", "-c", &script(&probes)], path);
    let outcome = harness(&world.tetral, &request, path, None);
    check_probes(&mut case, &policy, &outcome, &probes);
    assert!(!outside.exists(), "{} was written", outside.display());
}
