//! Real restricted processes through the isolation, for the isolation
//! suites of each platform: a world of canonical directories (a state root
//! with this Session's drive and a second Session's planted files, a socket
//! directory with a listening socket, task directories, a project with
//! `.git` and one without) whose commands run through `tetral`'s isolation
//! harness or as tool calls of a running App Server, and the probe tables
//! both platforms run against them, observed from outside every sandbox.

use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{IpAddr, TcpListener, UdpSocket};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use crate::binaries;
use crate::machine::{INSTANCE, seconds};
use crate::record::Case;
use crate::run::Tetral;
use crate::seams::Seams;
use crate::tasks::{BINDING, SESSION, TaskMachine, WAIT_CHECKED, millis, policy as task_policy};

/// The `argv[0]` under which `tetral` built with `test-seams` reads one
/// request on stdin, runs its command through the check and the launch, and
/// prints the outcome.
pub const HARNESS: &str = "tetral-isolation-harness";

/// The `PATH` of the harness and of the commands, where the host's `bwrap`
/// lies.
pub const SYSTEM_PATH: &str = "/usr/bin:/bin";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    Succeeds,
    Fails,
}
pub use Expect::{Fails, Succeeds};

/// The directories of one case, all canonical: a state root `R` holding the
/// credential, this Session's drive and a second Session's planted files;
/// the socket directory `S` with a listening control socket; the task
/// directories `T` with this task's private temporary directory; a project
/// `p` with a `.git` directory, the `tetral` under test and a symbolic link
/// to `R`; and a project `q` without `.git`.
pub struct World {
    _base: tempfile::TempDir,
    pub base: PathBuf,
    pub root: PathBuf,
    pub sockets: PathBuf,
    pub socket: PathBuf,
    _listener: UnixListener,
    pub tasks: PathBuf,
    pub task_temporary: PathBuf,
    pub drives: PathBuf,
    pub mount: PathBuf,
    pub project: PathBuf,
    pub bare: PathBuf,
    pub tetral: PathBuf,
}

impl Default for World {
    fn default() -> World {
        World::new()
    }
}

impl World {
    pub fn new() -> World {
        World::with_tetral(binaries::tetral())
    }

    /// A world whose `tetral` is a link to `built`, at `p/bin/tetral`.
    pub fn with_tetral(built: &Path) -> World {
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
            &root.join(state::paths::CREDENTIAL_DIRECTORY),
            &root.join(state::paths::RUN),
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
            (
                root.join("credentials/credentials.json"),
                "{\"key\":\"secret\"}",
            ),
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

    pub fn request(&self, policy: &Value, cwd: &Path, argv: &[&str], path: &str) -> Value {
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
pub struct Targets {
    /// Tells this runner's paths outside every grant from another's.
    pub label: &'static str,
    pub base: PathBuf,
    pub root: PathBuf,
    pub socket: PathBuf,
    pub project: PathBuf,
    pub bare: PathBuf,
    pub tetral: PathBuf,
    pub temporary: String,
}

/// How a case runs its commands: through the isolation harness of a world,
/// or as calls of a running App Server.
pub trait Runner {
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
pub struct Served {
    tasks: TaskMachine,
    base: PathBuf,
    calls: AtomicU64,
    _directory: tempfile::TempDir,
}

impl Served {
    pub fn start(case: &mut Case) -> Served {
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
        assert!(root.join("credentials/credentials.json").is_file());
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

/// A file every grant of `/` reads.
#[cfg(target_os = "linux")]
pub const READABLE_SYSTEM_FILE: &str = "/etc/hostname";
#[cfg(target_os = "macos")]
pub const READABLE_SYSTEM_FILE: &str = "/etc/hosts";

/// `R/credentials/credentials.json` through the platform's own alias of
/// its path: `/proc/self/root` on Linux; on macOS, whose file systems ignore
/// letter case, the `credentials` directory in capitals. Each alias reaches
/// the file from outside every sandbox.
pub fn platform_alias(root: &Path) -> (&'static str, PathBuf) {
    let (name, alias) = if cfg!(target_os = "macos") {
        (
            "with its directory in capitals",
            root.join("CREDENTIALS/credentials.json"),
        )
    } else {
        (
            "through /proc/self/root",
            Path::new("/proc/self/root")
                .join(root.strip_prefix("/").unwrap())
                .join("credentials/credentials.json"),
        )
    };
    assert!(alias.is_file(), "{} reaches no file", alias.display());
    (name, alias)
}

/// Links `built` to `link`, or copies it where a link cannot cross
/// filesystems. A link keeps the build's inode, so a write that went through
/// would fail with the running executable busy rather than change the build.
pub fn link_or_copy(built: &Path, link: &Path) {
    if fs::hard_link(built, link).is_err() {
        fs::copy(built, link).unwrap();
    }
}

pub fn policy(rules: Value, network: &str) -> Value {
    json!({"schema_version": 1, "filesystem": {"rules": rules}, "network": {"mode": network}})
}

pub fn read_write(project: &Path) -> Value {
    policy(
        json!([{"path": "/", "access": "read"}, {"path": project, "access": "read_write"}]),
        "direct",
    )
}

pub fn quoted(path: &Path) -> String {
    format!("'{}'", path.display())
}

/// Starts `tetral` at `program` as the isolation harness with `request` on
/// stdin and `PATH` set to `path`, and gives the harness's answer.
pub fn harness(program: &Path, request: &Value, path: &str, seams: Option<&Seams>) -> Value {
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
pub fn script(probes: &[(&str, String, Expect)]) -> String {
    let mut script = String::new();
    for (index, (_, probe, _)) in probes.iter().enumerate() {
        script.push_str(&format!(
            "if err=$( {{ {probe} ; }} 2>&1 >/dev/null ); then echo '{index} ok'; else echo \"{index} fail ${{err//$'\\n'/ }}\"; fi\n"
        ));
    }
    script
}

pub fn check_probes(
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

/// Mandatory private-path denial, ordinary cross-Session reads and executable
/// write protection, applied under every caller grant.
pub fn protected_probes(targets: &Targets) -> Vec<(&'static str, String, Expect)> {
    let mut probes = vec![
        (
            "read R/credentials/credentials.json",
            format!(
                "cat {}",
                quoted(&targets.root.join("credentials/credentials.json"))
            ),
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
            Succeeds,
        ),
        (
            "read the second Session's task log",
            format!(
                "cat {}",
                quoted(&targets.root.join("sessions/S2/tasks/t/stdout.log"))
            ),
            Succeeds,
        ),
    ];
    probes.extend(replacement_probes(&targets.project, &targets.tetral));
    probes
}

/// Each preparation and ordinary rename is asserted independently of the
/// protected rename. A copy/creation failure cannot count as protection.
pub fn replacement_probes(project: &Path, protected: &Path) -> Vec<(&'static str, String, Expect)> {
    let source = quoted(&project.join("replacement"));
    let ordinary = quoted(&project.join("replacement-moved"));
    vec![
        (
            "prepare an attribute-free replacement",
            format!("printf replacement-bytes > {source}"),
            Succeeds,
        ),
        (
            "move a replacement to an ordinary writable destination",
            format!("mv -f {source} {ordinary}"),
            Succeeds,
        ),
        (
            "verify ordinary replacement bytes",
            format!("test \"$(cat {ordinary})\" = replacement-bytes"),
            Succeeds,
        ),
        (
            "prepare a fresh protected replacement",
            format!("printf replacement-bytes > {source}"),
            Succeeds,
        ),
        (
            "attempt the protected rename",
            format!("mv -f {source} {}", quoted(protected)),
            Fails,
        ),
    ]
}

/// The probes under `/` read with `<p>` read_write, of a missing
/// top-level `.git`, of the descriptors, under `/` read_write, under no rule
/// and under a deny rule inside `<p>`.
pub fn grants_reach_only_what_their_rules_allow(case: &mut Case, runner: &impl Runner) {
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
    let tetral_bytes = fs::read(&targets.tetral).unwrap();

    let mut probes = vec![
        (
            "read a system file",
            format!("cat {READABLE_SYSTEM_FILE}"),
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
        "read R/credentials/credentials.json through the link <p>/l",
        format!("cat {}", quoted(&p.join("l/credentials/credentials.json"))),
        Fails,
    ));
    let relative = targets.root.strip_prefix(&targets.base).unwrap();
    let doubled = format!(
        "{}//{}///credentials/credentials.json",
        targets.base.display(),
        relative.display()
    );
    for (name, alias) in [
        (
            "through <p>/..",
            p.join("..")
                .join(relative)
                .join("credentials/credentials.json"),
        ),
        ("with repeated /", PathBuf::from(doubled)),
        platform_alias(&targets.root),
    ] {
        probes.push((name, format!("cat {}", quoted(&alias)), Fails));
    }
    let writable = read_write(p);
    let result = runner.run(&writable, p, &["/bin/bash", "-c", &script(&probes)]);
    assert_eq!(
        fs::metadata(&targets.tetral).unwrap().ino(),
        tetral_inode,
        "protected executable inode changed"
    );
    assert_eq!(
        fs::read(&targets.tetral).unwrap(),
        tetral_bytes,
        "protected executable bytes changed"
    );
    check_probes(case, &writable, &result, &probes);
    assert!(!outside.exists(), "{} was written", outside.display());
    assert!(p.join(".git/config").is_file() && !p.join("g").exists());

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
            "for n in {3..1023}; do if [ -e /dev/fd/$n ]; then echo $n; fi; done",
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
    let result = runner.run(&everything, p, &["/bin/bash", "-c", &script(&probes)]);
    assert_eq!(
        fs::metadata(&targets.tetral).unwrap().ino(),
        tetral_inode,
        "protected executable inode changed"
    );
    assert_eq!(
        fs::read(&targets.tetral).unwrap(),
        tetral_bytes,
        "protected executable bytes changed"
    );
    check_probes(case, &everything, &result, &probes);

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
    // On macOS the shell first notes that it cannot read its directory.
    assert!(
        stderr.lines().any(|line| line.starts_with("cat:")),
        "cat ran and failed: {shell}"
    );
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

/// The probe of a deny rule for a path below a missing `/<m>`.
pub fn a_missing_deny_path_stays_uncreatable(case: &mut Case, runner: &impl Runner) {
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

/// The probes of a `read_write` rule for a regular file and of a deny
/// rule below one.
pub fn regular_file_rules_leave_the_command_running(case: &mut Case, runner: &impl Runner) {
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

/// Whether a `view_image` call read its file: an image, or a file in which
/// it found no image; not when the file could not be read.
pub fn read_by_view_image(envelope: &Value) -> Expect {
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
pub fn file_probes(case: &mut Case, served: &Served) {
    let targets = served.targets();
    let p = &targets.project;
    let relative = targets.root.strip_prefix(&targets.base).unwrap();
    let credentials = targets.root.join("credentials/credentials.json");
    let protected = [
        ("R/credentials/credentials.json", credentials.clone()),
        (
            "the second Session's drive file",
            targets.root.join("sessions/S2/drives/d/f"),
        ),
        (
            "the second Session's task log",
            targets.root.join("sessions/S2/tasks/t/stdout.log"),
        ),
    ];
    let mut reads = vec![(
        READABLE_SYSTEM_FILE,
        PathBuf::from(READABLE_SYSTEM_FILE),
        Succeeds,
    )];
    reads.extend(protected.iter().map(|(name, path)| {
        (
            *name,
            path.clone(),
            if path == &credentials {
                Fails
            } else {
                Succeeds
            },
        )
    }));
    reads.extend([
        (
            "R/credentials/credentials.json through the link <p>/l",
            p.join("l/credentials/credentials.json"),
            Fails,
        ),
        (
            "R/credentials/credentials.json through <p>/..",
            p.join("..")
                .join(relative)
                .join("credentials/credentials.json"),
            Fails,
        ),
        (
            "R/credentials/credentials.json with repeated /",
            PathBuf::from(format!(
                "{}//{}///credentials/credentials.json",
                targets.base.display(),
                relative.display()
            )),
            Fails,
        ),
    ]);
    let (alias_name, alias) = platform_alias(&targets.root);
    let alias_name = format!("R/credentials/credentials.json {alias_name}");
    reads.push((&alias_name, alias, Fails));
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
        view(
            case,
            &everything,
            name,
            path,
            if path == &credentials {
                Fails
            } else {
                Succeeds
            },
        );
        edit(
            case,
            &everything,
            name,
            path,
            if path == &credentials {
                "connection_key"
            } else if name.contains("drive") {
                "drive"
            } else {
                "log"
            },
            Fails,
        );
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
    let mode = fs::metadata(&g).unwrap().mode();
    assert_eq!(mode & 0o777, 0o640, "{mode:o}");
    // The kernel clears setuid on this unprivileged in-place write. The
    // editor must not restore special permission bits after changing data.
    assert_eq!(mode & 0o4000, 0, "{mode:o}");
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

/// The host's LAN address: the source address of its default route.
pub fn lan_address() -> IpAddr {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket
        .connect("192.0.2.1:9")
        .expect("the host has a default route");
    let address = socket.local_addr().unwrap().ip();
    assert!(!address.is_loopback(), "the host has a LAN address");
    address
}

/// A TCP listener whose pending connections a test counts without waiting.
pub fn pending_connections(listener: &TcpListener) -> usize {
    let mut count = 0;
    loop {
        match listener.accept() {
            Ok(_) => count += 1,
            Err(error) if error.kind() == ErrorKind::WouldBlock => return count,
            Err(error) => panic!("accept: {error}"),
        }
    }
}

pub fn arrived_datagrams(socket: &UdpSocket) -> usize {
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
pub fn http_service(address: IpAddr) -> u16 {
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

/// TCP and UDP to a listener, a socket and an HTTP service on `127.0.0.1`
/// and on the host's LAN address, from commands under `disabled` and under
/// `direct`.
pub fn network_reach(case: &mut Case) {
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
        world.probe(case, &network, p, &probes);
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

/// A `disabled` task running across 20 seconds of the clock while a
/// `direct` task connects to a TCP listener on the host's LAN address.
pub fn disabled_task_beside_direct(case: &mut Case) {
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(case, directory.path());
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
