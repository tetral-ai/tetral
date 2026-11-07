//! The restricted path for every tool process and internal preparation Git:
//! the availability check at each execution, then the launch, in which
//! Codex's sandbox manager compiles the task's filesystem and network
//! permissions into one sandboxed command: `bwrap` and seccomp on Linux
//! (`linux.rs`), a Seatbelt profile under `sandbox-exec` on macOS
//! (`macos.rs`). No grant skips it.

#[cfg(feature = "test-seams")]
pub mod harness;
#[cfg(target_os = "linux")]
mod host_ipc;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
mod probe;

#[cfg(all(target_os = "macos", feature = "test-seams"))]
pub use macos::TEST_SANDBOX_EXEC_VARIABLE;

use std::collections::HashMap;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::models::PermissionProfile;
use codex_sandboxing::{
    SandboxCommand, SandboxExecRequest, SandboxManager, SandboxTransformRequest,
    SandboxablePreference,
};
use codex_utils_path_uri::PathUri;
use protocol::tool::RejectionCode;

use crate::policy::{PolicyRejection, TaskPermissions};

/// How long a probe of a `bwrap` candidate, or of `sandbox-exec`, may run.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// The most of a probe's stdout and of its stderr that the check reads.
pub const PROBE_OUTPUT_LIMIT_BYTES: usize = 65_536;

/// The availability check and the launch, with the probe bounds they use.
#[derive(Debug, Clone, Copy)]
pub struct Isolation {
    probe_timeout: Duration,
    probe_output_limit: usize,
}

impl Default for Isolation {
    fn default() -> Self {
        Isolation::new()
    }
}

/// A check that passed; only a launch after it can start a tool's process.
#[derive(Debug)]
pub struct Available(());

/// A tool's isolated entry and the environment data for its final command.
/// Launchers use a separate, controlled environment throughout setup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub argv: Vec<String>,
    /// Read by the exec entry after isolation, never installed in a launcher.
    /// File tools have no command environment.
    pub env: Option<HashMap<String, String>>,
    /// `None` starts it in the App Server's own working directory, the one
    /// against which the availability check and Codex's helper both resolve
    /// `PATH`, so that the check judges the `bwrap` the launch runs; a task
    /// enters its own directory inside the sandbox (`child`).
    pub directory: Option<PathBuf>,
}

/// The descriptor on which a launched command finds the descriptor that its
/// launch passes it.
pub const PASSED_DESCRIPTOR: RawFd = 3;

/// The exec entry's anonymous command-environment carrier.
pub const ENVIRONMENT_DESCRIPTOR: RawFd = 4;

/// The read end of a launched command's start pipe.
pub const START_DESCRIPTOR: RawFd = 5;

/// A started sandboxed command, with its stdin, stdout and stderr piped. Its
/// first process leads its own session and process group, so that a signal
/// to that group reaches no process of the App Server.
#[derive(Debug)]
pub struct Process {
    pub child: tokio::process::Child,
    /// The first process's ID, which is also its process group's ID.
    pub pid: u32,
    /// Taken by the one that lets the launcher entry go on.
    pub start: Option<StartPipe>,
}

/// The App Server's end of a launch's start pipe, close-on-exec: the
/// launcher entry runs its command, or a file tool its work, only once one
/// byte arrives on [`START_DESCRIPTOR`], and exits at the pipe's end, which
/// dropping this without [`StartPipe::release`] makes.
#[derive(Debug)]
pub struct StartPipe(io::PipeWriter);

impl StartPipe {
    /// Writes the byte that lets the launcher entry go on.
    pub fn release(mut self) -> io::Result<()> {
        self.0.write_all(&[0])
    }
}

#[derive(Debug)]
pub enum LaunchError {
    /// The Codex crates cannot build the sandboxed command for these
    /// permissions: `unsupported_policy`, before the command runs.
    Rejected(PolicyRejection),
    /// The sandboxed command could not be started.
    Spawn(io::Error),
}

/// The refusal of every `bwrap` while one on this process's `PATH` that this
/// user could have planted comes before every one it cannot change
/// (`linux.rs`), naming that path.
#[cfg(target_os = "linux")]
pub fn planted_launcher() -> Option<String> {
    linux::planted_launcher()
}

/// The trusted programs of a call whose working directory is `cwd`: the
/// running `tetral`, the cua-driver that `config.toml` records, canonical
/// when it exists and as recorded otherwise, and, on Linux, every `bwrap`
/// that its launch could select, each canonical and found with `stat`, none
/// run. The profile keeps each of them read-only (`policy::TranslationInputs`).
pub fn trusted_programs(tetral: &Path, cua_driver: Option<&Path>, cwd: &Path) -> Vec<PathBuf> {
    let mut programs = vec![tetral.to_path_buf()];
    if let Some(driver) = cua_driver {
        let driver = driver
            .canonicalize()
            .unwrap_or_else(|_| driver.to_path_buf());
        if !programs.contains(&driver) {
            programs.push(driver);
        }
    }
    #[cfg(target_os = "linux")]
    for bwrap in linux::bwrap_candidates(tetral, cwd) {
        if !programs.contains(&bwrap) {
            programs.push(bwrap);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = cwd;
    programs
}

impl Isolation {
    pub fn new() -> Isolation {
        Isolation {
            probe_timeout: PROBE_TIMEOUT,
            probe_output_limit: PROBE_OUTPUT_LIMIT_BYTES,
        }
    }

    pub fn with_probe_bounds(probe_timeout: Duration, probe_output_limit: usize) -> Isolation {
        Isolation {
            probe_timeout,
            probe_output_limit,
        }
    }

    /// Checks at each execution that the platform isolation can run this
    /// task: otherwise the call is rejected `sandbox_unavailable`, and no
    /// process of the task starts. `cwd` is the binding's canonical working
    /// directory and `tetral` the running executable.
    pub async fn check(
        &self,
        permissions: &TaskPermissions,
        cwd: &Path,
        tetral: &Path,
    ) -> Result<Available, PolicyRejection> {
        #[cfg(target_os = "linux")]
        let checked = linux::check(self, permissions, cwd, tetral).await;
        #[cfg(target_os = "macos")]
        let checked = {
            let _ = (permissions, cwd, tetral);
            macos::check(self).await
        };
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let checked = {
            let _ = (permissions, cwd, tetral);
            Err("this system has no platform isolation")
        };
        checked
            .map(|()| Available(()))
            .map_err(|message| PolicyRejection {
                code: RejectionCode::SandboxUnavailable,
                message: message.into(),
            })
    }

    /// Launches preparation through the same restricted final exec entry.
    /// The caller reads the final entry's startup status separately from Git
    /// output so a failed exec cannot look like a Git query with no matches.
    pub async fn preparation(
        &self,
        permissions: &TaskPermissions,
        tetral: &Path,
        arguments: &[&str],
        status: OwnedFd,
        low_speed_window: Duration,
    ) -> Result<Process, LaunchError> {
        let cwd = Path::new("/");
        let available = self
            .check(permissions, cwd, tetral)
            .await
            .map_err(LaunchError::Rejected)?;
        let mut argv = vec![tetral.display().to_string()];
        argv.extend(
            crate::child::exec::program_arguments(cwd, "git", arguments)
                .into_iter()
                .map(|arg| arg.to_string_lossy().into_owned()),
        );
        self.launch(
            &available,
            Command {
                argv,
                env: Some(crate::env::preparation_environment(low_speed_window)),
                directory: None,
            },
            permissions,
            cwd,
            tetral,
            Some(status),
        )
        .await
        .map(|mut process| {
            // Preparation's Git has no record to wait for.
            if let Some(start) = process.start.take() {
                let _ = start.release();
            }
            process
        })
    }

    /// Starts `command` inside the sandbox that the task's permissions
    /// describe, with `cwd`, the binding's canonical working directory, as
    /// the directory the permissions resolve against, and `passed`, when
    /// given, as its descriptor [`PASSED_DESCRIPTOR`]; its start pipe's read
    /// end is its [`START_DESCRIPTOR`]. On Linux the sandboxed
    /// command runs `tetral` as `codex-linux-sandbox`, which then runs the
    /// command under `bwrap` and seccomp; on macOS `sandbox-exec` runs it
    /// under the Seatbelt profile with the Unix-socket rules appended.
    pub async fn launch(
        &self,
        _available: &Available,
        command: Command,
        permissions: &TaskPermissions,
        cwd: &Path,
        tetral: &Path,
        passed: Option<OwnedFd>,
    ) -> Result<Process, LaunchError> {
        let environment = command
            .env
            .as_ref()
            .map(crate::env::command_environment_file)
            .transpose()
            .map_err(LaunchError::Spawn)?;
        let (start_reader, start_writer) = io::pipe().map_err(LaunchError::Spawn)?;
        // Sources above every destination cannot overwrite one another when
        // pre_exec installs the launch-status, environment and start
        // descriptors.
        let duplicate = |fd: OwnedFd| {
            rustix::io::fcntl_dupfd_cloexec(&fd, START_DESCRIPTOR + 1)
                .map_err(|error| LaunchError::Spawn(error.into()))
        };
        let passed = passed.map(duplicate).transpose()?;
        let environment = environment.map(OwnedFd::from).map(duplicate).transpose()?;
        let start_reader = duplicate(OwnedFd::from(start_reader))?;
        let mut launcher_environment = crate::env::launcher_environment(std::env::vars_os());
        // A launcher without network cannot reach the test harness because
        // seccomp refuses connect. Production builds have no harness value.
        if permissions.profile().network_sandbox_policy().is_enabled()
            && let Some((name, value)) = state::seams::harness_variable()
        {
            launcher_environment.insert(name.to_owned(), value.to_string_lossy().into_owned());
        }
        // The profile and the transform read the task's paths, so they run
        // off the runtime's workers.
        let profile = permissions.profile().clone();
        let cwd = cwd.to_path_buf();
        let tetral = tetral.to_path_buf();
        let directory = command.directory.clone();
        let argv = command.argv;
        let request = tokio::task::spawn_blocking(move || {
            transform(
                profile,
                &cwd,
                &tetral,
                directory,
                argv,
                launcher_environment,
            )
        })
        .await
        .map_err(|error| LaunchError::Spawn(io::Error::other(error)))??;
        #[cfg(target_os = "macos")]
        let request = {
            let mut request = request;
            macos::append_socket_rules(&mut request.command, permissions)
                .map_err(LaunchError::Spawn)?;
            request
        };
        let Some((program, args)) = request.command.split_first() else {
            return Err(LaunchError::Spawn(io::Error::other(
                "the sandboxed command is empty",
            )));
        };
        let mut child = tokio::process::Command::new(program);
        child
            .args(args)
            .env_clear()
            .envs(&request.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(directory) = &command.directory {
            child.current_dir(directory);
        }
        if let Some(arg0) = &request.arg0 {
            child.arg0(arg0);
        }
        let passed_raw = passed.as_ref().map(AsRawFd::as_raw_fd);
        let environment_raw = environment.as_ref().map(AsRawFd::as_raw_fd);
        let start_raw = start_reader.as_raw_fd();
        let parent = rustix::process::getpid().as_raw_nonzero().get();
        // SAFETY: the closure runs between fork and exec and makes only
        // system calls, which allocate nothing.
        unsafe {
            child.pre_exec(move || prepare_child(passed_raw, environment_raw, start_raw, parent));
        }
        state::seams::count("process.spawn");
        let child = child.spawn().map_err(LaunchError::Spawn)?;
        drop((passed, environment, start_reader));
        let pid = child
            .id()
            .ok_or_else(|| LaunchError::Spawn(io::Error::other("the command has no process ID")))?;
        Ok(Process {
            child,
            pid,
            start: Some(StartPipe(start_writer)),
        })
    }
}

/// The sandboxed form of `argv`, to start in `directory` or the App Server's
/// own working directory, under `profile` resolved against `cwd`.
fn transform(
    profile: PermissionProfile,
    cwd: &Path,
    tetral: &Path,
    directory: Option<PathBuf>,
    argv: Vec<String>,
    launcher_environment: HashMap<String, String>,
) -> Result<SandboxExecRequest, LaunchError> {
    let manager = SandboxManager::new();
    #[cfg(target_os = "linux")]
    let profile = &linux::launch_profile(&profile);
    #[cfg(target_os = "macos")]
    let profile = &macos::launch_profile(&profile);
    let sandbox = manager.select_initial(
        profile,
        SandboxablePreference::Require,
        codex_sandboxing::SandboxType::None,
        false,
    );
    let cwd_uri = PathUri::from_host_native_path(cwd).map_err(LaunchError::Spawn)?;
    let start = match directory {
        Some(directory) => directory,
        None => std::env::current_dir().map_err(LaunchError::Spawn)?,
    };
    let start_uri = PathUri::from_host_native_path(&start).map_err(LaunchError::Spawn)?;
    let mut argv = argv.into_iter();
    let program = argv.next().unwrap_or_default();
    manager
        .transform(SandboxTransformRequest {
            command: SandboxCommand {
                program: program.into(),
                args: argv.collect(),
                cwd: start_uri,
                env: launcher_environment,
                managed_network: None,
                additional_permissions: None,
            },
            permissions: profile,
            sandbox,
            enforce_managed_network: false,
            environment_id: None,
            network: None,
            sandbox_policy_cwd: &cwd_uri,
            sandbox_exe: Some(tetral),
            use_legacy_landlock: false,
            windows_sandbox_level: WindowsSandboxLevel::Disabled,
        })
        .map_err(|_| {
            LaunchError::Rejected(PolicyRejection {
                code: RejectionCode::UnsupportedPolicy,
                message: "sandbox_policy cannot be enforced for this command".to_owned(),
            })
        })
}

/// The child's side of a launch, as Codex's pipe spawn prepares it: a new
/// session, so that the command has no terminal and its process group is its
/// own; on Linux, SIGTERM once the App Server `parent` ends; and no
/// descriptors but its pipes, launch status, optional environment data and
/// start pipe.
fn prepare_child(
    passed: Option<RawFd>,
    environment: Option<RawFd>,
    start: RawFd,
    parent: libc::pid_t,
) -> io::Result<()> {
    codex_utils_pty::process_group::detach_from_tty()?;
    codex_utils_pty::process_group::set_parent_death_signal(parent)?;
    prepare_descriptors(passed, environment, Some(start))
}

/// Installs the private data channels without changing process identity.
/// Every source descriptor has already been duplicated above destinations.
fn prepare_descriptors(
    passed: Option<RawFd>,
    environment: Option<RawFd>,
    start: Option<RawFd>,
) -> io::Result<()> {
    close_on_exec_from(PASSED_DESCRIPTOR);
    for (source, destination) in [
        (passed, PASSED_DESCRIPTOR),
        (environment, ENVIRONMENT_DESCRIPTOR),
        (start, START_DESCRIPTOR),
    ] {
        if let Some(raw) = source {
            // SAFETY: sources are open above the reserved destinations;
            // dup2 installs one and clears its close-on-exec flag.
            let placed = unsafe { libc::dup2(raw, destination) };
            if placed < 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

/// Leaves no descriptor from `first` (3 at every call) open across exec,
/// while the spawn's own close-on-exec pipe still reports a failed exec. On
/// Linux every one is marked close-on-exec rather than closed; where
/// `close_range` is refused, by a kernel before 5.11 or a seccomp filter,
/// each descriptor that `/proc/self/fd` lists is marked instead
/// (`linux::mark_listed_close_on_exec`), as Codex's spawn does. macOS has no
/// `close_range` and sets close-on-exec on a new pipe or socket only after
/// creating it, so one made while another thread forks arrives inheritable:
/// Codex's sweep closes every listed descriptor above 2 that is not
/// close-on-exec.
fn close_on_exec_from(first: RawFd) {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: close_range takes integers only and changes only this
        // child's descriptors.
        let marked = unsafe {
            libc::syscall(
                libc::SYS_close_range,
                first as libc::c_uint,
                libc::c_uint::MAX,
                libc::CLOSE_RANGE_CLOEXEC,
            )
        } == 0;
        if !marked {
            linux::mark_listed_close_on_exec(first);
        }
    }
    #[cfg(target_os = "macos")]
    {
        let _ = first;
        codex_utils_pty::pty::close_inherited_fds_except(&[]);
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    use super::close_on_exec_from;

    #[cfg(target_os = "linux")]
    #[test]
    fn command_environment_is_data_until_the_final_exec() {
        let directory = tempfile::tempdir().unwrap();
        let requested = std::collections::BTreeMap::from([
            ("HANDOFF_VALUE".into(), "requested value".into()),
            ("TERM".into(), "xterm".into()),
            ("TETRAL_SECRET".into(), "excluded".into()),
        ]);
        let environment = crate::env::command_environment(
            [("PATH".into(), "/usr/bin:/bin".into())],
            Some(&requested),
            directory.path(),
            None,
        )
        .unwrap();
        let environment = crate::env::command_environment_file(&environment).unwrap();
        let status = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/null")
            .unwrap();
        let (start, mut start_writer) = std::io::pipe().unwrap();
        std::io::Write::write_all(&mut start_writer, &[0]).unwrap();
        let above = |fd: &dyn std::os::fd::AsFd| {
            rustix::io::fcntl_dupfd_cloexec(fd, super::START_DESCRIPTOR + 1).unwrap()
        };
        let (environment, status, start) = (above(&environment), above(&status), above(&start));
        let environment_raw = environment.as_raw_fd();
        let status_raw = status.as_raw_fd();
        let start_raw = start.as_raw_fd();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "isolation::tests::command_environment_child",
                "--nocapture",
            ])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("TETRAL_ENVIRONMENT_TEST_CHILD", "1")
            .env("LAUNCHER_ONLY", "launcher value")
            .current_dir(directory.path());
        // SAFETY: descriptor preparation makes only system calls and all
        // source descriptors remain owned until the child has started.
        unsafe {
            command.pre_exec(move || {
                super::prepare_descriptors(Some(status_raw), Some(environment_raw), Some(start_raw))
            });
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("environment handoff complete"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn command_environment_child() {
        if std::env::var_os("TETRAL_ENVIRONMENT_TEST_CHILD").is_none() {
            return;
        }
        assert!(std::env::var_os("HANDOFF_VALUE").is_none());
        assert_eq!(std::env::var("LAUNCHER_ONLY").unwrap(), "launcher value");
        let directory = std::env::current_dir().unwrap();
        let script = r#"
            test "$HANDOFF_VALUE" = 'requested value' &&
            test "$TERM:$NO_COLOR:$PAGER:$GIT_PAGER" = 'dumb:1:cat:cat' &&
            test "$TMPDIR" = "$PWD" &&
            test -z "${LAUNCHER_ONLY+x}" &&
            test -z "${TETRAL_SECRET+x}" &&
            test -z "${TETRAL_ENVIRONMENT_TEST_CHILD+x}" &&
            test ! -e /proc/self/fd/3 &&
            test ! -e /proc/self/fd/4 &&
            test ! -e /proc/self/fd/5 &&
            printf 'environment handoff complete\n'
        "#;
        let arguments = crate::child::exec::arguments(&directory, "/bin/sh", script);
        let result = crate::child::run(&arguments[1..]);
        panic!("the command entry returned {result:?}");
    }

    /// Makes `close_range` fail with `EPERM` in this process, through a
    /// seccomp filter; it runs between fork and exec and allocates nothing.
    #[cfg(target_os = "linux")]
    fn refuse_close_range() -> std::io::Result<()> {
        let mut filter = [
            libc::sock_filter {
                code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
                jt: 0,
                jf: 0,
                k: std::mem::offset_of!(libc::seccomp_data, nr) as u32,
            },
            libc::sock_filter {
                code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
                jt: 0,
                jf: 1,
                k: libc::SYS_close_range as u32,
            },
            libc::sock_filter {
                code: (libc::BPF_RET | libc::BPF_K) as u16,
                jt: 0,
                jf: 0,
                k: libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
            },
            libc::sock_filter {
                code: (libc::BPF_RET | libc::BPF_K) as u16,
                jt: 0,
                jf: 0,
                k: libc::SECCOMP_RET_ALLOW,
            },
        ];
        let program = libc::sock_fprog {
            len: filter.len() as u16,
            filter: filter.as_mut_ptr(),
        };
        // SAFETY: the kernel copies the filter, which lives on this stack,
        // before prctl returns.
        let installed = unsafe {
            libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0
                && libc::prctl(
                    libc::PR_SET_SECCOMP,
                    libc::SECCOMP_MODE_FILTER,
                    &raw const program,
                    0,
                    0,
                ) == 0
        };
        if installed {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[test]
    fn an_inherited_descriptor_is_closed_at_exec() {
        let (pipe, _writer) = std::io::pipe().unwrap();
        // An inheritable copy above every descriptor of the test runner.
        // SAFETY: F_DUPFD makes a new descriptor and replaces none.
        let inherited = unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_DUPFD, 200) };
        assert!(inherited >= 200, "{}", std::io::Error::last_os_error());
        let seen = |prepare: Option<bool>| {
            let mut command = Command::new("/bin/sh");
            command.arg("-c").arg(format!(
                "[ -e /dev/fd/{inherited} ] && echo inherited || echo closed"
            ));
            if let Some(refused) = prepare {
                // SAFETY: the closure makes only system calls.
                unsafe {
                    command.pre_exec(move || {
                        #[cfg(target_os = "linux")]
                        if refused {
                            refuse_close_range()?;
                        }
                        #[cfg(target_os = "macos")]
                        let _ = refused;
                        close_on_exec_from(3);
                        Ok(())
                    });
                }
            }
            String::from_utf8(command.output().unwrap().stdout).unwrap()
        };
        assert_eq!(seen(None), "inherited\n");
        assert_eq!(seen(Some(false)), "closed\n");
        // Without close_range, as a kernel before 5.11 or a seccomp filter.
        #[cfg(target_os = "linux")]
        assert_eq!(seen(Some(true)), "closed\n");
        // SAFETY: closes the copy this test made.
        unsafe { libc::close(inherited) };
    }
}
