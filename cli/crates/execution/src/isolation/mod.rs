//! The one restricted path that every process of a tool call runs through:
//! the availability check at each execution, then the launch, in which
//! Codex's sandbox manager compiles the task's filesystem and network
//! permissions into one sandboxed command. No grant skips it.

#[cfg(feature = "test-seams")]
pub mod harness;
#[cfg(target_os = "linux")]
mod linux;

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use codex_protocol::config_types::WindowsSandboxLevel;
use codex_sandboxing::{
    SandboxCommand, SandboxManager, SandboxTransformRequest, SandboxablePreference,
};
use codex_utils_path_uri::PathUri;
use protocol::tool::RejectionCode;

use crate::policy::{PolicyRejection, TaskPermissions};

/// How long a probe of a `bwrap` candidate may run.
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

/// A tool's command: the program and its arguments, exactly the environment
/// it gets, and the directory it starts in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub argv: Vec<String>,
    pub env: HashMap<String, String>,
    /// `None` starts it in the App Server's own working directory, the one
    /// against which the availability check and Codex's helper both resolve
    /// `PATH`, so that the check judges the `bwrap` the launch runs; a task
    /// enters its own directory inside the sandbox (`child`).
    pub directory: Option<PathBuf>,
}

/// The descriptor on which a launched command finds the descriptor that its
/// launch passes it.
pub const PASSED_DESCRIPTOR: RawFd = 3;

/// A started sandboxed command, with its stdin, stdout and stderr piped. Its
/// first process leads its own session and process group, so that a signal
/// to that group reaches no process of the App Server.
#[derive(Debug)]
pub struct Process {
    pub child: tokio::process::Child,
    /// The first process's ID, which is also its process group's ID.
    pub pid: u32,
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
/// running `tetral` and, on Linux, every `bwrap` that its launch could
/// select, each canonical and found with `stat`, none run. The profile keeps
/// each of them read-only (`policy::TranslationInputs`).
pub fn trusted_programs(tetral: &Path, cwd: &Path) -> Vec<PathBuf> {
    let mut programs = vec![tetral.to_path_buf()];
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
        #[cfg(not(target_os = "linux"))]
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

    /// Starts `command` inside the sandbox that the task's permissions
    /// describe, with `cwd`, the binding's canonical working directory, as
    /// the directory the permissions resolve against, and `passed`, when
    /// given, as its descriptor [`PASSED_DESCRIPTOR`]. The sandboxed command
    /// runs `tetral` as `codex-linux-sandbox`, which then runs the command
    /// under `bwrap` and seccomp.
    pub async fn launch(
        &self,
        _available: &Available,
        command: Command,
        permissions: &TaskPermissions,
        cwd: &Path,
        tetral: &Path,
        passed: Option<OwnedFd>,
    ) -> Result<Process, LaunchError> {
        let manager = SandboxManager::new();
        #[cfg(target_os = "linux")]
        let profile = &linux::launch_profile(permissions.profile());
        #[cfg(not(target_os = "linux"))]
        let profile = permissions.profile();
        let sandbox = manager.select_initial(
            profile,
            SandboxablePreference::Require,
            codex_sandboxing::SandboxType::None,
            false,
        );
        let cwd_uri = PathUri::from_host_native_path(cwd).map_err(LaunchError::Spawn)?;
        let start = match &command.directory {
            Some(directory) => directory.clone(),
            None => std::env::current_dir().map_err(LaunchError::Spawn)?,
        };
        let start_uri = PathUri::from_host_native_path(&start).map_err(LaunchError::Spawn)?;
        let mut argv = command.argv.into_iter();
        let program = argv.next().unwrap_or_default();
        let request = manager
            .transform(SandboxTransformRequest {
                command: SandboxCommand {
                    program: program.into(),
                    args: argv.collect(),
                    cwd: start_uri,
                    env: command.env,
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
            })?;
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
        let parent = rustix::process::getpid().as_raw_nonzero().get();
        // SAFETY: the closure runs between fork and exec and makes only
        // system calls, which allocate nothing.
        unsafe {
            child.pre_exec(move || prepare_child(passed_raw, parent));
        }
        state::seams::count("process.spawn");
        let child = child.spawn().map_err(LaunchError::Spawn)?;
        drop(passed);
        let pid = child
            .id()
            .ok_or_else(|| LaunchError::Spawn(io::Error::other("the command has no process ID")))?;
        Ok(Process { child, pid })
    }
}

/// The child's side of a launch, as Codex's pipe spawn prepares it: a new
/// session, so that the command has no terminal and its process group is its
/// own; on Linux, SIGTERM once the App Server `parent` ends; and no
/// descriptor but its pipes and the passed one, at [`PASSED_DESCRIPTOR`].
fn prepare_child(passed: Option<RawFd>, parent: libc::pid_t) -> io::Result<()> {
    codex_utils_pty::process_group::detach_from_tty()?;
    codex_utils_pty::process_group::set_parent_death_signal(parent)?;
    let kept = match passed {
        Some(raw) => {
            // SAFETY: `raw` is open in this process until exec; dup2 onto
            // the passed descriptor clears its close-on-exec flag, and the
            // same descriptor needs the flag cleared by hand.
            let placed = unsafe {
                if raw == PASSED_DESCRIPTOR {
                    libc::fcntl(raw, libc::F_SETFD, 0)
                } else {
                    libc::dup2(raw, PASSED_DESCRIPTOR)
                }
            };
            if placed < 0 {
                return Err(io::Error::last_os_error());
            }
            PASSED_DESCRIPTOR
        }
        None => PASSED_DESCRIPTOR - 1,
    };
    close_on_exec_from(kept + 1);
    Ok(())
}

/// Marks every descriptor from `first` up close-on-exec, where the system
/// can, rather than closing it: the spawn still reports a failed exec on its
/// own close-on-exec pipe. One left unmarked is close-on-exec already in a
/// process that opens files only with that flag.
fn close_on_exec_from(first: RawFd) {
    #[cfg(target_os = "linux")]
    // SAFETY: close_range takes integers only and changes only this child's
    // descriptors.
    unsafe {
        libc::syscall(
            libc::SYS_close_range,
            first as libc::c_uint,
            libc::c_uint::MAX,
            libc::CLOSE_RANGE_CLOEXEC,
        );
    }
    #[cfg(not(target_os = "linux"))]
    let _ = first;
}
