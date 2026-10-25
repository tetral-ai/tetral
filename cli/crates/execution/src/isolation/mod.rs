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
use std::path::{Path, PathBuf};
use std::time::Duration;

use codex_protocol::config_types::WindowsSandboxLevel;
use codex_sandboxing::{
    SandboxCommand, SandboxManager, SandboxTransformRequest, SandboxablePreference,
};
use codex_utils_path_uri::PathUri;
use codex_utils_pty::SpawnedProcess;
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

/// A tool's command: the program and its arguments, and exactly the
/// environment it gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub argv: Vec<String>,
    pub env: HashMap<String, String>,
}

#[derive(Debug)]
pub enum LaunchError {
    /// The Codex crates cannot build the sandboxed command for these
    /// permissions: `unsupported_policy`, before the command runs.
    Rejected(PolicyRejection),
    /// The sandboxed command could not be started.
    Spawn(io::Error),
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
                message: message.to_owned(),
            })
    }

    /// Starts `command` inside the sandbox that the task's permissions
    /// describe, with `cwd` as its working directory and as the directory
    /// the permissions resolve against. The sandboxed command runs `tetral`
    /// as `codex-linux-sandbox`, which then runs the command under `bwrap`
    /// and seccomp.
    pub async fn launch(
        &self,
        _available: &Available,
        command: Command,
        permissions: &TaskPermissions,
        cwd: &Path,
        tetral: &Path,
    ) -> Result<SpawnedProcess, LaunchError> {
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
        let mut argv = command.argv.into_iter();
        let program = argv.next().unwrap_or_default();
        let request = manager
            .transform(SandboxTransformRequest {
                command: SandboxCommand {
                    program: program.into(),
                    args: argv.collect(),
                    cwd: cwd_uri.clone(),
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
        state::seams::count("process.spawn");
        codex_utils_pty::spawn_pipe_process(program, args, cwd, &request.env, &request.arg0, &[])
            .await
            .map_err(|error| LaunchError::Spawn(io::Error::other(error)))
    }
}
