//! A local server's process, `<driver> mcp`: spawned without a sandbox in a
//! new process group, in the working directory it serves, with the App
//! Server's environment less `TETRAL_HOME`. Its stdin and stdout are the MCP
//! stdio transport; its stderr is read as it arrives and each chunk is
//! recorded as its size, never its content.

use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use rustix::process::{Pid, Signal};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// A spawned process with its transport pipes.
pub(crate) struct Process {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    group: Pid,
}

/// Spawns `<driver> mcp` in `cwd`, recording its stderr under `server`.
pub(crate) fn spawn(driver: &Path, cwd: &Path, server: &'static str) -> std::io::Result<Process> {
    let mut child = Command::new(driver)
        .arg("mcp")
        .current_dir(cwd)
        .env_remove(state::paths::ROOT_VARIABLE)
        .process_group(0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = child.id().expect("a child just spawned has its process ID");
    let group = Pid::from_raw(pid as i32).expect("a process ID is positive");
    let stdin = child.stdin.take().expect("stdin is piped");
    let stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");
    tokio::spawn(async move {
        let mut chunk = vec![0; 8_192];
        while let Ok(read) = stderr.read(&mut chunk).await {
            if read == 0 {
                break;
            }
            observe::info!(
                "mcp.server_stderr",
                mcp.server = observe::Id(server),
                mcp.stderr_bytes = read
            );
        }
    });
    Ok(Process {
        child,
        stdin,
        stdout,
        group,
    })
}

impl Process {
    pub fn group(&self) -> Pid {
        self.group
    }
}

/// Stops the process whose group is `group` and whose leader is `child`:
/// SIGTERM to the group and, when any member remains `grace` later on the
/// seam clock, SIGKILL to the group. Returns once the leader is reaped and
/// no member remains unsignalled by SIGKILL.
pub(crate) async fn stop(child: &mut Child, group: Pid, grace: Duration) {
    let deadline = state::seams::now() + grace;
    let _ = rustix::process::kill_process_group(group, Signal::TERM);
    let leader_exited = tokio::select! {
        _ = child.wait() => true,
        () = state::seams::sleep_until_counted(deadline, "mcp.stop_checked") => false,
    };
    if leader_exited {
        if !group_remains(group) {
            return;
        }
        state::seams::sleep_until_counted(deadline, "mcp.stop_checked").await;
        if !group_remains(group) {
            return;
        }
    }
    let _ = rustix::process::kill_process_group(group, Signal::KILL);
    let _ = child.wait().await;
}

/// Whether any process of `group` remains.
pub(crate) fn group_remains(group: Pid) -> bool {
    rustix::process::test_kill_process_group(group).is_ok()
}

/// How a process ended: `exit code <n>` or `signal <n>`.
pub(crate) fn describe(status: ExitStatus) -> String {
    match (status.code(), status.signal()) {
        (Some(code), _) => format!("exit code {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => status.to_string(),
    }
}
