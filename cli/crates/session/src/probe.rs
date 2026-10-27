//! The supervision of one directory probe: the running executable spawned
//! as `tetral-cwd-probe` in its own process group, with `/` as its working
//! directory and an empty environment but for the test harness's variable
//! (`state::seams::harness_variable`), its input on stdin and its one line
//! of result on stdout. A task of its own writes the input, reads the line
//! and reaps the child. It is also the only one that kills the group, and
//! only before the reaping, so that a process group ID that the system may
//! already have reused is never signalled; the probe's slot stays taken
//! until the reaping. The test seam barrier `probe.started` stands between
//! the input's write and the read of the result, so that a test can hold a
//! spawned probe unreaped.

use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use rustix::process::{Pid, Signal};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use crate::probe_child::{Input, Output, PROBE_NAME};

/// The time a probe has from its spawn.
pub const PROBE_LIMIT: Duration = Duration::from_secs(5);

/// The bytes of a probe's stdout that are read; its one line is far shorter.
pub const OUTPUT_MAX_BYTES: u64 = 65_536;

/// The production probe entry. On Linux this resolves to the process
/// doing the spawn, so replacing or unlinking its installation cannot
/// change the version used for a later directory check.
pub fn probe_program() -> io::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        Ok(PathBuf::from("/proc/self/exe"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::env::current_exe()
    }
}

/// A spawned probe. Dropping it kills the probe, as [`Probe::kill`] does.
#[derive(Debug)]
pub struct Probe {
    kill: Option<oneshot::Sender<()>>,
}

impl Probe {
    /// A probe whose task has already reaped it.
    #[cfg(test)]
    pub(crate) fn reaped() -> Probe {
        Probe { kill: None }
    }

    /// Has the probe's task send SIGKILL to its process group, unless the
    /// task has already reaped it.
    pub fn kill(&mut self) {
        if let Some(kill) = self.kill.take() {
            let _ = kill.send(());
        }
    }
}

/// Spawns `program` as the probe of `input`, counted as the test seam
/// counter `probe.spawn`, and reads at most `output_max_bytes` of its stdout:
/// [`OUTPUT_MAX_BYTES`] but for tests. Once the child is reaped, `done` gets
/// its result, `None` when it wrote none that reads.
pub fn spawn(
    program: &Path,
    input: &Input,
    output_max_bytes: u64,
    done: impl FnOnce(Option<Output>) + Send + 'static,
) -> io::Result<Probe> {
    let line = serde_json::to_vec(input).map_err(io::Error::other)?;
    let mut command = tokio::process::Command::new(program);
    command
        .arg0(PROBE_NAME)
        .current_dir("/")
        .env_clear()
        .process_group(0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some((name, value)) = state::seams::harness_variable() {
        command.env(name, value);
    }
    let mut child = command.spawn()?;
    let group = child
        .id()
        .and_then(|id| Pid::from_raw(i32::try_from(id).ok()?))
        .ok_or_else(|| io::Error::other("the spawned probe has no process ID"))?;
    state::seams::count("probe.spawn");
    let (stdin, stdout) = (child.stdin.take(), child.stdout.take());
    let (kill, mut killed) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let read = async {
            if let Some(mut stdin) = stdin {
                let _ = stdin.write_all(&line).await;
            }
            state::seams::barrier("probe.started").await;
            let mut output = Vec::new();
            if let Some(stdout) = stdout {
                let _ = stdout.take(output_max_bytes).read_to_end(&mut output).await;
            }
            output
        };
        let reap = async {
            let output = read.await;
            let _ = child.wait().await;
            output
        };
        tokio::pin!(reap);
        // A dropped sender kills as a sent kill does.
        let output = tokio::select! {
            biased;
            _ = &mut killed => {
                let _ = rustix::process::kill_process_group(group, Signal::KILL);
                reap.await
            }
            output = &mut reap => output,
        };
        let result = output
            .split(|byte| *byte == b'\n')
            .next()
            .and_then(|line| serde_json::from_slice(line).ok());
        done(result);
    });
    Ok(Probe { kill: Some(kill) })
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[tokio::test]
    async fn a_result_line_over_the_output_bound_reads_as_no_result() {
        let directory = tempfile::tempdir().unwrap();
        let input = Input {
            cwd: "/project".into(),
            create_cwd: false,
            state_root: "/root".into(),
        };
        let entered = Output::Entered {
            cwd: "/project".to_owned(),
        };
        // Both lines parse as the same result when read whole.
        let short = r#"{"cwd":"/project"}"#.to_owned();
        let padding = "x".repeat(usize::try_from(OUTPUT_MAX_BYTES).unwrap());
        let long = format!(r#"{{"cwd":"/project","padding":"{padding}"}}"#);
        let rows = [
            (short.clone(), OUTPUT_MAX_BYTES, Some(entered)),
            (long, OUTPUT_MAX_BYTES, None),
            (short, 8, None),
        ];
        for (index, (line, limit, expected)) in rows.into_iter().enumerate() {
            let program = directory.path().join(format!("probe-{index}"));
            std::fs::write(&program, format!("#!/bin/sh\necho '{line}'\n")).unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
            let (result, done) = oneshot::channel();
            let _probe = spawn(&program, &input, limit, move |output| {
                let _ = result.send(output);
            })
            .unwrap();
            assert_eq!(
                done.await.unwrap(),
                expected,
                "line of {} bytes, bound {limit}",
                line.len()
            );
        }
    }
}
