//! A run of a program that the availability check judges: an empty
//! environment, each output stream read up to the probe's limit, and the
//! probe's timeout on the seam clock.

use std::io;
use std::path::Path;
use std::process::Stdio;

use tokio::io::{AsyncRead, AsyncReadExt};

use super::Isolation;

pub(super) struct ProbeOutput {
    pub(super) success: bool,
    #[cfg_attr(not(target_os = "linux"), expect(dead_code))]
    pub(super) stdout: String,
    #[cfg_attr(not(target_os = "linux"), expect(dead_code))]
    pub(super) stderr: String,
}

/// How a probe's run ended.
pub(super) enum Probed {
    Exited(ProbeOutput),
    /// It did not exit within the probe timeout, and was killed.
    TimedOut,
    /// It could not be started or waited for.
    Failed(#[cfg_attr(not(target_os = "macos"), expect(dead_code))] io::Error),
}

impl Isolation {
    /// Runs `program` with an empty environment, reading at most the output
    /// limit of each stream, and kills it at the probe timeout.
    pub(super) async fn probe(&self, program: &Path, args: &[&str]) -> Probed {
        let spawned = tokio::process::Command::new(program)
            .args(args)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn();
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) => return Probed::Failed(error),
        };
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        let limit = self.probe_output_limit;
        let finished = async {
            let (stdout, stderr) =
                tokio::join!(read_bounded(stdout, limit), read_bounded(stderr, limit));
            match child.wait().await {
                Ok(status) => Probed::Exited(ProbeOutput {
                    success: status.success(),
                    stdout,
                    stderr,
                }),
                Err(error) => Probed::Failed(error),
            }
        };
        let deadline = state::seams::now() + self.probe_timeout;
        tokio::select! {
            probed = finished => probed,
            () = state::seams::sleep_until(deadline) => Probed::TimedOut,
        }
    }
}

async fn read_bounded(stream: impl AsyncRead + Unpin, limit: usize) -> String {
    let mut bytes = Vec::new();
    let _ = stream
        .take(u64::try_from(limit).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)
        .await;
    String::from_utf8_lossy(&bytes).into_owned()
}
