//! A run of a program that the availability check judges: an empty
//! environment, each output stream read up to the probe's limit, and the
//! probe's timeout on the seam clock.

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

impl Isolation {
    /// Runs `program` with an empty environment, reading at most the output
    /// limit of each stream; `None` when it cannot start or does not exit
    /// within the probe timeout, after which it is killed.
    pub(super) async fn probe(&self, program: &Path, args: &[&str]) -> Option<ProbeOutput> {
        let mut child = tokio::process::Command::new(program)
            .args(args)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .ok()?;
        let stdout = child.stdout.take()?;
        let stderr = child.stderr.take()?;
        let limit = self.probe_output_limit;
        let finished = async {
            let (stdout, stderr) =
                tokio::join!(read_bounded(stdout, limit), read_bounded(stderr, limit));
            let status = child.wait().await.ok()?;
            Some(ProbeOutput {
                success: status.success(),
                stdout,
                stderr,
            })
        };
        let deadline = state::seams::now() + self.probe_timeout;
        tokio::select! {
            output = finished => output,
            () = state::seams::sleep_until(deadline) => None,
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
