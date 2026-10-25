//! Linux: the `bwrap` that a launch uses, chosen as Codex's helper chooses
//! it, and Codex's check that it can create the namespaces a sandbox needs.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use codex_protocol::permissions::FileSystemSandboxPolicy;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::Isolation;
use crate::policy::TaskPermissions;

/// The digest of the bundled `bwrap`, built into release builds. A build
/// without it accepts the bundled `bwrap` unchecked, as Codex does.
const BUNDLED_BWRAP_SHA256: Option<&str> = option_env!("CODEX_BWRAP_SHA256");

/// The stderr texts with which Codex recognizes a host that refuses the user
/// namespaces `bwrap` needs.
const USER_NAMESPACE_FAILURES: [&str; 4] = [
    "loopback: Failed RTM_NEWADDR",
    "loopback: Failed RTM_NEWLINK",
    "setting up uid map: Permission denied",
    "No permissions to create a new namespace",
];

pub(super) async fn check(
    isolation: &Isolation,
    permissions: &TaskPermissions,
    cwd: &Path,
    tetral: &Path,
) -> Result<(), &'static str> {
    let file_system = permissions.profile().file_system_sandbox_policy();
    let Some(bwrap) = select(isolation, &file_system, cwd, tetral).await else {
        return Err(
            "no bwrap is available: none on PATH passes the capability probe, and none beside tetral matches its digest",
        );
    };
    if is_wsl1() {
        return Err("WSL1 cannot create the namespaces that bwrap needs");
    }
    let probe = isolation
        .probe(
            &bwrap,
            &[
                "--unshare-user",
                "--unshare-net",
                "--ro-bind",
                "/",
                "/",
                "/bin/true",
            ],
        )
        .await;
    // As in Codex, a probe that cannot start or does not finish in time
    // counts as available; only a refusal of the namespaces does not.
    match probe {
        Some(output)
            if !output.success
                && USER_NAMESPACE_FAILURES
                    .iter()
                    .any(|failure| output.stderr.contains(failure)) =>
        {
            Err("bwrap cannot create user namespaces on this machine")
        }
        _ => Ok(()),
    }
}

/// The system `bwrap` when one on `PATH` passes the capability probe,
/// otherwise the bundled one beside `tetral` when its digest matches.
async fn select(
    isolation: &Isolation,
    file_system: &FileSystemSandboxPolicy,
    cwd: &Path,
    tetral: &Path,
) -> Option<PathBuf> {
    // Skips candidates inside the App Server's working directory and those
    // the call's permissions could replace.
    if let Some(system) = codex_sandboxing::find_system_bwrap_in_path(file_system, cwd)
        && let Some(help) = isolation.probe(&system, &["--help"]).await
        && help.lists("--as-pid-1")
        && help.lists("--perms")
    {
        return Some(system);
    }
    let bundled = codex_linux_sandbox::find_bundled_bwrap_for_exe(tetral, cwd, |name| {
        std::env::var_os(name)
    })?
    .into_path_buf();
    digest_matches(&bundled).then_some(bundled)
}

fn digest_matches(bwrap: &Path) -> bool {
    let Some(expected) = BUNDLED_BWRAP_SHA256 else {
        return true;
    };
    let expected = expected.to_ascii_lowercase();
    // Codex reads an all-zero digest as no digest.
    if expected.bytes().all(|digit| digit == b'0') {
        return true;
    }
    let Ok(mut file) = File::open(bwrap) else {
        return false;
    };
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => hasher.update(&buffer[..read]),
            Err(_) => return false,
        }
    }
    let actual: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    actual == expected
}

fn is_wsl1() -> bool {
    std::fs::read_to_string("/proc/version").is_ok_and(|version| proc_version_is_wsl1(&version))
}

/// Codex's reading of `/proc/version`: a `wsl<digits>` marker names the
/// version, and otherwise a Microsoft kernel that is not WSL2's standard
/// one is WSL1.
fn proc_version_is_wsl1(version: &str) -> bool {
    let version = version.to_ascii_lowercase();
    let mut remaining = version.as_str();
    while let Some(marker) = remaining.find("wsl") {
        let digits: String = remaining[marker + 3..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if let Ok(number) = digits.parse::<u32>() {
            return number == 1;
        }
        remaining = &remaining[marker + 3..];
    }
    version.contains("microsoft") && !version.contains("microsoft-standard")
}

struct ProbeOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

impl ProbeOutput {
    fn lists(&self, option: &str) -> bool {
        self.stdout.contains(option) || self.stderr.contains(option)
    }
}

impl Isolation {
    /// Runs `program` with an empty environment, reading at most the output
    /// limit of each stream; `None` when it cannot start or does not exit
    /// within the probe timeout, after which it is killed.
    async fn probe(&self, program: &Path, args: &[&str]) -> Option<ProbeOutput> {
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
