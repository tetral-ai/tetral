//! The cua-driver version this release was tested with, and the version the
//! recorded driver reports, read at each start of `cua` before
//! `<driver> mcp`.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;

/// The cua-driver release that `cli/UPSTREAM.md` pins.
pub const CUA_DRIVER_TESTED_VERSION: &str = "0.38.0";

/// How long `<driver> --version` may run.
const LIMIT: Duration = Duration::from_secs(5);

/// The `<v>` of `<driver> --version`'s single output line `cua-driver <v>`,
/// run in `cwd` with the environment `<driver> mcp` gets; `None` for any
/// other output, a non-zero exit, a failed spawn or no exit within
/// [`LIMIT`] on the seam clock, each reading of which is the test seam
/// counter `mcp.version_checked`.
pub(crate) async fn read(driver: &Path, cwd: &Path) -> Option<String> {
    let deadline = state::seams::now() + LIMIT;
    let child = Command::new(driver)
        .arg("--version")
        .current_dir(cwd)
        .env_remove(state::paths::ROOT_VARIABLE)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;
    let output = tokio::select! {
        output = child.wait_with_output() => output.ok()?,
        () = state::seams::sleep_until_counted(deadline, "mcp.version_checked") => return None,
    };
    if !output.status.success() {
        return None;
    }
    parse(&output.stdout)
}

fn parse(output: &[u8]) -> Option<String> {
    let output = std::str::from_utf8(output).ok()?;
    let line = output.strip_suffix('\n').unwrap_or(output);
    let version = line.strip_prefix("cua-driver ")?;
    (!version.is_empty() && !line.contains('\n')).then(|| version.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tested_version_is_the_release_upstream_md_pins() {
        let upstream = include_str!("../../../UPSTREAM.md");
        let row = upstream
            .lines()
            .find(|line| line.starts_with("| cua-driver"))
            .expect("UPSTREAM.md has a cua-driver row");
        // The columns are source, origin, pinned version, ...
        let pinned = row
            .split(" | ")
            .nth(2)
            .and_then(|column| column.strip_prefix("`cua-driver-rs-v"))
            .and_then(|rest| rest.split('`').next())
            .expect("the cua-driver row pins a cua-driver-rs-v<version> tag");
        assert_eq!(CUA_DRIVER_TESTED_VERSION, pinned);
    }

    #[test]
    fn only_a_single_line_cua_driver_v_gives_a_version() {
        for (output, version) in [
            (&b"cua-driver 0.38.0\n"[..], Some("0.38.0")),
            (b"cua-driver 0.38.0", Some("0.38.0")),
            (b"cua-driver 0.38.0\nmore\n", None),
            (b"cua-driver \n", None),
            (b"cua-driver-rs 0.38.0\n", None),
            (b"", None),
            (b"cua-driver \xff\n", None),
        ] {
            assert_eq!(parse(output).as_deref(), version, "{output:?}");
        }
    }
}
