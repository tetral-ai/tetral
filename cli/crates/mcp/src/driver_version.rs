//! The cua-driver version this release was tested with, and the version the
//! recorded driver reports, read at each start of `cua` before
//! `<driver> mcp`.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;

/// The cua-driver release that `cli/UPSTREAM.md` pins.
pub const CUA_DRIVER_TESTED_VERSION: &str = "0.38.0";

/// The `<v>` of `<driver> --version`'s single output line `cua-driver <v>`,
/// run in `cwd` with the environment `<driver> mcp` gets; `None` for any
/// other output, a non-zero exit, a failed spawn or no exit within `limit`
/// on the seam clock, each reading of which is the test seam counter
/// `mcp.version_checked`.
pub(crate) async fn read(driver: &Path, cwd: &Path, limit: Duration) -> Option<String> {
    let deadline = state::seams::now() + limit;
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
        // Every release tag UPSTREAM.md names with a version, wherever it
        // stands in the document.
        let upstream = include_str!("../../../UPSTREAM.md");
        let pinned: std::collections::BTreeSet<&str> = upstream
            .split("cua-driver-rs-v")
            .skip(1)
            .map(|rest| {
                let end = rest
                    .find(|c: char| !c.is_ascii_digit() && c != '.')
                    .unwrap_or(rest.len());
                rest[..end].trim_end_matches('.')
            })
            .filter(|version| !version.is_empty())
            .collect();
        assert_eq!(pinned, [CUA_DRIVER_TESTED_VERSION].into());
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
