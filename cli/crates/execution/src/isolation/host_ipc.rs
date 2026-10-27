//! Filesystem endpoints through which a task could ask host services to act.
//! This is a bounded list, not a general Unix-socket or host-service firewall.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};

use codex_protocol::permissions::{FileSystemAccessMode, FileSystemPath, FileSystemSandboxEntry};
use codex_utils_path_uri::PathUri;

/// Resolve the known service addresses before sandbox construction. Only
/// existing sockets become deny entries: a missing path must not cause the
/// sandbox builder to create a mount placeholder in the host filesystem.
pub(super) fn deny_host_services(entries: &mut Vec<FileSystemSandboxEntry>) {
    let candidates = socket_candidates(rustix::process::geteuid().as_raw(), |name| {
        std::env::var_os(name)
    });
    let paths = candidates.into_iter().filter_map(|path| {
        let canonical = path.canonicalize().ok()?;
        std::fs::metadata(&canonical)
            .ok()?
            .file_type()
            .is_socket()
            .then_some(canonical)
    });
    append_denies(entries, paths);
}

fn append_denies(
    entries: &mut Vec<FileSystemSandboxEntry>,
    paths: impl IntoIterator<Item = PathBuf>,
) {
    for path in paths.into_iter().collect::<BTreeSet<_>>() {
        let Ok(path) = PathUri::from_host_native_path(&path) else {
            continue;
        };
        entries.push(FileSystemSandboxEntry::new(
            FileSystemPath::Path { path },
            FileSystemAccessMode::Deny,
        ));
    }
}

fn socket_candidates(uid: u32, variable: impl Fn(&str) -> Option<OsString>) -> BTreeSet<PathBuf> {
    let mut paths = BTreeSet::new();
    for run in [Path::new("/run"), Path::new("/var/run")] {
        paths.insert(run.join("dbus/system_bus_socket"));
        paths.insert(run.join("systemd/private"));
        let user = run.join("user").join(uid.to_string());
        paths.insert(user.join("bus"));
        paths.insert(user.join("systemd/private"));
    }
    if let Some(runtime) = variable("XDG_RUNTIME_DIR").map(PathBuf::from)
        && runtime.is_absolute()
    {
        paths.insert(runtime.join("bus"));
        paths.insert(runtime.join("systemd/private"));
    }
    for name in ["DBUS_SESSION_BUS_ADDRESS", "DBUS_SYSTEM_BUS_ADDRESS"] {
        if let Some(address) = variable(name)
            && let Some(address) = address.to_str()
        {
            paths.extend(address_paths(address));
        }
    }
    paths
}

/// D-Bus address lists use semicolons, comma-separated fields, and percent
/// escapes in field values. Only absolute Unix filesystem addresses name a
/// path that mount masking can protect. Other transports remain unsupported.
fn address_paths(addresses: &str) -> impl Iterator<Item = PathBuf> + '_ {
    addresses.split(';').filter_map(|address| {
        let fields = address.strip_prefix("unix:")?;
        let path = fields
            .split(',')
            .find_map(|field| field.strip_prefix("path="))?;
        let mut decoded = Vec::with_capacity(path.len());
        let mut bytes = path.bytes();
        while let Some(byte) = bytes.next() {
            let byte = if byte == b'%' {
                let high = (bytes.next()? as char).to_digit(16)?;
                let low = (bytes.next()? as char).to_digit(16)?;
                (high * 16 + low) as u8
            } else {
                byte
            };
            if byte == 0 {
                return None;
            }
            decoded.push(byte);
        }
        let path = PathBuf::from(OsString::from_vec(decoded));
        path.is_absolute().then_some(path)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::permissions::FileSystemSandboxPolicy;

    #[test]
    fn known_service_candidates_include_runtime_and_configured_addresses() {
        let candidates = socket_candidates(1001, |name| match name {
            "XDG_RUNTIME_DIR" => Some("/custom/runtime".into()),
            "DBUS_SESSION_BUS_ADDRESS" => Some("unix:path=/custom/session%2cbus,guid=abc".into()),
            "DBUS_SYSTEM_BUS_ADDRESS" => Some("unix:path=/custom/system".into()),
            _ => None,
        });
        let expected = [
            "/run/dbus/system_bus_socket",
            "/run/systemd/private",
            "/run/user/1001/bus",
            "/run/user/1001/systemd/private",
            "/var/run/dbus/system_bus_socket",
            "/var/run/systemd/private",
            "/var/run/user/1001/bus",
            "/var/run/user/1001/systemd/private",
            "/custom/runtime/bus",
            "/custom/runtime/systemd/private",
            "/custom/session,bus",
            "/custom/system",
        ];
        assert_eq!(
            candidates,
            expected.into_iter().map(PathBuf::from).collect()
        );
        assert_eq!(socket_candidates(1001, |_| None).len(), 8);
        assert_eq!(
            socket_candidates(1001, |_| Some("relative".into())).len(),
            8
        );
    }

    #[test]
    fn address_parser_accepts_only_absolute_filesystem_addresses() {
        let addresses = concat!(
            "unix:abstract=host;tcp:host=localhost,port=1234;autolaunch:;",
            "unix:path=relative;unix:path=/bad%00name;unix:path=/bad%2;",
            "unix:path=/bad%xx;unix:guid=abc,path=/one;unix:path=/two%3bpart"
        );
        assert_eq!(
            address_paths(addresses).collect::<Vec<_>>(),
            vec![PathBuf::from("/one"), PathBuf::from("/two;part")]
        );
    }

    #[test]
    fn host_service_denies_override_broad_and_exact_grants() {
        for access in [FileSystemAccessMode::Read, FileSystemAccessMode::Write] {
            let socket = PathBuf::from("/run/user/1001/bus");
            let mut entries = [Path::new("/"), Path::new("/run"), socket.as_path()]
                .into_iter()
                .map(|path| {
                    FileSystemSandboxEntry::new(
                        FileSystemPath::Path {
                            path: PathUri::from_host_native_path(path).unwrap(),
                        },
                        access,
                    )
                })
                .collect();
            append_denies(&mut entries, [socket.clone(), socket.clone()]);
            assert_eq!(entries.len(), 4);
            let policy = FileSystemSandboxPolicy::restricted(entries);
            assert_eq!(
                policy.resolve_access_for_local_path_with_cwd(&socket, Path::new("/")),
                FileSystemAccessMode::Deny
            );
            assert_eq!(
                policy.resolve_access_for_local_path_with_cwd(
                    Path::new("/run/ordinary-service/socket"),
                    Path::new("/")
                ),
                access
            );
        }
    }
}
