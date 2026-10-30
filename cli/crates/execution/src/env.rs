//! The environment of an `exec_command` command: the App Server's own
//! without the CLI's credential and control variables, then the call's
//! `env` in key order, then the entries that `env` cannot override. No
//! variable whose name begins with `TETRAL_` reaches a command, inherited or
//! requested; the CLI sets only `TETRAL_DRIVES` itself.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::path::Path;

/// The prefix of the CLI's credential and control variables.
const CONTROL_PREFIX: &str = "TETRAL_";

/// The only inherited inputs used to locate trusted launcher programs.
/// Task overrides never participate in this environment.
const LAUNCHER_VARIABLES: [&str; 2] = ["PATH", "HOME"];

pub fn launcher_environment(
    inherited: impl IntoIterator<Item = (OsString, OsString)>,
) -> HashMap<String, String> {
    inherited
        .into_iter()
        .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(name, _)| LAUNCHER_VARIABLES.contains(&name.as_str()))
        .collect()
}

/// A seekable, anonymous carrier for one command's environment. The parent
/// finishes writing before spawn; the isolated entry reads and closes it
/// before exec. No writer can block waiting for sandbox startup, and no
/// environment text appears in argv or a persistent file.
pub(crate) fn command_environment_file(environment: &HashMap<String, String>) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    let mut file = {
        // SAFETY: the name is a static C string; memfd_create returns a new
        // owned descriptor, initially close-on-exec.
        let raw =
            unsafe { libc::memfd_create(c"tetral-task-environment".as_ptr(), libc::MFD_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: this is the successful call's new descriptor.
        unsafe { File::from_raw_fd(raw) }
    };
    #[cfg(not(target_os = "linux"))]
    let mut file = tempfile::tempfile()?;
    serde_json::to_writer(&mut file, environment).map_err(io::Error::other)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

/// The entries that every command gets after the call's `env`, so that its
/// output carries no terminal control sequences and no pager waits.
const FIXED: [(&str, &str); 4] = [
    ("TERM", "dumb"),
    ("NO_COLOR", "1"),
    ("PAGER", "cat"),
    ("GIT_PAGER", "cat"),
];

/// An `env` entry whose name or value holds U+0000, which no process
/// environment can carry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the env entry {name:?} holds U+0000")]
pub struct NulEntry {
    pub name: String,
}

/// The environment of a command whose App Server runs with `inherited` and
/// whose call asks for `requested`; `TMPDIR` is the task's temporary
/// directory `temporary`, and `TETRAL_DRIVES` is `drives` when the Session's
/// drive sync gives one. An inherited entry that is not UTF-8 is left out.
pub fn command_environment(
    inherited: impl IntoIterator<Item = (OsString, OsString)>,
    requested: Option<&BTreeMap<String, String>>,
    temporary: &Path,
    drives: Option<&Path>,
) -> Result<HashMap<String, String>, NulEntry> {
    let mut environment: HashMap<String, String> = inherited
        .into_iter()
        .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(name, _)| !name.starts_with(CONTROL_PREFIX))
        .collect();
    for (name, value) in requested.into_iter().flatten() {
        if name.contains('\0') || value.contains('\0') {
            return Err(NulEntry { name: name.clone() });
        }
        if !name.starts_with(CONTROL_PREFIX) {
            environment.insert(name.clone(), value.clone());
        }
    }
    for (name, value) in FIXED {
        environment.insert(name.to_owned(), value.to_owned());
    }
    environment.insert("TMPDIR".to_owned(), temporary.display().to_string());
    if let Some(drives) = drives {
        environment.insert("TETRAL_DRIVES".to_owned(), drives.display().to_string());
    }
    Ok(environment)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn launchers_receive_only_trusted_program_lookup_inputs() {
        let inherited = [
            ("PATH", "/usr/bin:/bin"),
            ("HOME", "/home/operator"),
            ("CUSTOM_VALUE", "for the command"),
            ("TMPDIR", "/task/tmp"),
            ("TETRAL_HOME", "/state"),
            ("LD_LIBRARY_PATH", "/custom/libraries"),
        ];
        assert_eq!(
            launcher_environment(inherited.map(|(name, value)| (name.into(), value.into()))),
            HashMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), "/home/operator".into()),
            ])
        );
    }

    #[test]
    fn command_environment_round_trips_as_data_without_changing_its_rules() {
        let inherited = vec![
            ("CUSTOM_VALUE".into(), "inherited".into()),
            ("UNCHANGED".into(), "inherited".into()),
            ("TETRAL_HOME".into(), "/state".into()),
            ("NON_UTF8".into(), OsString::from_vec(vec![0xff])),
        ];
        let requested = BTreeMap::from([
            ("CUSTOM_VALUE".into(), "requested\nvalue".into()),
            ("TERM".into(), "xterm".into()),
            ("TMPDIR".into(), "/wrong".into()),
            ("TETRAL_SECRET".into(), "excluded".into()),
        ]);
        let environment =
            command_environment(inherited, Some(&requested), Path::new("/task/tmp"), None).unwrap();
        let file = command_environment_file(&environment).unwrap();
        let decoded: HashMap<String, String> = serde_json::from_reader(file).unwrap();
        assert_eq!(decoded, environment);
        assert_eq!(decoded["CUSTOM_VALUE"], "requested\nvalue");
        assert_eq!(decoded["UNCHANGED"], "inherited");
        assert_eq!(decoded["TMPDIR"], "/task/tmp");
        for (name, value) in FIXED {
            assert_eq!(decoded[name], value);
        }
        assert!(!decoded.contains_key("NON_UTF8"));
        assert!(!decoded.keys().any(|name| name.starts_with(CONTROL_PREFIX)));
        // `TETRAL_DRIVES` is the drive sync's value, never the call's.
        let requested = BTreeMap::from([("TETRAL_DRIVES".into(), "/elsewhere".into())]);
        let drives = Path::new("/state/sessions/s/drives");
        let set = command_environment([], Some(&requested), Path::new("/t"), Some(drives)).unwrap();
        assert_eq!(set["TETRAL_DRIVES"], "/state/sessions/s/drives");
        let unset = command_environment([], Some(&requested), Path::new("/t"), None).unwrap();
        assert!(!unset.contains_key("TETRAL_DRIVES"));
        let invalid = BTreeMap::from([("CUSTOM_VALUE".into(), "invalid\0value".into())]);
        assert!(command_environment([], Some(&invalid), Path::new("/task/tmp"), None).is_err());
    }
}
