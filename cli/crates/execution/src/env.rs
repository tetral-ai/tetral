//! The environment of an `exec_command` command: the App Server's own
//! without the CLI's credential and control variables, then the call's
//! `env` in key order, then the entries that `env` cannot override. No
//! variable whose name begins with `TETRAL_` reaches a command, inherited or
//! requested.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::path::Path;

/// The prefix of the CLI's credential and control variables.
const CONTROL_PREFIX: &str = "TETRAL_";

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
/// directory `temporary`. An inherited entry that is not UTF-8 is left out.
pub fn command_environment(
    inherited: impl IntoIterator<Item = (OsString, OsString)>,
    requested: Option<&BTreeMap<String, String>>,
    temporary: &Path,
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
    Ok(environment)
}
