//! The directory probe's child: `tetral` run as `tetral-cwd-probe`, which
//! checks one working directory and enters it as the App Server's user, so
//! that the App Server never inspects a target itself and never changes its
//! own working directory. It runs no user command and writes no project
//! file; with `create_cwd` it creates only the missing directories of the
//! target. After the directory's checks it reports, for each repository
//! path, whether its resolved path lies in the state root and whether an
//! entry exists there.

use std::ffi::OsStr;
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

use protocol::connection::{PreparationError, PreparationErrorCode};
use rustix::fs::{CWD, Mode, OFlags};
use serde::{Deserialize, Serialize};

/// The `argv[0]` under which `tetral` runs as the probe.
pub const PROBE_NAME: &str = "tetral-cwd-probe";

/// The whole message of a resolved path that is not UTF-8, which the
/// report's `cwd` cannot carry and only Linux allows: the fixed text of
/// `cwd_inaccessible` would report it as denied access.
const CANONICAL_PATH_NOT_UTF8: &str = "the working directory's canonical path is not UTF-8";

/// The bytes of stdin the probe reads: twice the largest instruction event,
/// so that any `cwd` one carries fits with the state root. A longer input is
/// cut and fails to parse.
pub const INPUT_MAX_BYTES: u64 = 131_072;

/// Reads the probe's input from `reader`, cut at `max_bytes`:
/// [`INPUT_MAX_BYTES`] but for tests.
pub fn read_input(reader: impl Read, max_bytes: u64) -> Vec<u8> {
    let mut input = Vec::new();
    let _ = reader.take(max_bytes).read_to_end(&mut input);
    input
}

/// The probe's stdin, one JSON object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Input {
    pub cwd: String,
    pub create_cwd: bool,
    /// The state root's canonical path.
    pub state_root: String,
    /// The paths the preparation's repositories clone to.
    #[serde(default)]
    pub repository_paths: Vec<String>,
}

/// What the probe found at one repository path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryCheck {
    /// The path resolved, its deepest existing ancestor through symbolic
    /// links with the remaining components appended, is the state root or
    /// below it.
    pub inside_state_root: bool,
    /// An entry, of any kind, exists at the path.
    pub exists: bool,
}

/// The probe's one line of stdout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Output {
    /// The canonical working directory and each repository path's check,
    /// in the order of `repository_paths`.
    Entered {
        cwd: String,
        #[serde(default)]
        repositories: Vec<RepositoryCheck>,
    },
    Failed {
        error: PreparationError,
    },
}

/// Checks the directory that `input` names and gives the line to write.
pub fn run(input: &[u8]) -> Vec<u8> {
    let output = match serde_json::from_slice::<Input>(input) {
        Ok(input) => match check(&input) {
            Ok(cwd) => match cwd.into_os_string().into_string() {
                Ok(cwd) => Output::Entered {
                    cwd,
                    repositories: input
                        .repository_paths
                        .iter()
                        .map(|path| repository_check(Path::new(path), Path::new(&input.state_root)))
                        .collect(),
                },
                Err(_) => Output::Failed {
                    error: PreparationError {
                        code: PreparationErrorCode::CwdInaccessible,
                        message: CANONICAL_PATH_NOT_UTF8.to_owned(),
                    },
                },
            },
            Err(error) => Output::Failed { error },
        },
        Err(error) => Output::Failed {
            error: PreparationError::new(
                PreparationErrorCode::InvalidInput,
                Some(&error.to_string()),
            ),
        },
    };
    let mut line = serde_json::to_vec(&output).expect("the output encodes");
    line.push(b'\n');
    line
}

fn check(input: &Input) -> Result<PathBuf, PreparationError> {
    let resolved = match std::fs::canonicalize(&input.cwd) {
        Err(error) if error.kind() == io::ErrorKind::NotFound && input.create_cwd => {
            create_missing(Path::new(&input.cwd), Path::new(&input.state_root))?;
            std::fs::canonicalize(&input.cwd).map_err(|error| resolution(&error))?
        }
        resolved => resolved.map_err(|error| resolution(&error))?,
    };
    if resolved.starts_with(&input.state_root) {
        return Err(failure(PreparationErrorCode::CwdForbidden, None));
    }
    let metadata = std::fs::metadata(&resolved).map_err(|error| resolution(&error))?;
    if !metadata.is_dir() {
        return Err(failure(PreparationErrorCode::CwdNotDirectory, None));
    }
    // Entering needs search permission on the directory and on each
    // ancestor, and checks neither read nor write permission, as a command's
    // launch in it does.
    std::env::set_current_dir(&resolved).map_err(|error| match error.raw_os_error() {
        Some(code) if code == libc_eacces() || code == libc_eperm() => {
            failure(PreparationErrorCode::CwdInaccessible, Some(&error))
        }
        _ => resolution(&error),
    })?;
    Ok(resolved)
}

/// Creates the components of `target` below its deepest existing ancestor,
/// one at a time with the process umask, each opened without following a
/// symbolic link, after refusing an ancestor that resolves into the state
/// root. The ancestor is found by resolving each shorter prefix of `target`
/// as given, so a `..` after a symbolic link leaves the link's target, as the
/// kernel resolves it. A target whose missing components hold a `..` is not
/// found: reaching it would create a directory that the `..` then leaves.
fn create_missing(target: &Path, state_root: &Path) -> Result<(), PreparationError> {
    let mut missing = Vec::new();
    let mut ancestor = target;
    let resolved = loop {
        match std::fs::canonicalize(ancestor) {
            Ok(resolved) => break resolved,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(resolution(&error)),
        }
        let (Some(parent), Some(name)) = (ancestor.parent(), ancestor.file_name()) else {
            return Err(failure(PreparationErrorCode::CwdNotFound, None));
        };
        missing.push(name.to_owned());
        ancestor = parent;
    };
    if resolved.starts_with(state_root) {
        return Err(failure(PreparationErrorCode::CwdForbidden, None));
    }
    // Linux search handles do not require directory read permission.
    // mkdirat and the final chdir still enforce write/search permissions.
    #[cfg(target_os = "linux")]
    let directory_access = OFlags::PATH;
    #[cfg(not(target_os = "linux"))]
    let directory_access = OFlags::RDONLY;
    let directory_flags = OFlags::DIRECTORY | directory_access | OFlags::CLOEXEC;
    let mut directory = rustix::fs::openat(CWD, &resolved, directory_flags, Mode::empty())
        .map_err(|errno| resolution(&io::Error::from(errno)))?;
    for name in missing.iter().rev() {
        match rustix::fs::mkdirat(&directory, name.as_os_str(), Mode::from_raw_mode(0o777)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(errno) => return Err(creation(&io::Error::from(errno))),
        }
        directory = rustix::fs::openat(
            &directory,
            name.as_os_str(),
            directory_flags | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|errno| creation(&io::Error::from(errno)))?;
    }
    Ok(())
}

fn repository_check(path: &Path, state_root: &Path) -> RepositoryCheck {
    RepositoryCheck {
        inside_state_root: resolved(path).starts_with(state_root),
        exists: std::fs::symlink_metadata(path).is_ok(),
    }
}

/// `path` with its deepest existing ancestor resolved through symbolic
/// links and the remaining components appended.
fn resolved(path: &Path) -> PathBuf {
    let mut remaining = Vec::new();
    let mut ancestor = path;
    loop {
        if let Ok(resolved) = std::fs::canonicalize(ancestor) {
            return remaining
                .iter()
                .rev()
                .fold(resolved, |path, name| path.join(name));
        }
        match (ancestor.parent(), ancestor.file_name()) {
            (Some(parent), Some(name)) => {
                remaining.push(name.to_owned());
                ancestor = parent;
            }
            _ => return lexical(path),
        }
    }
}

/// `path` with `.` removed and each `..` taking away the component before
/// it, without reading the filesystem.
pub fn lexical(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normal.pop();
            }
            other => normal.push(other.as_os_str()),
        }
    }
    normal
}

/// A failure while resolving a path: a missing component, a component that
/// is not a directory and a symbolic-link loop are a missing directory, a
/// denied search an inaccessible one.
fn resolution(error: &io::Error) -> PreparationError {
    match error.raw_os_error() {
        Some(code) if code == libc_eacces() => {
            failure(PreparationErrorCode::CwdInaccessible, Some(error))
        }
        _ => failure(PreparationErrorCode::CwdNotFound, Some(error)),
    }
}

/// A failure while creating a missing directory: a denied one is
/// inaccessible; any other leaves the directory missing.
fn creation(error: &io::Error) -> PreparationError {
    match error.raw_os_error() {
        Some(code) if code == libc_eacces() || code == libc_eperm() => {
            failure(PreparationErrorCode::CwdInaccessible, Some(error))
        }
        _ => failure(PreparationErrorCode::CwdNotFound, Some(error)),
    }
}

fn failure(code: PreparationErrorCode, cause: Option<&io::Error>) -> PreparationError {
    PreparationError::new(code, cause.map(io::Error::to_string).as_deref())
}

fn libc_eacces() -> i32 {
    rustix::io::Errno::ACCESS.raw_os_error()
}

fn libc_eperm() -> i32 {
    rustix::io::Errno::PERM.raw_os_error()
}

/// Whether `argv0` names the probe, as a path whose last component is
/// [`PROBE_NAME`].
pub fn is_probe(argv0: &OsStr) -> bool {
    Path::new(argv0)
        .file_name()
        .is_some_and(|name| name.as_bytes() == PROBE_NAME.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(line: &[u8]) -> PreparationErrorCode {
        match serde_json::from_slice(line).unwrap() {
            Output::Failed { error } => error.code,
            Output::Entered { cwd, .. } => panic!("entered {cwd}"),
        }
    }

    #[test]
    fn an_input_cut_at_its_limit_fails_as_invalid_input() {
        let input = br#"{"cwd":"/missing-directory-of-the-probe-test","create_cwd":false,"state_root":"/root"}"#;
        let limit = input.len() as u64;
        assert_eq!(
            code(&run(&read_input(&input[..], limit))),
            PreparationErrorCode::CwdNotFound
        );
        assert_eq!(
            code(&run(&read_input(&input[..], limit - 1))),
            PreparationErrorCode::InvalidInput
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn directory_creation_uses_search_permissions_and_keeps_partial_prefixes() {
        use std::os::unix::fs::PermissionsExt;

        // Permission refusals need the same unprivileged user as production.
        assert!(!rustix::process::geteuid().is_root());
        let base = tempfile::tempdir().unwrap();
        for (name, parent_mode, umask, succeeds, leaves_prefix) in [
            ("search_and_write", 0o300, 0o477, true, true),
            ("no_write", 0o100, 0o022, false, false),
            ("no_search", 0o200, 0o022, false, false),
            ("no_child_search", 0o700, 0o777, false, true),
        ] {
            let parent = base.path().join(name);
            std::fs::create_dir(&parent).unwrap();
            std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(parent_mode))
                .unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "probe_child::tests::directory_creation_child",
                    "--nocapture",
                ])
                .env("TETRAL_PROBE_TEST_PARENT", &parent)
                .env("TETRAL_PROBE_TEST_UMASK", umask.to_string())
                .env("TETRAL_PROBE_TEST_SUCCESS", succeeds.to_string())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{name}: {}",
                String::from_utf8_lossy(&output.stdout)
            );
            assert_eq!(
                std::fs::metadata(&parent).unwrap().permissions().mode() & 0o777,
                parent_mode
            );
            // Restore only fixture access after the child's operation so the
            // test can inspect its result and remove the temporary directory.
            std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
            let first = parent.join("first");
            assert_eq!(first.exists(), leaves_prefix, "{name}");
            if leaves_prefix {
                assert_eq!(
                    std::fs::metadata(&first).unwrap().permissions().mode() & 0o777,
                    0o777 & !umask
                );
                std::fs::set_permissions(&first, std::fs::Permissions::from_mode(0o700)).unwrap();
                let second = first.join("second");
                assert_eq!(second.exists(), succeeds, "{name}");
                if succeeds {
                    assert_eq!(
                        std::fs::metadata(&second).unwrap().permissions().mode() & 0o777,
                        0o777 & !umask
                    );
                    std::fs::set_permissions(&second, std::fs::Permissions::from_mode(0o700))
                        .unwrap();
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn directory_creation_child() {
        let Some(parent) = std::env::var_os("TETRAL_PROBE_TEST_PARENT") else {
            return;
        };
        let umask = std::env::var("TETRAL_PROBE_TEST_UMASK")
            .unwrap()
            .parse()
            .unwrap();
        let succeeds = std::env::var("TETRAL_PROBE_TEST_SUCCESS").unwrap() == "true";
        rustix::process::umask(Mode::from_raw_mode(umask));
        let input = Input {
            cwd: Path::new(&parent)
                .join("first/second")
                .to_str()
                .unwrap()
                .to_owned(),
            create_cwd: true,
            state_root: "/unrelated-state-root".into(),
            repository_paths: Vec::new(),
        };
        let result = check(&input);
        if succeeds {
            assert_eq!(result.unwrap(), std::env::current_dir().unwrap());
        } else {
            assert_eq!(
                result.unwrap_err().code,
                PreparationErrorCode::CwdInaccessible
            );
        }
    }
}
