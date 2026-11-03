//! The acceptance record of one run of a test file.
//!
//! Each run of a test file of `tests/` writes one Markdown file,
//! `<test>_<os>-<arch>_<commit>_<run>.md`, into the directory that
//! `TETRAL_ACCEPTANCE_DIR` names: `<test>` is the file's stem, `<os>-<arch>`
//! the platform as a ready names it (`linux-x86_64`, `macos-aarch64`) and
//! `<commit>` the full commit of the repository. The run identity preserves
//! separate executions of the same revision. A case is written as incomplete
//! when it starts and passes only at its explicit `pass`; dropping an
//! unfinished case during a panic records failure. The record includes the
//! commands, observations and reasons for cases not run. A test with
//! `TETRAL_ACCEPTANCE_DIR` unset fails at once.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::workspace_dir;

/// The variable that names the directory of acceptance records.
pub const DIRECTORY_VARIABLE: &str = "TETRAL_ACCEPTANCE_DIR";

/// The record of the test file that calls it.
#[macro_export]
macro_rules! record {
    () => {
        $crate::record::Record::of(env!("CARGO_CRATE_NAME"))
    };
}

/// The record of one run of one test file.
pub struct Record {
    path: PathBuf,
    header: String,
    cases: Mutex<BTreeMap<String, String>>,
}

impl Record {
    /// The record of the test file `test`, created at its first use in the
    /// process.
    pub fn of(test: &'static str) -> &'static Record {
        static RECORD: OnceLock<Record> = OnceLock::new();
        RECORD.get_or_init(|| Record::create(test))
    }

    fn create(test: &str) -> Record {
        let directory = std::env::var_os(DIRECTORY_VARIABLE).unwrap_or_else(|| {
            panic!("{DIRECTORY_VARIABLE} must name the directory of acceptance records")
        });
        let directory = PathBuf::from(directory);
        std::fs::create_dir_all(&directory)
            .unwrap_or_else(|error| panic!("create {}: {error}", directory.display()));
        let platform = platform();
        let commit = output("git", &["rev-parse", "HEAD"]);
        let run = format!(
            "{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            std::process::id()
        );
        let mut header = format!("# {test} on {platform} at {commit}\n\n");
        let _ = writeln!(header, "- Run: {run}");
        let _ = writeln!(
            header,
            "- Uncommitted changes: {}",
            !output(
                "git",
                &["status", "--porcelain", "--untracked-files=normal"]
            )
            .is_empty()
        );
        let _ = writeln!(
            header,
            "- Test arguments: {:?}",
            std::env::args().skip(1).collect::<Vec<_>>()
        );
        if let Ok(context) = std::env::var("TETRAL_ACCEPTANCE_CONTEXT") {
            let _ = writeln!(header, "- Run context: {context}");
        }
        let _ = writeln!(header, "- Operating system: {}", operating_system());
        let _ = writeln!(header, "- Architecture: {}", std::env::consts::ARCH);
        let _ = writeln!(header, "- Rust: {}", output("rustc", &["--version"]));
        let _ = writeln!(header, "- Go: {}", output("go", &["version"]));
        let _ = writeln!(header, "- tetral: {}", env!("CARGO_PKG_VERSION"));
        let record = Record {
            path: directory.join(format!("{test}_{platform}_{commit}_{run}.md")),
            header,
            cases: Mutex::new(BTreeMap::new()),
        };
        record.write(&BTreeMap::new());
        record
    }

    /// Starts a case and immediately records that it is incomplete.
    pub fn case(&self, name: &str) -> Case<'_> {
        let case = Case {
            record: self,
            name: name.to_owned(),
            commands: Vec::new(),
            observations: Vec::new(),
            outcome: Outcome::Incomplete,
        };
        case.write();
        case
    }

    fn write(&self, cases: &BTreeMap<String, String>) {
        let mut text = self.header.clone();
        for case in cases.values() {
            text.push('\n');
            text.push_str(case);
        }
        std::fs::write(&self.path, text)
            .unwrap_or_else(|error| panic!("write {}: {error}", self.path.display()));
    }
}

/// One case of a record: what it ran and observed.
pub struct Case<'a> {
    record: &'a Record,
    name: String,
    commands: Vec<String>,
    observations: Vec<String>,
    outcome: Outcome,
}

enum Outcome {
    Incomplete,
    Passed,
    Failed,
    NotRun(String),
}

impl Case<'_> {
    /// Completes a case after its assertions have passed. Later failures in
    /// another case cannot change this result.
    pub fn pass(mut self) {
        assert!(
            matches!(self.outcome, Outcome::Incomplete),
            "the case already ended"
        );
        self.outcome = Outcome::Passed;
        self.write();
    }

    /// Notes a command the case ran and how it exited.
    pub fn command(&mut self, command: &str, exit: &str) {
        self.commands.push(format!("`{command}`: {exit}"));
    }

    /// Notes what the case observed.
    pub fn observe(&mut self, observation: impl Into<String>) {
        self.observations.push(observation.into());
    }

    /// Marks the case as not run, for `reason`.
    pub fn not_run(&mut self, reason: impl Into<String>) {
        assert!(
            matches!(self.outcome, Outcome::Incomplete),
            "the case already ended"
        );
        self.outcome = Outcome::NotRun(reason.into());
        self.write();
    }

    /// Whether a case that needs a non-root user can run: under root it
    /// cannot, `why`, and the case is marked as not run.
    pub fn runs_as_non_root(&mut self, why: &str) -> bool {
        if rustix::process::geteuid().is_root() {
            assert!(
                std::env::var_os("GITHUB_ACTIONS").is_none_or(|value| value != "true"),
                "this required acceptance case needs a non-root user: {why}"
            );
            self.not_run(format!("the test runs as root, and {why}"));
            return false;
        }
        true
    }

    fn write(&self) {
        let result = match &self.outcome {
            Outcome::Incomplete => "incomplete".to_owned(),
            Outcome::Passed => "passed".to_owned(),
            Outcome::Failed => "failed".to_owned(),
            Outcome::NotRun(reason) => format!("not run: {reason}"),
        };
        let mut text = format!("## {}\n\nResult: {result}\n", self.name);
        for (title, lines) in [
            ("Commands", &self.commands),
            ("Observations", &self.observations),
        ] {
            if !lines.is_empty() {
                let _ = write!(text, "\n{title}:\n\n");
                for line in lines {
                    let _ = writeln!(text, "- {line}");
                }
            }
        }
        let mut cases = self
            .record
            .cases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cases.insert(self.name.clone(), text);
        self.record.write(&cases);
    }
}

impl Drop for Case<'_> {
    fn drop(&mut self) {
        if matches!(self.outcome, Outcome::Incomplete) {
            if std::thread::panicking() {
                self.outcome = Outcome::Failed;
            }
            self.write();
        }
    }
}

/// The platform as a ready names it; Rust's names of the operating system
/// and the architecture are the ready's.
pub fn platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn operating_system() -> String {
    if cfg!(target_os = "macos") {
        format!("macOS {}", output("sw_vers", &["-productVersion"]))
    } else {
        output("uname", &["-sr"])
    }
}

fn output(program: &str, arguments: &[&str]) -> String {
    let result = Command::new(program)
        .args(arguments)
        .current_dir(workspace_dir())
        .output()
        .unwrap_or_else(|error| panic!("{program} did not start: {error}"));
    assert!(result.status.success(), "{program} {arguments:?} failed");
    String::from_utf8_lossy(&result.stdout).trim().to_owned()
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt;

    use super::*;

    fn record(directory: &std::path::Path) -> Record {
        Record {
            path: directory.join("record.md"),
            header: "# Isolated acceptance record\n".to_owned(),
            cases: Mutex::new(BTreeMap::new()),
        }
    }

    #[test]
    fn a_completed_case_stays_passed_when_the_next_case_panics() {
        let directory = tempfile::tempdir().unwrap();
        let record = record(directory.path());
        let failed = std::panic::catch_unwind(|| {
            record.case("A").pass();
            let mut case = record.case("B");
            let run = crate::run::Run {
                status: std::process::ExitStatus::from_raw(1 << 8),
                stdout: b"{}\n".to_vec(),
                stderr: Vec::new(),
            };
            case.command("example command", &run.exit());
            assert_eq!(run.object(), serde_json::json!({}));
            assert_eq!(run.status.code(), Some(0), "the command failed");
            case.observe("exit 0");
            case.pass();
        });
        assert!(failed.is_err());
        let saved = std::fs::read_to_string(&record.path).unwrap();
        assert!(saved.contains("## A\n\nResult: passed\n"));
        assert!(saved.contains("## B\n\nResult: failed\n"));
        assert!(saved.contains("exit status: 1"));
        assert!(!saved.contains("exit 0"));
    }

    #[test]
    fn a_case_is_incomplete_from_its_start_until_it_explicitly_passes() {
        let directory = tempfile::tempdir().unwrap();
        let record = record(directory.path());
        let case = record.case("unfinished");
        assert!(
            std::fs::read_to_string(&record.path)
                .unwrap()
                .contains("Result: incomplete")
        );
        drop(case);
        assert!(
            std::fs::read_to_string(&record.path)
                .unwrap()
                .contains("Result: incomplete")
        );
    }

    #[test]
    fn an_unrun_case_keeps_its_reason_when_another_case_panics() {
        let directory = tempfile::tempdir().unwrap();
        let record = record(directory.path());
        let failed = std::panic::catch_unwind(|| {
            let mut skipped = record.case("unavailable");
            skipped.not_run("the platform lacks this facility");
            let _case = record.case("failed");
            panic!("another case failed");
        });
        assert!(failed.is_err());
        let saved = std::fs::read_to_string(&record.path).unwrap();
        assert!(saved.contains("Result: not run: the platform lacks this facility"));
        assert!(saved.contains("## failed\n\nResult: failed\n"));
    }
}
