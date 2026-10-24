//! The acceptance record of one run of a test file.
//!
//! Each run of a test file of `tests/` writes one Markdown file,
//! `<test>_<os>-<arch>_<commit>.md`, into the directory that
//! `TETRAL_ACCEPTANCE_DIR` names: `<test>` is the file's stem, `<os>-<arch>`
//! the platform as a ready names it (`linux-x86_64`, `macos-aarch64`) and
//! `<commit>` the full commit of the repository. A later run on the same
//! platform at the same commit replaces it. The file is rewritten as each
//! case ends, so it holds every case of the run, with the commands the case
//! ran, what it observed, and the cases not run with their reason. A test
//! with `TETRAL_ACCEPTANCE_DIR` unset fails at once.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

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
        let mut header = format!("# {test} on {platform} at {commit}\n\n");
        let _ = writeln!(header, "- Operating system: {}", operating_system());
        let _ = writeln!(header, "- Architecture: {}", std::env::consts::ARCH);
        let _ = writeln!(header, "- Rust: {}", output("rustc", &["--version"]));
        let _ = writeln!(header, "- Go: {}", output("go", &["version"]));
        let _ = writeln!(header, "- tetral: {}", env!("CARGO_PKG_VERSION"));
        let record = Record {
            path: directory.join(format!("{test}_{platform}_{commit}.md")),
            header,
            cases: Mutex::new(BTreeMap::new()),
        };
        record.write(&BTreeMap::new());
        record
    }

    /// Starts the record of one case; it is written when the case ends.
    pub fn case(&'static self, name: &str) -> Case {
        Case {
            record: self,
            name: name.to_owned(),
            commands: Vec::new(),
            observations: Vec::new(),
            not_run: None,
        }
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
pub struct Case {
    record: &'static Record,
    name: String,
    commands: Vec<String>,
    observations: Vec<String>,
    not_run: Option<String>,
}

impl Case {
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
        self.not_run = Some(reason.into());
    }

    /// Whether a case that needs a non-root user can run: under root it
    /// cannot, `why`, and the case is marked as not run.
    pub fn runs_as_non_root(&mut self, why: &str) -> bool {
        if rustix::process::geteuid().is_root() {
            self.not_run(format!("the test runs as root, and {why}"));
            return false;
        }
        true
    }
}

impl Drop for Case {
    fn drop(&mut self) {
        let result = match (&self.not_run, std::thread::panicking()) {
            (Some(reason), _) => format!("not run: {reason}"),
            (None, true) => "failed".to_owned(),
            (None, false) => "passed".to_owned(),
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
