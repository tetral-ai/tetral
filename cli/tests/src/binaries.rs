//! The programs a test runs, each built once per test process, and under
//! cargo-nextest, which starts a process for each test, once per run.

use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use crate::workspace_dir;

/// The target directory of the harness's builds, apart from the one the
/// running tests' build holds.
pub const TARGET_DIR: &str = "target/test-seams";

/// `tetral` with its test seams: `cargo build --locked -p tetral --features
/// test-seams --target-dir target/test-seams` in `cli/`.
pub fn tetral() -> &'static Path {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT.get_or_init(|| {
        once_per_run("tetral", "", || {
            run(cargo_command()
                .args([
                    "build",
                    "--locked",
                    "-p",
                    "tetral",
                    "--features",
                    "test-seams",
                    "--target-dir",
                    TARGET_DIR,
                ])
                .current_dir(workspace_dir()));
        });
        workspace_dir()
            .join(TARGET_DIR)
            .join("debug")
            .join("tetral")
    })
}

/// The target directory of [`tetral_with_bwrap_digest`].
pub const DIGEST_TARGET_DIR: &str = "target/test-seams-bwrap-digest";

/// `tetral` with its test seams, built with `CODEX_BWRAP_SHA256=<digest>` as
/// a release build is, into [`DIGEST_TARGET_DIR`]; one digest per test
/// process.
pub fn tetral_with_bwrap_digest(digest: &str) -> &'static Path {
    static BUILT: OnceLock<(String, PathBuf)> = OnceLock::new();
    let (built_digest, path) = BUILT.get_or_init(|| {
        once_per_run("tetral-with-bwrap-digest", digest, || {
            run(cargo_command()
                .args([
                    "build",
                    "--locked",
                    "-p",
                    "tetral",
                    "--features",
                    "test-seams",
                    "--target-dir",
                    DIGEST_TARGET_DIR,
                ])
                .env("CODEX_BWRAP_SHA256", digest)
                .current_dir(workspace_dir()));
        });
        (
            digest.to_owned(),
            workspace_dir()
                .join(DIGEST_TARGET_DIR)
                .join("debug")
                .join("tetral"),
        )
    });
    assert_eq!(built_digest, digest, "one bwrap digest per test process");
    path
}

/// The reference control server, built with `go build` inside its own Go
/// module, `cli/reference`.
pub fn reference_server() -> &'static Path {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT.get_or_init(|| {
        let output = workspace_dir().join(TARGET_DIR).join("tetral-reference");
        once_per_run("tetral-reference", "", || {
            run(Command::new("go")
                .arg("build")
                .arg("-o")
                .arg(&output)
                .arg("./cmd/tetral-reference")
                .current_dir(workspace_dir().join("reference")));
        });
        output
    })
}

/// Runs `build` of the program `name` in its `variant`, unless a process of
/// the same cargo-nextest run has done so: the processes of a run take the
/// build in turn, and all but the first find its mark. Without cargo-nextest
/// every process builds.
fn once_per_run(name: &str, variant: &str, build: impl FnOnce()) {
    let Ok(run) = std::env::var("NEXTEST_RUN_ID") else {
        return build();
    };
    let directory = workspace_dir().join(TARGET_DIR);
    std::fs::create_dir_all(&directory).expect("the harness's target directory");
    let path = directory.join(format!("{name}.built"));
    let mut mark = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    rustix::fs::flock(&mark, rustix::fs::FlockOperation::LockExclusive)
        .unwrap_or_else(|error| panic!("lock {}: {error}", path.display()));
    let built = format!("{run} {variant}");
    let mut found = String::new();
    mark.read_to_string(&mut found).expect("the build's mark");
    if found == built {
        return;
    }
    build();
    mark.set_len(0).expect("the build's mark");
    mark.rewind().expect("the build's mark");
    mark.write_all(built.as_bytes()).expect("the build's mark");
}

/// The `cargo` that runs the tests, so that the pinned toolchain builds.
pub fn cargo() -> PathBuf {
    std::env::var_os("CARGO").map_or_else(|| PathBuf::from("cargo"), PathBuf::from)
}

/// A fixture build with the same package-metadata environment as a direct
/// Cargo invocation. `cargo test` sets these for this harness; ring's build
/// script tracks their incoming values, so inheriting them invalidates an
/// otherwise reusable direct build. Cargo supplies each built package's own
/// metadata to its build scripts. Keep toolchain and user build settings.
pub fn cargo_command() -> Command {
    let mut command = Command::new(cargo());
    for name in [
        "CARGO_MANIFEST_DIR",
        "CARGO_PKG_NAME",
        "CARGO_PKG_VERSION_MAJOR",
        "CARGO_PKG_VERSION_MINOR",
        "CARGO_PKG_VERSION_PATCH",
        "CARGO_PKG_VERSION_PRE",
    ] {
        command.env_remove(name);
    }
    command
}

fn run(command: &mut Command) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("{command:?} did not start: {error}"));
    assert!(status.success(), "{command:?} failed: {status}");
}
