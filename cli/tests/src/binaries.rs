//! The programs a test runs, each built once per test process.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use crate::{repository_root, workspace_dir};

/// The target directory of the harness's builds, apart from the one the
/// running `cargo test` holds.
pub const TARGET_DIR: &str = "target/test-seams";

/// `tetral` with its test seams: `cargo build --locked -p tetral --features
/// test-seams --target-dir target/test-seams` in `cli/`.
pub fn tetral() -> &'static Path {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT.get_or_init(|| {
        run(Command::new(cargo())
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
        run(Command::new(cargo())
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

/// The reference control server, built with `go build` at the repository
/// root.
pub fn reference_server() -> &'static Path {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT.get_or_init(|| {
        let output = workspace_dir().join(TARGET_DIR).join("tetral-reference");
        run(Command::new("go")
            .arg("build")
            .arg("-o")
            .arg(&output)
            .arg("./cli/reference/cmd/tetral-reference")
            .current_dir(repository_root()));
        output
    })
}

/// The `cargo` that runs the tests, so that the pinned toolchain builds.
pub fn cargo() -> PathBuf {
    std::env::var_os("CARGO").map_or_else(|| PathBuf::from("cargo"), PathBuf::from)
}

fn run(command: &mut Command) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("{command:?} did not start: {error}"));
    assert!(status.success(), "{command:?} failed: {status}");
}
