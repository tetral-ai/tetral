#![doc = include_str!("../README.md")]

pub mod binaries;
pub mod record;
pub mod reference;
pub mod run;
pub mod seams;

use std::path::{Path, PathBuf};

/// The Cargo workspace, `cli/`.
pub fn workspace_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the test package lies inside the workspace")
}

/// The repository that holds the workspace.
pub fn repository_root() -> PathBuf {
    workspace_dir()
        .parent()
        .expect("the workspace lies inside the repository")
        .to_path_buf()
}
