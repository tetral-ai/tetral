#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod binding_change;
pub mod cache;
pub mod catchup;
pub mod commit;
mod content_syncs;
pub mod final_upload;
#[cfg(target_os = "linux")]
pub mod fuse;
mod handles;
pub mod local;
pub mod lost_changes;
pub mod mount;
#[cfg(target_os = "macos")]
pub mod mount_macos;
#[cfg(target_os = "linux")]
pub mod mountinfo;
#[cfg(target_os = "macos")]
pub mod nfs;
pub mod ops;
pub mod outcome;
pub mod paths;
pub mod report;
pub mod state;
pub mod sync;
pub mod sync_point;
pub mod transfer;
pub mod upload;

pub use final_upload::{FinalMode, FinalUpload, FinalUploadRequest};
pub use sync::{DriveSync, SyncConfig};
pub use sync_point::{SyncOutcome, SyncPointRequest};
pub use transfer::DriveTransfer;

// `transfer/checked.rs` names this crate as its integration tests do.
#[cfg(any(test, feature = "test-seams"))]
extern crate self as drive;
#[cfg(test)]
mod tests;
