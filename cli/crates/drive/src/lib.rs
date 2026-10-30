#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod cache;
pub mod catchup;
pub mod commit;
#[cfg(target_os = "linux")]
pub mod fuse;
pub mod local;
pub mod lost_changes;
pub mod mount;
#[cfg(target_os = "linux")]
pub mod mountinfo;
pub mod ops;
pub mod outcome;
pub mod paths;
pub mod report;
pub mod state;
pub mod sync;
pub mod sync_point;
pub mod transfer;
pub mod upload;

pub use sync::{DriveSync, SyncConfig};
pub use sync_point::{SyncOutcome, SyncPointRequest};
pub use transfer::DriveTransfer;
