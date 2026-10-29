#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod cache;
pub mod catchup;
pub mod local;
pub mod ops;
pub mod paths;
pub mod state;
pub mod sync;
pub mod transfer;

pub use sync::{DriveSync, SyncConfig};
pub use transfer::DriveTransfer;
