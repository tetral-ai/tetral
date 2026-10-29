#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod capabilities;
pub mod configuration;
pub mod lifecycle;
pub mod local_api;
pub mod lock;
pub mod shutdown;
pub mod startup;
pub mod tasks;
