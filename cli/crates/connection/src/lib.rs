#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod attempt;
pub mod backoff;
pub mod heartbeat;
pub mod http;
pub mod owner;
pub mod ready;
pub mod staging;
pub mod status;
pub mod stream;

use tokio::sync::mpsc;

pub use owner::{Config, Connection, Credential, Instruction, Times};
pub use ready::ReadyFields;
pub use status::ConnectionStatus;

pub enum OutboxMessage {}

pub type OutboxMailbox = mpsc::UnboundedSender<OutboxMessage>;

pub fn spawn_report_outbox() -> OutboxMailbox {
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            handle_outbox_message(message);
        }
    });
    mailbox
}

fn handle_outbox_message(message: OutboxMessage) {
    match message {}
}
