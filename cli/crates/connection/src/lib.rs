#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

use tokio::sync::mpsc;

pub enum ConnectionMessage {}

pub type ConnectionMailbox = mpsc::UnboundedSender<ConnectionMessage>;

pub fn spawn_connection_owner() -> ConnectionMailbox {
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            handle_connection_message(message);
        }
    });
    mailbox
}

fn handle_connection_message(message: ConnectionMessage) {
    match message {}
}

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
