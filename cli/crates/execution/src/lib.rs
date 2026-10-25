#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod capture;
pub mod isolation;
pub mod policy;
pub mod report;
pub mod store;

use tokio::sync::mpsc;

pub enum SupervisorMessage {}

pub type SupervisorMailbox = mpsc::UnboundedSender<SupervisorMessage>;

pub fn spawn_task_supervisor() -> SupervisorMailbox {
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            handle_supervisor_message(message);
        }
    });
    mailbox
}

fn handle_supervisor_message(message: SupervisorMessage) {
    match message {}
}
