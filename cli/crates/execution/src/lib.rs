#![doc = include_str!("../README.md")]

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
