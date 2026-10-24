//! The Session binding owner: one task per Session on this machine.

use tokio::sync::mpsc;

pub enum SessionMessage {}

pub type SessionMailbox = mpsc::UnboundedSender<SessionMessage>;

pub fn spawn() -> SessionMailbox {
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            handle_session_message(message);
        }
    });
    mailbox
}

fn handle_session_message(message: SessionMessage) {
    match message {}
}
