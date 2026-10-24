#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

use tokio::sync::mpsc;

pub enum McpManagerMessage {}

pub type McpManagerMailbox = mpsc::UnboundedSender<McpManagerMessage>;

pub fn spawn_mcp_manager() -> McpManagerMailbox {
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            handle_mcp_manager_message(message);
        }
    });
    mailbox
}

fn handle_mcp_manager_message(message: McpManagerMessage) {
    match message {}
}
