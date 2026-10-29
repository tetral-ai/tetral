#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod cache;
pub mod state;
pub mod transfer;

use tokio::sync::mpsc;

pub use transfer::DriveTransfer;

pub enum DriveSyncMessage {}

pub type DriveSyncMailbox = mpsc::UnboundedSender<DriveSyncMessage>;

pub fn spawn_drive_sync_owner() -> DriveSyncMailbox {
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            handle_drive_sync_message(message);
        }
    });
    mailbox
}

fn handle_drive_sync_message(message: DriveSyncMessage) {
    match message {}
}

pub enum ContentCacheMessage {}

pub type ContentCacheMailbox = mpsc::UnboundedSender<ContentCacheMessage>;

pub fn spawn_content_cache() -> ContentCacheMailbox {
    let (mailbox, mut messages) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            handle_content_cache_message(message);
        }
    });
    mailbox
}

fn handle_content_cache_message(message: ContentCacheMessage) {
    match message {}
}
