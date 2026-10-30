//! The drive transfer interface: the five requests of the drive transfer
//! wire, which the Drive sync owners and the Content cache send and the
//! `connection` crate implements. The implementation retries a temporary
//! answer, waits out a rebind and the connection's rules, and gives the
//! caller only the answers it acts on.

use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use protocol::drive_transfer::{
    CommitChange, CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::errors::ErrorBody;
use protocol::field::Sha256;
use protocol::id::Id;
use protocol::path::DrivePath;
use tokio::sync::watch;

/// The total of a PUT or a commit for each started share of this many
/// request bytes.
pub const SCALE_BYTES: u64 = 65_536;
pub const TOTAL_PER_SCALE: std::time::Duration = std::time::Duration::from_secs(10);

/// The total of a PUT or a commit of `request_bytes`:
/// 10 s × max(1, ceil(request_bytes / 65,536)).
pub fn total(request_bytes: u64) -> std::time::Duration {
    let shares = request_bytes.div_ceil(SCALE_BYTES).max(1);
    TOTAL_PER_SCALE * u32::try_from(shares).unwrap_or(u32::MAX)
}

/// The binding a drive sync holds, which its requests carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub connection_id: Id,
    pub binding_id: Id,
}

/// The drive sync's binding: `None` while it holds none, and a new value at
/// each binding of a later preparation. Each attempt carries the value it
/// holds then. A request that waits for a new binding ends once the drive
/// sync has dropped the sender.
pub type BindingWatch = watch::Receiver<Option<Binding>>;

/// One request for `drive_id`.
pub struct Request<B> {
    pub drive_id: Id,
    pub binding: BindingWatch,
    /// The `traceparent` of the instruction whose sync point sends it.
    pub traceparent: Option<String>,
    pub body: B,
}

/// `GET files`, after `after_path` or from the first page.
#[derive(Debug, Clone)]
pub struct ListFiles {
    pub after_path: Option<DrivePath>,
}

/// `GET changes` strictly after `(after_seq, after_index)`, or after the
/// whole commit `after_seq` without an index.
#[derive(Debug, Clone)]
pub struct ListChanges {
    pub after_seq: i64,
    pub after_index: Option<i64>,
}

/// `PUT content/{sha256}` of `content`.
pub struct Upload {
    pub sha256: Sha256,
    pub content: Content,
    /// Told of each `429 rate_limited` answer, for the upload slots'
    /// pressure point.
    pub rate_limited: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// The bytes of an upload, read again from the start at each attempt.
#[derive(Debug, Clone)]
pub enum Content {
    Bytes(Bytes),
    /// A file of `size` bytes; an attempt that reads another count fails.
    File {
        path: PathBuf,
        size: u64,
    },
}

impl Content {
    pub fn size(&self) -> u64 {
        match self {
            Content::Bytes(bytes) => bytes.len() as u64,
            Content::File { size, .. } => *size,
        }
    }
}

/// `POST commits` of `changes`.
#[derive(Debug, Clone)]
pub struct Commit {
    pub changes: Vec<CommitChange>,
}

/// `GET content/{sha256}` of content of `size` bytes, into a sink.
#[derive(Debug, Clone)]
pub struct Download {
    pub sha256: Sha256,
    pub size: u64,
}

/// Where a download's bytes go. An attempt after one that left `received`
/// bytes, with `0 < received < size`, asks for the rest with `Range`; an
/// answer that restarts from byte 0 calls `restart` first.
pub trait ContentSink: Send {
    fn received(&self) -> u64;
    fn restart(&mut self) -> io::Result<()>;
    fn append(&mut self, bytes: &[u8]) -> io::Result<()>;
}

/// The class of an answer, as the drive transfer client sorts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// A network failure, a passed deadline, `500`, `502`, `503`, `504` or
    /// `429`: sent again after a delay.
    Temporary,
    /// `409 stale_connection` or `409 stale_binding`: sent again with the
    /// binding of a later preparation.
    Rebind,
    /// `401` or `403 instance_unavailable`: the connection's rules, then as a
    /// rebind.
    ConnectionRule,
    /// `400 content_hash_mismatch`.
    ReRead,
    /// `410 drive_cursor_expired`.
    CursorExpired,
    /// `409 content_missing` or `413 file_too_large`.
    Decision,
    /// Anything else, `403 drive_access_denied` included.
    Permanent,
}

/// How a request ended without the answer it asked for.
#[derive(Debug)]
pub enum Ended {
    /// `400 content_hash_mismatch`: the file is read again.
    ReRead,
    /// `410 drive_cursor_expired`: the drive is listed again.
    CursorExpired,
    /// `409 content_missing` or `413 file_too_large`, with its body.
    Decision(ErrorBody),
    /// A permanent answer with its status; `code` is the error code when
    /// the answer carried a valid error body.
    Permanent {
        status: u16,
        code: Option<&'static str>,
    },
    /// The download's sink failed.
    Sink(io::Error),
}

/// A request in flight.
pub type Transfer<'a, T> = Pin<Box<dyn Future<Output = Result<T, Ended>> + Send + 'a>>;

/// How the Drive sync owners and the Content cache send their drive transfer
/// requests. Dropping a request's future cancels it; it then has no further
/// effect.
pub trait DriveTransfer: Send + Sync {
    fn list_files(&self, request: Request<ListFiles>) -> Transfer<'_, ListFilesResponse>;
    fn list_changes(&self, request: Request<ListChanges>) -> Transfer<'_, ListChangesResponse>;
    fn upload_content(&self, request: Request<Upload>) -> Transfer<'_, UploadContentResponse>;
    fn commit(&self, request: Request<Commit>) -> Transfer<'_, CommitResponse>;
    /// Writes the content into `sink`, from what it already received; the
    /// count and hash of what it holds are the caller's to check.
    fn download_content<'a>(
        &'a self,
        request: Request<Download>,
        sink: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()>;
}
