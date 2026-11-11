//! The Content cache owner: the machine's one writer under
//! `$TETRAL_HOME/cache/`. `cache/blobs/<sha256>` holds committed content
//! only, each hash once however many Sessions and drives name it; content
//! enters only when the whole of it hashes to its name. Downloads write into
//! `cache/tmp/`, which the owner empties when it starts, and reach the disk
//! before their rename into `cache/blobs/`. Opening a blob for a
//! read sets its modification time, its last read time; while the state
//! root's volume has less than [`FREE_SPACE_MIN`] free, the blobs read
//! longest ago are deleted, except those read within [`KEPT_AFTER_READ`].
//!
//! The owner also holds the App Server's transfer slots, the pressure points
//! `tetral_downloads` and `tetral_uploads`: [`DOWNLOADS_MAX`] downloads run
//! at once, the others wait in one queue for the machine, first come, first
//! served; [`UPLOADS_MAX`] uploads hold a slot at once, and a freed upload
//! slot goes to the oldest waiting request of the next Session in turn.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use observe::pressure::{Episodes, Name, QUIET_INTERVAL};
use observe::{Counter, Gauge, Histogram};
use protocol::field::Sha256;
use protocol::id::{Id, SafeId};
use sha2::Digest;
use state::clock::Clock;
use tokio::sync::{mpsc, oneshot};

use crate::transfer::{
    Binding, BindingWatch, ContentSink, Download, DriveTransfer, Ended, Request,
};

/// The downloads and the uploads that run at once on the App Server.
pub const DOWNLOADS_MAX: usize = 8;
pub const UPLOADS_MAX: usize = 8;

/// The free space of the state root's volume that eviction keeps: 5 GiB.
pub const FREE_SPACE_MIN: u64 = 5_368_709_120;

/// How often the free space is read besides after each added blob.
pub const EVICTION_INTERVAL: Duration = Duration::from_secs(10);

/// How long a read keeps its blob from eviction. A Mac mount's reads hold
/// no descriptor between them, so a blob deleted while a file is read would
/// be downloaded again for its next read.
pub const KEPT_AFTER_READ: Duration = Duration::from_secs(60);

/// Construction-time cache and transfer limits. Production uses the defaults.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub downloads: usize,
    pub uploads: usize,
    pub free_space: u64,
    pub eviction_interval: Duration,
    pub kept_after_read: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            downloads: DOWNLOADS_MAX,
            uploads: UPLOADS_MAX,
            free_space: FREE_SPACE_MIN,
            eviction_interval: EVICTION_INTERVAL,
            kept_after_read: KEPT_AFTER_READ,
        }
    }
}

/// The bytes of the state root's volume that are free.
pub type FreeSpace = Arc<dyn Fn() -> io::Result<u64> + Send + Sync>;

/// `statvfs` of `root`: `f_bavail` × `f_frsize`.
pub fn volume_free_space(root: PathBuf) -> FreeSpace {
    Arc::new(move || {
        let stat = rustix::fs::statvfs(&root)?;
        Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
    })
}

/// What the Content cache owner is built with.
pub struct CacheConfig {
    /// The state root, under which `cache/` lies.
    pub root: PathBuf,
    pub transfer: Arc<dyn DriveTransfer>,
    pub clock: Clock,
    pub free_space: FreeSpace,
}

/// A download a drive sync's read needs: the content of `sha256`, `size`
/// bytes, of `drive_id`, with the binding the drive sync holds.
#[derive(Clone)]
pub struct Needed {
    pub session_id: SafeId,
    pub drive_id: Id,
    pub sha256: Sha256,
    pub size: u64,
    pub binding: BindingWatch,
}

/// A handle on the Content cache owner.
#[derive(Clone)]
pub struct ContentCache {
    mailbox: mpsc::UnboundedSender<Message>,
    tmp: PathBuf,
    #[cfg(test)]
    pub(crate) verifications: Arc<HeldVerifications>,
}

/// Verifications that a drive test holds at their start, to order them after
/// another event.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct HeldVerifications {
    /// Whether verifications are held, and how many are held now.
    state: std::sync::Mutex<(bool, usize)>,
    released: std::sync::Condvar,
}

#[cfg(test)]
impl HeldVerifications {
    /// Holds verifications until the returned guard drops, a failed
    /// assertion's unwind included, so that no held thread keeps the test's
    /// runtime from shutting down.
    pub(crate) fn hold(self: &Arc<Self>) -> Hold {
        self.state.lock().unwrap().0 = true;
        Hold(Arc::clone(self))
    }

    pub(crate) fn held(&self) -> usize {
        self.state.lock().unwrap().1
    }

    fn pass(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 += 1;
        while state.0 {
            state = self.released.wait(state).unwrap();
        }
        state.1 -= 1;
    }
}

#[cfg(test)]
pub(crate) struct Hold(Arc<HeldVerifications>);

#[cfg(test)]
impl Drop for Hold {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.0 = false;
        self.0.released.notify_all();
    }
}

/// An immutable private copy whose bytes were checked against the accepted
/// hash and size. Dropping an unused or cancelled result removes its scratch.
pub(crate) type VerifiedCopies =
    HashMap<String, Result<VerifiedFile, (&'static str, observe::Cause)>>;

pub(crate) struct VerifiedFile {
    path: PathBuf,
    sha256: Sha256,
}

impl VerifiedFile {
    pub(crate) fn hash(&self) -> &Sha256 {
        &self.sha256
    }
    pub(crate) fn open(&self) -> io::Result<File> {
        File::open(&self.path)
    }
}

impl Drop for VerifiedFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

enum Message {
    Accessed(Sha256),
    Progress {
        session_id: SafeId,
        reply: oneshot::Sender<Vec<(Sha256, u64)>>,
    },
    Read {
        needed: Needed,
        reply: oneshot::Sender<io::Result<File>>,
    },
    Downloaded {
        key: (SafeId, Sha256),
        id: u64,
        outcome: DownloadOutcome,
    },
    Unbound {
        key: (SafeId, Sha256),
        id: u64,
    },
    ForgetSession(SafeId),
    Adopt {
        verified: VerifiedFile,
        reply: oneshot::Sender<io::Result<()>>,
    },
    RemoveUnnamed {
        named: HashSet<Sha256>,
        reply: oneshot::Sender<()>,
    },
    UploadSlot {
        session_id: SafeId,
        upload_id: u64,
        reply: oneshot::Sender<()>,
    },
    UploadSlotsWithdraw(SafeId),
    UploadSlotWithdraw {
        session_id: SafeId,
        upload_id: u64,
    },
    UploadSlotFree {
        session_id: SafeId,
        upload_id: u64,
    },
    RateLimited,
}

impl ContentCache {
    /// Empties `cache/tmp/` and starts the owner.
    pub fn spawn(config: CacheConfig) -> io::Result<ContentCache> {
        Self::spawn_with_limits(config, Limits::default())
    }

    /// Starts the owner with construction overrides for behavioral tests.
    pub fn spawn_with_limits(config: CacheConfig, limits: Limits) -> io::Result<ContentCache> {
        let tmp = config.root.join("cache/tmp");
        let blobs = config.root.join("cache/blobs");
        if tmp.exists() {
            fs::remove_dir_all(&tmp)?;
        }
        state::files::create_private_dirs(&tmp).map_err(|error| error.source)?;
        state::files::create_private_dirs(&blobs).map_err(|error| error.source)?;
        let (mailbox, inbox) = mpsc::unbounded_channel();
        let owner = Owner {
            limits,
            tmp: tmp.clone(),
            blobs,
            transfer: config.transfer,
            clock: config.clock,
            free_space: config.free_space,
            mailbox: mailbox.clone(),
            downloads: HashMap::new(),
            downloads_waiting: VecDeque::new(),
            downloads_running: 0,
            next_download: 0,
            uploads: Uploads::default(),
            download_series: Series::new("tetral_downloads", limits.downloads),
            upload_series: Series::new("tetral_uploads", limits.uploads),
        };
        tokio::spawn(owner.run(inbox));
        Ok(ContentCache {
            mailbox,
            tmp,
            #[cfg(test)]
            verifications: Arc::default(),
        })
    }

    /// The blob of `needed`, opened for reading, downloaded first when the
    /// cache lacks it; `EIO` when the download fails or brings other bytes.
    /// The read takes its place among the downloads when this is called.
    pub fn read(&self, needed: Needed) -> impl Future<Output = io::Result<File>> + use<> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Read { needed, reply });
        async move { answer.await.unwrap_or_else(|_| Err(eio())) }
    }

    /// A held descriptor served another read. Touch only an existing blob;
    /// eviction of its name does not invalidate the descriptor.
    pub(crate) fn accessed(&self, sha256: Sha256) {
        self.send(Message::Accessed(sha256));
    }

    /// The received bytes of each download that `session_id`'s reads wait
    /// for, a waiting download's 0 included.
    pub async fn downloading(&self, session_id: &SafeId) -> Vec<(Sha256, u64)> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Progress {
            session_id: session_id.clone(),
            reply,
        });
        answer.await.unwrap_or_default()
    }

    /// Drops queued and suspended downloads and cancels active attempts when
    /// the Session's local state is removed.
    pub fn forget_session(&self, session: SafeId) {
        self.send(Message::ForgetSession(session));
    }

    /// Copies and verifies a source on the caller's blocking worker. The
    /// immutable result can cross the mailbox without relying on a mutable
    /// Session pathname remaining unchanged after verification.
    pub(crate) fn verify_copy(
        &self,
        source: &Path,
        sha256: Sha256,
        size: u64,
    ) -> io::Result<VerifiedFile> {
        #[cfg(test)]
        self.verifications.pass();
        let source = File::open(source)?;
        let path = self.tmp.join(format!("{:016x}", rand::random::<u64>()));
        let mut target = File::create_new(&path)?;
        let verified = VerifiedFile { path, sha256 };
        let mut source = source.take(size.saturating_add(1));
        let mut digest = sha2::Sha256::new();
        let mut buffer = [0; 65_536];
        let mut count = 0;
        loop {
            let read = source.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            target.write_all(&buffer[..read])?;
            digest.update(&buffer[..read]);
            count += read as u64;
        }
        let hash: String = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if count != size || hash != verified.sha256.as_str() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "accepted content does not match its hash and size",
            ));
        }
        target.sync_all()?;
        Ok(verified)
    }

    /// Publishes an already verified, immutable copy or removes a duplicate.
    /// This owner wait performs no hashing or network work.
    pub(crate) async fn adopt(&self, verified: VerifiedFile) -> io::Result<()> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Adopt { verified, reply });
        answer.await.unwrap_or_else(|_| Err(eio()))
    }

    /// Deletes every blob whose hash is not in `named`, the hashes that the
    /// remaining Sessions' files name.
    pub async fn remove_unnamed(&self, named: HashSet<Sha256>) {
        let (reply, answer) = oneshot::channel();
        self.send(Message::RemoveUnnamed { named, reply });
        let _ = answer.await;
    }

    /// Asks for an upload slot; the reply is its grant.
    pub fn upload_slot(&self, session_id: SafeId, upload_id: u64) -> oneshot::Receiver<()> {
        let (reply, grant) = oneshot::channel();
        self.send(Message::UploadSlot {
            session_id,
            upload_id,
            reply,
        });
        grant
    }

    /// Gives up a waiting request; a request already granted keeps its slot
    /// until [`ContentCache::upload_slot_free`].
    pub fn upload_slot_withdraw(&self, session_id: SafeId, upload_id: u64) {
        self.send(Message::UploadSlotWithdraw {
            session_id,
            upload_id,
        });
    }

    /// Withdraws a Session's entire waiting queue before cancelling its
    /// running workers, whose drops may immediately return granted slots.
    pub fn upload_slots_withdraw(&self, session: SafeId) {
        self.send(Message::UploadSlotsWithdraw(session));
    }

    pub fn upload_slot_free(&self, session_id: SafeId, upload_id: u64) {
        self.send(Message::UploadSlotFree {
            session_id,
            upload_id,
        });
    }

    /// An upload was answered `429 rate_limited`.
    pub fn upload_rate_limited(&self) {
        self.send(Message::RateLimited);
    }

    fn send(&self, message: Message) {
        let _ = self.mailbox.send(message);
    }
}

fn eio() -> io::Error {
    io::Error::from(rustix::io::Errno::IO)
}

/// One download in progress for a drive sync and hash, and the reads that
/// wait for it.
struct Running {
    waiting: Vec<oneshot::Sender<io::Result<File>>>,
    /// The download until it takes a slot.
    pending: Option<Needed>,
    since: Duration,
    received: Arc<AtomicU64>,
    id: u64,
    active: bool,
    retained: Option<Hashing>,
    rejected: Option<Option<Binding>>,
    watcher: Option<tokio::task::AbortHandle>,
    cancel: Option<oneshot::Sender<()>>,
    forgotten: bool,
    span: tracing::Span,
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(watcher) = self.watcher.take() {
            watcher.abort();
        }
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

const DOWNLOADS: Name = Name("tetral_downloads");
const UPLOADS: Name = Name("tetral_uploads");
const RATE_LIMITED: Name = Name("rate_limited");

struct Series {
    in_progress: Gauge,
    queued: Gauge,
    oldest_queued: Gauge,
    queue_wait: Histogram,
    rejected: Counter,
    episodes: Episodes,
}

impl Series {
    fn new(point: &'static str, limit: usize) -> Series {
        let registry = observe::registry();
        let name = |suffix: &str| format!("{point}_{suffix}");
        registry.gauge(&name("limit"), &[]).set(limit as f64);
        Series {
            in_progress: registry.gauge(&name("in_progress"), &[]),
            queued: registry.gauge(&name("queued"), &[]),
            oldest_queued: registry.gauge(&name("oldest_queued_seconds"), &[]),
            queue_wait: registry.histogram(&name("queue_wait_seconds"), &[]),
            rejected: registry.counter(&name("rejected_total"), &[("cause", "rate_limited")]),
            episodes: Episodes::new(
                if point == "tetral_uploads" {
                    UPLOADS
                } else {
                    DOWNLOADS
                },
                QUIET_INTERVAL,
            ),
        }
    }

    fn show(&self, running: usize, queued: usize, oldest: Duration) {
        self.in_progress.set(running as f64);
        self.queued.set(queued as f64);
        self.oldest_queued.set(oldest.as_secs_f64());
    }

    fn granted(&mut self, waited: Duration, now: Duration) {
        self.episodes.admitted(now);
        self.queue_wait.observe(waited.as_secs_f64());
    }
}

/// The upload slots: the granted ones and each Session's waiting requests.
#[derive(Default)]
struct Uploads {
    granted: HashSet<(SafeId, u64)>,
    waiting: BTreeMap<SafeId, VecDeque<(u64, Duration, oneshot::Sender<()>)>>,
    last_granted: Option<SafeId>,
}

impl Uploads {
    /// The oldest request of the first Session with one that follows the
    /// Session granted last, or of the first Session when none follows.
    fn next(&mut self) -> Option<(SafeId, u64, Duration, oneshot::Sender<()>)> {
        let session = match &self.last_granted {
            Some(last) => self
                .waiting
                .range::<SafeId, _>((std::ops::Bound::Excluded(last), std::ops::Bound::Unbounded))
                .next()
                .or_else(|| self.waiting.iter().next())
                .map(|(session, _)| session.clone()),
            None => self.waiting.keys().next().cloned(),
        }?;
        let queue = self.waiting.get_mut(&session)?;
        let (upload_id, since, reply) = queue.pop_front()?;
        if queue.is_empty() {
            self.waiting.remove(&session);
        }
        Some((session, upload_id, since, reply))
    }

    fn queued(&self) -> usize {
        self.waiting.values().map(VecDeque::len).sum()
    }

    fn oldest(&self) -> Option<Duration> {
        self.waiting
            .values()
            .filter_map(|queue| queue.front().map(|(_, since, _)| *since))
            .min()
    }
}

struct Owner {
    limits: Limits,
    tmp: PathBuf,
    blobs: PathBuf,
    transfer: Arc<dyn DriveTransfer>,
    clock: Clock,
    free_space: FreeSpace,
    mailbox: mpsc::UnboundedSender<Message>,
    downloads: HashMap<(SafeId, Sha256), Running>,
    /// The downloads that wait for a slot, in the order they started.
    downloads_waiting: VecDeque<(SafeId, Sha256)>,
    downloads_running: usize,
    next_download: u64,
    uploads: Uploads,
    download_series: Series,
    upload_series: Series,
}

impl Owner {
    async fn run(mut self, mut inbox: mpsc::UnboundedReceiver<Message>) {
        let mut next_eviction = self.clock.now() + self.limits.eviction_interval;
        self.evict();
        loop {
            tokio::select! {
                message = inbox.recv() => match message {
                    Some(message) => self.handle(message),
                    None => return,
                },
                () = self.clock.sleep_step(next_eviction) => {}
            }
            self.show_downloads();
            self.show_uploads();
            if self.clock.now() >= next_eviction {
                self.evict();
                next_eviction = self.clock.now() + self.limits.eviction_interval;
            }
        }
    }

    fn handle(&mut self, message: Message) {
        match message {
            Message::Accessed(sha256) => {
                let _ = self.open(&sha256);
            }
            Message::Progress { session_id, reply } => {
                let mut progress: Vec<_> = self
                    .downloads
                    .iter()
                    .filter(|((session, _), _)| *session == session_id)
                    .map(|((_, hash), running)| {
                        (hash.clone(), running.received.load(Ordering::Acquire))
                    })
                    .collect();
                progress.sort();
                let _ = reply.send(progress);
            }
            Message::Read { needed, reply } => self.read(needed, reply),
            Message::Downloaded { key, id, outcome } => self.downloaded(key, id, outcome),
            Message::Unbound { key, id } => {
                if let Some(running) = self.downloads.get_mut(&key)
                    && running.id == id
                    && !running.active
                {
                    running.pending = None;
                    running.rejected = Some(None);
                    for reply in running.waiting.drain(..) {
                        let _ = reply.send(Err(eio()));
                    }
                    self.downloads_waiting.retain(|each| *each != key);
                }
            }
            Message::ForgetSession(session) => {
                self.downloads_waiting.retain(|(each, _)| *each != session);
                self.downloads.retain(|(each, _), running| {
                    if *each != session {
                        return true;
                    }
                    for reply in running.waiting.drain(..) {
                        let _ = reply.send(Err(eio()));
                    }
                    if let Some(cancel) = running.cancel.take() {
                        let _ = cancel.send(());
                    }
                    running.forgotten = true;
                    running.active
                });
            }
            Message::Adopt { verified, reply } => {
                let _ = reply.send(self.store(&verified.path, &verified.sha256));
                self.evict();
            }
            Message::RemoveUnnamed { named, reply } => {
                self.remove_unnamed(&named);
                let _ = reply.send(());
            }
            Message::UploadSlot {
                session_id,
                upload_id,
                reply,
            } => self.upload_slot(session_id, upload_id, reply),
            Message::UploadSlotsWithdraw(session) => {
                self.uploads.waiting.remove(&session);
            }
            Message::UploadSlotWithdraw {
                session_id,
                upload_id,
            } => {
                if let Some(queue) = self.uploads.waiting.get_mut(&session_id) {
                    queue.retain(|(each, _, _)| *each != upload_id);
                    if queue.is_empty() {
                        self.uploads.waiting.remove(&session_id);
                    }
                }
                self.show_uploads();
            }
            Message::UploadSlotFree {
                session_id,
                upload_id,
            } => {
                if self.uploads.granted.remove(&(session_id, upload_id)) {
                    self.grant_uploads();
                }
                self.show_uploads();
            }
            Message::RateLimited => {
                let now = self.clock.now();
                self.upload_series.rejected.increment();
                self.upload_series.episodes.rejected(RATE_LIMITED, now);
            }
        }
    }

    fn blob(&self, sha256: &Sha256) -> PathBuf {
        self.blobs.join(sha256.as_str())
    }

    /// Opens the blob of `sha256` for a read, which sets its last read time.
    fn open(&self, sha256: &Sha256) -> io::Result<File> {
        let file = File::open(self.blob(sha256))?;
        file.set_modified(SystemTime::now())?;
        Ok(file)
    }

    fn read(&mut self, needed: Needed, reply: oneshot::Sender<io::Result<File>>) {
        match self.open(&needed.sha256) {
            Ok(file) => {
                let _ = reply.send(Ok(file));
                return;
            }
            Err(error) if error.kind() != io::ErrorKind::NotFound => {
                download_failed(
                    &download_span(&needed),
                    "content_read_failed",
                    cause(&error),
                );
                let _ = reply.send(Err(error));
                return;
            }
            Err(_) => {}
        }
        let binding = needed.binding.borrow().clone();
        if binding.is_none() {
            let _ = reply.send(Err(eio()));
            return;
        }
        let key = (needed.session_id.clone(), needed.sha256.clone());
        if let Some(running) = self.downloads.get_mut(&key) {
            if running.forgotten || running.rejected.as_ref() == Some(&binding) {
                let _ = reply.send(Err(eio()));
                return;
            }
            running.waiting.push(reply);
            if running.rejected.is_none() {
                return;
            }
            running.pending = Some(needed.clone());
            running.rejected = None;
            running.since = self.clock.now();
        } else {
            self.downloads.insert(
                key.clone(),
                Running {
                    waiting: vec![reply],
                    pending: Some(needed.clone()),
                    since: self.clock.now(),
                    received: Arc::new(AtomicU64::new(0)),
                    id: 0,
                    active: false,
                    retained: None,
                    rejected: None,
                    watcher: None,
                    cancel: None,
                    forgotten: false,
                    span: tracing::Span::none(),
                },
            );
        }
        self.next_download += 1;
        let id = self.next_download;
        let running = self.downloads.get_mut(&key).expect("inserted download");
        running.id = id;
        if let Some(watcher) = running.watcher.take() {
            watcher.abort();
        }
        let mailbox = self.mailbox.clone();
        let watched_key = key.clone();
        let watcher = tokio::spawn(async move {
            unbound(needed.binding).await;
            let _ = mailbox.send(Message::Unbound {
                key: watched_key,
                id,
            });
        });
        running.watcher = Some(watcher.abort_handle());
        self.downloads_waiting.push_back(key);
        self.start_downloads();
    }

    /// Gives each free download slot to the download that waited longest.
    fn start_downloads(&mut self) {
        let now = self.clock.now();
        while self.downloads_running < self.limits.downloads
            && let Some(key) = self.downloads_waiting.pop_front()
        {
            let Some(running) = self.downloads.get_mut(&key) else {
                continue;
            };
            let Some(needed) = running.pending.take() else {
                continue;
            };
            if needed.binding.borrow().is_none() {
                running.rejected = Some(None);
                for reply in running.waiting.drain(..) {
                    let _ = reply.send(Err(eio()));
                }
                continue;
            }
            self.downloads_running += 1;
            running.active = true;
            running.span = download_span(&needed);
            let waited = now.saturating_sub(running.since);
            self.download_series.granted(waited, now);
            let task = DownloadTask {
                transfer: self.transfer.clone(),
                tmp: self.tmp.clone(),
                received: running.received.clone(),
            };
            let id = running.id;
            let retained = running.retained.take();
            let (cancel, cancelled) = oneshot::channel();
            running.cancel = Some(cancel);
            let mailbox = self.mailbox.clone();
            tokio::spawn(async move {
                let outcome = task.run(needed, retained, cancelled).await;
                let _ = mailbox.send(Message::Downloaded { key, id, outcome });
            });
        }
        self.show_downloads();
    }

    fn downloaded(&mut self, key: (SafeId, Sha256), id: u64, outcome: DownloadOutcome) {
        let Some(mut running) = self.downloads.remove(&key) else {
            return;
        };
        if running.id != id {
            self.downloads.insert(key, running);
            return;
        }
        self.downloads_running -= 1;
        running.active = false;
        running.cancel = None;
        if let Some(watcher) = running.watcher.take() {
            watcher.abort();
        }
        if running.forgotten {
            self.start_downloads();
            return;
        }
        match outcome {
            DownloadOutcome::Suspended(sink, binding) => {
                running.retained = Some(sink);
                running.rejected = Some(binding);
                for reply in running.waiting.drain(..) {
                    let _ = reply.send(Err(eio()));
                }
                self.downloads.insert(key, running);
            }
            outcome => {
                // Publish and open every descriptor before eviction can unlink it.
                let (stored, mut failure) = match outcome {
                    DownloadOutcome::Verified(sink) => match self.store(&sink.path, &key.1) {
                        Ok(()) => (true, None),
                        Err(error) => (false, Some(("content_write_failed", cause(&error)))),
                    },
                    DownloadOutcome::Failed(code) => (false, code),
                    DownloadOutcome::Suspended(..) => unreachable!(),
                };
                for reply in running.waiting.drain(..) {
                    let answer = if stored {
                        self.open(&key.1).inspect_err(|error| {
                            failure.get_or_insert_with(|| ("content_read_failed", cause(error)));
                        })
                    } else {
                        Err(eio())
                    };
                    let _ = reply.send(answer);
                }
                if let Some((code, cause)) = failure {
                    download_failed(&running.span, code, cause);
                }
                if stored {
                    self.evict();
                }
            }
        }
        self.start_downloads();
    }

    /// Renames `path` to the blob of `sha256`, or deletes it when that blob
    /// is already stored.
    fn store(&self, path: &Path, sha256: &Sha256) -> io::Result<()> {
        store(path, &self.blob(sha256))
    }

    /// Deletes blobs last read more than [`KEPT_AFTER_READ`] ago, in
    /// ascending modification time, while the volume has less than
    /// [`FREE_SPACE_MIN`] free.
    fn evict(&self) {
        let Ok(mut free) = (self.free_space)() else {
            return;
        };
        if free >= self.limits.free_space {
            return;
        }
        let Ok(entries) = fs::read_dir(&self.blobs) else {
            return;
        };
        let Some(read_before) = SystemTime::now().checked_sub(self.limits.kept_after_read) else {
            return;
        };
        let mut blobs: Vec<(SystemTime, PathBuf)> = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let modified = entry.metadata().ok()?.modified().ok()?;
                (modified < read_before).then(|| (modified, entry.path()))
            })
            .collect();
        blobs.sort();
        for (_, path) in blobs {
            if free >= self.limits.free_space {
                return;
            }
            let _ = fs::remove_file(&path);
            match (self.free_space)() {
                Ok(now) => free = now,
                Err(_) => return,
            }
        }
    }

    fn remove_unnamed(&self, named: &HashSet<Sha256>) {
        let Ok(entries) = fs::read_dir(&self.blobs) else {
            return;
        };
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name();
            let kept = name
                .to_str()
                .and_then(|name| Sha256::new(name).ok())
                .is_some_and(|sha256| named.contains(&sha256));
            if !kept {
                let _ = fs::remove_file(entry.path());
            }
        }
    }

    fn upload_slot(&mut self, session_id: SafeId, upload_id: u64, reply: oneshot::Sender<()>) {
        let now = self.clock.now();
        self.uploads
            .waiting
            .entry(session_id)
            .or_default()
            .push_back((upload_id, now, reply));
        self.grant_uploads();
        self.show_uploads();
    }

    fn grant_uploads(&mut self) {
        let now = self.clock.now();
        while self.uploads.granted.len() < self.limits.uploads
            && let Some((session_id, upload_id, since, reply)) = self.uploads.next()
        {
            self.uploads.last_granted = Some(session_id.clone());
            self.upload_series.granted(now.saturating_sub(since), now);
            // A request whose asker is gone takes no slot.
            if reply.send(()).is_ok() {
                self.uploads.granted.insert((session_id, upload_id));
            }
        }
    }

    fn show_downloads(&self) {
        let now = self.clock.now();
        let oldest = self
            .downloads_waiting
            .front()
            .and_then(|key| self.downloads.get(key))
            .map_or(Duration::ZERO, |running| now.saturating_sub(running.since));
        self.download_series
            .show(self.downloads_running, self.downloads_waiting.len(), oldest);
    }

    fn show_uploads(&self) {
        let now = self.clock.now();
        let oldest = self
            .uploads
            .oldest()
            .map_or(Duration::ZERO, |since| now.saturating_sub(since));
        self.upload_series
            .show(self.uploads.granted.len(), self.uploads.queued(), oldest);
    }
}

/// Renames `path` to `blob`, or deletes it when `blob` exists.
fn store(path: &Path, blob: &Path) -> io::Result<()> {
    if blob.exists() {
        return fs::remove_file(path);
    }
    fs::rename(path, blob)
}

/// One download's work, apart from the owner.
struct DownloadTask {
    transfer: Arc<dyn DriveTransfer>,
    tmp: PathBuf,
    received: Arc<AtomicU64>,
}

enum DownloadOutcome {
    Verified(Hashing),
    Suspended(Hashing, Option<Binding>),
    /// With the code and the cause of a failure that is recorded.
    Failed(Option<(&'static str, observe::Cause)>),
}

/// Reads have no instruction trace. Their actual transfer still identifies
/// the Session, Drive and binding whose authority it used.
fn download_span(needed: &Needed) -> tracing::Span {
    use observe::Identity;
    let binding = needed.binding.borrow();
    let mut identities = vec![
        (Identity::Session, needed.session_id.as_str()),
        (Identity::Drive, needed.drive_id.as_str()),
    ];
    if let Some(binding) = binding.as_ref() {
        identities.push((Identity::Connection, binding.connection_id.as_str()));
        identities.push((Identity::Binding, binding.binding_id.as_str()));
    }
    observe::correlation_span(&identities, None)
}

fn download_failed(span: &tracing::Span, code: &'static str, cause: observe::Cause) {
    span.in_scope(|| {
        observe::warn!(
            "drive.download_failed",
            error.code = observe::Code(code),
            error.message = cause
        )
    });
}

fn cause(error: &io::Error) -> observe::Cause {
    observe::Cause::system(&error.to_string())
}

/// A dropped sender with a retained value is used by standalone cache users.
/// Session removal explicitly cancels its downloads through `forget_session`.
async fn unbound(mut binding: BindingWatch) {
    loop {
        if binding.borrow_and_update().is_none() {
            return;
        }
        if binding.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

impl DownloadTask {
    async fn run(
        self,
        needed: Needed,
        retained: Option<Hashing>,
        mut cancelled: oneshot::Receiver<()>,
    ) -> DownloadOutcome {
        let mut sink = if let Some(sink) = retained {
            sink
        } else {
            let path = self.tmp.join(format!("{:016x}", rand::random::<u64>()));
            let file = match File::create_new(&path) {
                Ok(file) => file,
                Err(error) => {
                    return DownloadOutcome::Failed(Some(("content_write_failed", cause(&error))));
                }
            };
            Hashing {
                file,
                path,
                digest: sha2::Sha256::new(),
                received: self.received,
            }
        };
        let binding = needed.binding.borrow().clone();
        let watched = needed.binding.clone();
        let request = Request {
            drive_id: needed.drive_id,
            binding: needed.binding,
            traceparent: None,
            body: Download {
                sha256: needed.sha256.clone(),
                size: needed.size,
            },
        };
        let result = tokio::select! {
            result = self.transfer.download_content(request, &mut sink) => result,
            () = unbound(watched) => Err(Ended::Rebind(binding)),
            _ = &mut cancelled => return DownloadOutcome::Failed(None),
        };
        if let Err(Ended::Rebind(binding)) = result {
            return DownloadOutcome::Suspended(sink, binding);
        }
        match result {
            Err(Ended::Sink(error)) => {
                return DownloadOutcome::Failed(Some(("content_write_failed", cause(&error))));
            }
            Err(_) => return DownloadOutcome::Failed(None),
            Ok(()) => {}
        }
        let digest = sink.digest.clone().finalize();
        let hash: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        let received = sink.received.load(Ordering::Acquire);
        if received != needed.size || hash != needed.sha256.as_str() {
            let mismatch = format!(
                "received {received} bytes with SHA-256 {hash}; the listing gives {} bytes with SHA-256 {}",
                needed.size,
                needed.sha256.as_str()
            );
            DownloadOutcome::Failed(Some((
                "content_integrity_failed",
                observe::Cause::system(&mismatch),
            )))
        } else if let Err(error) = sink.file.sync_all() {
            DownloadOutcome::Failed(Some(("content_write_failed", cause(&error))))
        } else {
            DownloadOutcome::Verified(sink)
        }
    }
}

/// The file of a download, hashed while it is written.
struct Hashing {
    file: File,
    path: PathBuf,
    digest: sha2::Sha256,
    received: Arc<AtomicU64>,
}

impl Drop for Hashing {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl ContentSink for Hashing {
    fn received(&self) -> u64 {
        self.received.load(Ordering::Acquire)
    }

    fn restart(&mut self) -> io::Result<()> {
        self.file = File::create(&self.path)?;
        self.digest = sha2::Sha256::new();
        self.received.store(0, Ordering::Release);
        Ok(())
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)?;
        self.digest.update(bytes);
        self.received
            .fetch_add(bytes.len() as u64, Ordering::AcqRel);
        Ok(())
    }
}
