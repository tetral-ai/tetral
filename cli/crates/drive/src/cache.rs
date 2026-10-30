//! The Content cache owner: the machine's one writer under
//! `$TETRAL_HOME/cache/`. `cache/blobs/<sha256>` holds committed content
//! only, each hash once however many Sessions and drives name it; content
//! enters only when the whole of it hashes to its name. Downloads write into
//! `cache/tmp/`, which the owner empties when it starts, and reach the disk
//! before their rename into `cache/blobs/`. Opening a blob for a
//! read sets its modification time, its last read time; while the state
//! root's volume has less than [`FREE_SPACE_MIN`] free, the blobs read
//! longest ago are deleted.
//!
//! The owner also holds the App Server's transfer slots, the pressure points
//! `tetral_downloads` and `tetral_uploads`: [`DOWNLOADS_MAX`] downloads run
//! at once, the others wait in one queue for the machine, first come, first
//! served; [`UPLOADS_MAX`] uploads hold a slot at once, and a freed upload
//! slot goes to the oldest waiting request of the next Session in turn.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use observe::pressure::{Episodes, Name, QUIET_INTERVAL};
use observe::{Counter, Gauge, Histogram};
use protocol::field::Sha256;
use protocol::id::{Id, SafeId};
use sha2::Digest;
use state::clock::Clock;
use tokio::sync::{mpsc, oneshot};

use crate::transfer::{BindingWatch, ContentSink, Download, DriveTransfer, Request};

/// The downloads and the uploads that run at once on the App Server.
pub const DOWNLOADS_MAX: usize = 8;
pub const UPLOADS_MAX: usize = 8;

/// The free space of the state root's volume that eviction keeps: 5 GiB.
pub const FREE_SPACE_MIN: u64 = 5_368_709_120;

/// How often the free space is read besides after each added blob.
pub const EVICTION_INTERVAL: Duration = Duration::from_secs(10);

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
    progress: Progress,
}

/// The received bytes of each download in progress, by Session and hash.
type Progress = Arc<Mutex<HashMap<(SafeId, Sha256), Arc<AtomicU64>>>>;

enum Message {
    Read {
        needed: Needed,
        reply: oneshot::Sender<io::Result<File>>,
    },
    Downloaded {
        key: (SafeId, Sha256),
        stored: bool,
    },
    Adopt {
        path: PathBuf,
        sha256: Sha256,
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
        let tmp = config.root.join("cache/tmp");
        let blobs = config.root.join("cache/blobs");
        if tmp.exists() {
            fs::remove_dir_all(&tmp)?;
        }
        state::files::create_private_dirs(&tmp).map_err(|error| error.source)?;
        state::files::create_private_dirs(&blobs).map_err(|error| error.source)?;
        let (mailbox, inbox) = mpsc::unbounded_channel();
        let progress = Progress::default();
        let owner = Owner {
            tmp,
            blobs,
            transfer: config.transfer,
            clock: config.clock,
            free_space: config.free_space,
            mailbox: mailbox.clone(),
            progress: progress.clone(),
            downloads: HashMap::new(),
            downloads_waiting: VecDeque::new(),
            downloads_running: 0,
            uploads: Uploads::default(),
            download_series: Series::new("tetral_downloads", DOWNLOADS_MAX),
            upload_series: Series::new("tetral_uploads", UPLOADS_MAX),
        };
        tokio::spawn(owner.run(inbox));
        Ok(ContentCache { mailbox, progress })
    }

    /// The blob of `needed`, opened for reading, downloaded first when the
    /// cache lacks it; `EIO` when the download fails or brings other bytes.
    /// The read takes its place among the downloads when this is called.
    pub fn read(&self, needed: Needed) -> impl Future<Output = io::Result<File>> + use<> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Read { needed, reply });
        async move { answer.await.unwrap_or_else(|_| Err(eio())) }
    }

    /// The received bytes of each download that `session_id`'s reads wait
    /// for, a waiting download's 0 included.
    pub fn downloading(&self, session_id: &SafeId) -> Vec<(Sha256, u64)> {
        let progress = self.progress.lock().expect("progress lock");
        let mut each: Vec<(Sha256, u64)> = progress
            .iter()
            .filter(|((session, _), _)| session == session_id)
            .map(|((_, sha256), received)| (sha256.clone(), received.load(Ordering::Acquire)))
            .collect();
        each.sort();
        each
    }

    /// Takes the committed content file at `path` as the blob of `sha256`,
    /// or deletes it when that blob is already stored.
    pub async fn adopt(&self, path: PathBuf, sha256: Sha256) -> io::Result<()> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Adopt {
            path,
            sha256,
            reply,
        });
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
    tmp: PathBuf,
    blobs: PathBuf,
    transfer: Arc<dyn DriveTransfer>,
    clock: Clock,
    free_space: FreeSpace,
    mailbox: mpsc::UnboundedSender<Message>,
    progress: Progress,
    downloads: HashMap<(SafeId, Sha256), Running>,
    /// The downloads that wait for a slot, in the order they started.
    downloads_waiting: VecDeque<(SafeId, Sha256)>,
    downloads_running: usize,
    uploads: Uploads,
    download_series: Series,
    upload_series: Series,
}

impl Owner {
    async fn run(mut self, mut inbox: mpsc::UnboundedReceiver<Message>) {
        let mut next_eviction = self.clock.now() + EVICTION_INTERVAL;
        self.evict();
        loop {
            tokio::select! {
                message = inbox.recv() => match message {
                    Some(message) => self.handle(message),
                    None => return,
                },
                () = self.clock.sleep_step(next_eviction) => {}
            }
            if self.clock.now() >= next_eviction {
                self.evict();
                next_eviction = self.clock.now() + EVICTION_INTERVAL;
            }
        }
    }

    fn handle(&mut self, message: Message) {
        match message {
            Message::Read { needed, reply } => self.read(needed, reply),
            Message::Downloaded { key, stored } => self.downloaded(key, stored),
            Message::Adopt {
                path,
                sha256,
                reply,
            } => {
                let _ = reply.send(self.store(&path, &sha256));
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
                let _ = reply.send(Err(error));
                return;
            }
            Err(_) => {}
        }
        let key = (needed.session_id.clone(), needed.sha256.clone());
        if let Some(running) = self.downloads.get_mut(&key) {
            running.waiting.push(reply);
            return;
        }
        self.progress
            .lock()
            .expect("progress lock")
            .insert(key.clone(), Arc::new(AtomicU64::new(0)));
        self.downloads.insert(
            key.clone(),
            Running {
                waiting: vec![reply],
                pending: Some(needed),
                since: self.clock.now(),
            },
        );
        self.downloads_waiting.push_back(key);
        self.start_downloads();
    }

    /// Gives each free download slot to the download that waited longest.
    fn start_downloads(&mut self) {
        let now = self.clock.now();
        while self.downloads_running < DOWNLOADS_MAX
            && let Some(key) = self.downloads_waiting.pop_front()
        {
            let Some(running) = self.downloads.get_mut(&key) else {
                continue;
            };
            let Some(needed) = running.pending.take() else {
                continue;
            };
            self.downloads_running += 1;
            let waited = now.saturating_sub(running.since);
            self.download_series.granted(waited, now);
            let received = self
                .progress
                .lock()
                .expect("progress lock")
                .get(&key)
                .cloned()
                .unwrap_or_default();
            let task = DownloadTask {
                transfer: self.transfer.clone(),
                tmp: self.tmp.clone(),
                blobs: self.blobs.clone(),
                received,
            };
            let mailbox = self.mailbox.clone();
            tokio::spawn(async move {
                let stored = task.run(needed).await;
                let _ = mailbox.send(Message::Downloaded { key, stored });
            });
        }
        self.show_downloads();
    }

    fn downloaded(&mut self, key: (SafeId, Sha256), stored: bool) {
        self.downloads_running = self.downloads_running.saturating_sub(1);
        self.progress.lock().expect("progress lock").remove(&key);
        if let Some(running) = self.downloads.remove(&key) {
            for reply in running.waiting {
                let opened = if stored {
                    self.open(&key.1)
                } else {
                    Err(eio())
                };
                let _ = reply.send(opened);
            }
        }
        self.start_downloads();
        if stored {
            self.evict();
        }
    }

    /// Renames `path` to the blob of `sha256`, or deletes it when that blob
    /// is already stored.
    fn store(&self, path: &Path, sha256: &Sha256) -> io::Result<()> {
        store(path, &self.blob(sha256))
    }

    /// Deletes blobs in ascending modification time while the volume has
    /// less than [`FREE_SPACE_MIN`] free.
    fn evict(&self) {
        let Ok(mut free) = (self.free_space)() else {
            return;
        };
        if free >= FREE_SPACE_MIN {
            return;
        }
        let Ok(entries) = fs::read_dir(&self.blobs) else {
            return;
        };
        let mut blobs: Vec<(SystemTime, PathBuf)> = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let modified = entry.metadata().ok()?.modified().ok()?;
                Some((modified, entry.path()))
            })
            .collect();
        blobs.sort();
        for (_, path) in blobs {
            if free >= FREE_SPACE_MIN {
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
        while self.uploads.granted.len() < UPLOADS_MAX
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
    blobs: PathBuf,
    received: Arc<AtomicU64>,
}

impl DownloadTask {
    /// Whether the blob of `needed` is stored once the download ends.
    async fn run(self, needed: Needed) -> bool {
        let path = self.tmp.join(format!("{:016x}", rand::random::<u64>()));
        let Ok(file) = File::create_new(&path) else {
            return false;
        };
        let mut sink = Hashing {
            file,
            path: path.clone(),
            digest: sha2::Sha256::new(),
            received: self.received,
        };
        let request = Request {
            drive_id: needed.drive_id,
            binding: needed.binding,
            traceparent: None,
            body: Download {
                sha256: needed.sha256.clone(),
                size: needed.size,
            },
        };
        let downloaded = self
            .transfer
            .download_content(request, &mut sink)
            .await
            .is_ok();
        let digest = sink.digest.finalize();
        let hash: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        let whole = downloaded
            && sink.received.load(Ordering::Acquire) == needed.size
            && hash == needed.sha256.as_str()
            && sink.file.sync_all().is_ok();
        drop(sink.file);
        if !whole {
            let _ = fs::remove_file(&path);
            return false;
        }
        store(&path, &self.blobs.join(needed.sha256.as_str())).is_ok()
    }
}

/// The file of a download, hashed while it is written.
struct Hashing {
    file: File,
    path: PathBuf,
    digest: sha2::Sha256,
    received: Arc<AtomicU64>,
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
