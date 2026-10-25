//! The two sinks, stderr and the log file, each a background thread behind a
//! bounded queue, and the rotating log file.

use std::fs::File;
use std::io::{self, Write};
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, mpsc};
use std::thread;

use rustix::fs::{Mode, OFlags, RenameFlags};

use crate::metrics::{Counter, Registry};

/// Records a sink holds that are queued or being written.
pub const SINK_QUEUE_RECORDS: usize = 1024;

/// The size a record may not make `tetral.log` exceed.
pub const LOG_FILE_LIMIT_BYTES: u64 = 10_485_760;

const LOG_NAMES: [&str; 3] = ["tetral.log", "tetral.log.1", "tetral.log.2"];

enum Job {
    Line(Arc<[u8]>),
    Flush(mpsc::Sender<()>),
}

/// One writer thread. A record that the full queue cannot take, and a failed
/// write, are dropped and counted, so no caller ever waits for a sink.
pub struct Sink {
    sender: mpsc::Sender<Job>,
    pending: Arc<AtomicUsize>,
    capacity: usize,
    failures: Counter,
}

impl Sink {
    pub fn spawn_stderr(writer: Box<dyn Write + Send>, registry: &Registry) -> Sink {
        Sink::spawn(writer, SINK_QUEUE_RECORDS, failures(registry, "stderr"))
    }

    pub fn spawn_file(writer: Box<dyn Write + Send>, registry: &Registry) -> Sink {
        Sink::spawn(writer, SINK_QUEUE_RECORDS, failures(registry, "file"))
    }

    /// A sink holding at most `capacity` records, the one being written
    /// included.
    pub fn spawn(mut writer: Box<dyn Write + Send>, capacity: usize, failures: Counter) -> Sink {
        let (sender, jobs) = mpsc::channel::<Job>();
        let pending = Arc::new(AtomicUsize::new(0));
        let (thread_pending, thread_failures) = (pending.clone(), failures.clone());
        thread::spawn(move || {
            for job in jobs {
                match job {
                    Job::Line(line) => {
                        if writer
                            .write_all(&line)
                            .and_then(|()| writer.flush())
                            .is_err()
                        {
                            thread_failures.increment();
                        }
                        thread_pending.fetch_sub(1, Ordering::AcqRel);
                    }
                    Job::Flush(done) => {
                        let _ = done.send(());
                    }
                }
            }
        });
        Sink {
            sender,
            pending,
            capacity,
            failures,
        }
    }

    pub fn send(&self, line: Arc<[u8]>) {
        if self.reserve() {
            self.send_reserved(line);
        }
    }

    /// Takes a place in the queue for one record, or drops and counts the
    /// record when the queue is full.
    fn reserve(&self) -> bool {
        if self.pending.fetch_add(1, Ordering::AcqRel) >= self.capacity {
            self.pending.fetch_sub(1, Ordering::AcqRel);
            self.failures.increment();
            return false;
        }
        true
    }

    fn send_reserved(&self, line: Arc<[u8]>) {
        if self.sender.send(Job::Line(line)).is_err() {
            self.pending.fetch_sub(1, Ordering::AcqRel);
            self.failures.increment();
        }
    }

    /// Waits until every record sent before the call is written or dropped.
    pub fn flush(&self) {
        let (done, written) = mpsc::channel();
        if self.sender.send(Job::Flush(done)).is_ok() {
            let _ = written.recv();
        }
    }
}

pub(crate) fn failures(registry: &Registry, sink: &str) -> Counter {
    registry.counter("tetral_log_write_failures_total", &[("sink", sink)])
}

/// Stderr from the start, and the log file once one is attached.
pub struct Sinks {
    stderr: Sink,
    file: OnceLock<FileSink>,
}

enum FileSink {
    Open(Sink),
    /// The log file could not be opened: each record it would have taken
    /// counts as one it could not take.
    Unopened(Counter),
}

impl Sinks {
    pub fn new(stderr: Sink) -> Sinks {
        Sinks {
            stderr,
            file: OnceLock::new(),
        }
    }

    /// Attaches the log file; a second file is ignored.
    pub fn attach_file(&self, file: Sink) {
        let _ = self.file.set(FileSink::Open(file));
    }

    /// Stands in for a log file that could not be opened, counting in
    /// `failures` every record written from then on.
    pub fn attach_unopened_file(&self, failures: Counter) {
        let _ = self.file.set(FileSink::Unopened(failures));
    }

    pub fn write(&self, line: Vec<u8>) {
        self.write_with(|| line);
    }

    /// Writes the record that `build` makes, building it only once a sink has
    /// taken a place for it.
    pub fn write_with(&self, build: impl FnOnce() -> Vec<u8>) {
        let file = match self.file.get() {
            Some(FileSink::Open(file)) => Some(file).filter(|file| file.reserve()),
            Some(FileSink::Unopened(failures)) => {
                failures.increment();
                None
            }
            None => None,
        };
        let stderr = self.stderr.reserve();
        if file.is_none() && !stderr {
            return;
        }
        let line: Arc<[u8]> = build().into();
        if let Some(file) = file {
            file.send_reserved(line.clone());
        }
        if stderr {
            self.stderr.send_reserved(line);
        }
    }

    pub fn flush(&self) {
        self.stderr.flush();
        if let Some(FileSink::Open(file)) = self.file.get() {
            file.flush();
        }
    }
}

/// `tetral.log` in its directory, rotated into `tetral.log.1` and
/// `tetral.log.2` before a record would make it exceed its limit.
pub struct LogFile {
    directory: OwnedFd,
    file: File,
    size: u64,
    limit: u64,
}

impl LogFile {
    pub fn open(directory: &Path) -> io::Result<LogFile> {
        LogFile::open_with_limit(directory, LOG_FILE_LIMIT_BYTES)
    }

    /// Creates `directory` with mode `0700` and `tetral.log` with mode `0600`
    /// where missing, following no symbolic link, and counts the size of an
    /// existing `tetral.log`.
    pub fn open_with_limit(directory: &Path, limit: u64) -> io::Result<LogFile> {
        match rustix::fs::mkdir(directory, Mode::from_raw_mode(0o700)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        let directory = rustix::fs::open(
            directory,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let file = open_log(&directory, OFlags::empty())?;
        let size = file.metadata()?.len();
        Ok(LogFile {
            directory,
            file,
            size,
            limit,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        let [current, first, second] = LOG_NAMES;
        match rustix::fs::renameat_with(
            &self.directory,
            first,
            &self.directory,
            second,
            RenameFlags::empty(),
        ) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => {}
            Err(error) => return Err(error.into()),
        }
        rustix::fs::renameat(&self.directory, current, &self.directory, first)?;
        self.file = open_log(&self.directory, OFlags::EXCL)?;
        self.size = 0;
        Ok(())
    }
}

fn open_log(directory: &OwnedFd, extra: OFlags) -> io::Result<File> {
    let flags = OFlags::WRONLY
        | OFlags::APPEND
        | OFlags::CREATE
        | OFlags::NOFOLLOW
        | OFlags::CLOEXEC
        | extra;
    Ok(File::from(rustix::fs::openat(
        directory,
        LOG_NAMES[0],
        flags,
        Mode::from_raw_mode(0o600),
    )?))
}

impl Write for LogFile {
    /// Writes one whole record, after rotating when it would not fit.
    fn write(&mut self, record: &[u8]) -> io::Result<usize> {
        if self.size > 0 && self.size + record.len() as u64 > self.limit {
            self.rotate()?;
        }
        self.file.write_all(record)?;
        self.size += record.len() as u64;
        Ok(record.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Barrier;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::catalogue::catalogue;
    use crate::record::tests::Written;

    fn mode(path: &Path) -> u32 {
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    fn record(size: usize) -> Vec<u8> {
        let mut line = vec![b'x'; size - 1];
        line.push(b'\n');
        line
    }

    #[test]
    fn forty_mebibytes_of_records_leave_three_files_within_the_limit() {
        let root = tempfile::tempdir().unwrap();
        let logs = root.path().join("logs");
        let mut file = LogFile::open(&logs).unwrap();
        let line = record(4096);
        for _ in 0..(40 * 1024 * 1024 / line.len()) {
            file.write_all(&line).unwrap();
        }
        let mut names: Vec<String> = fs::read_dir(&logs)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["tetral.log", "tetral.log.1", "tetral.log.2"]);
        // 2,560 records of 4,096 bytes fill a file to exactly the limit, so
        // each file holds 10,485,760 bytes and none rotated early.
        for name in &names {
            let path = logs.join(name);
            assert_eq!(fs::metadata(&path).unwrap().len(), 10_485_760);
            assert_eq!(mode(&path), 0o600);
        }
        assert_eq!(mode(&logs), 0o700);
    }

    #[test]
    fn a_symbolic_link_at_the_log_directory_or_file_is_not_followed() {
        let root = tempfile::tempdir().unwrap();
        let elsewhere = root.path().join("elsewhere");
        fs::create_dir(&elsewhere).unwrap();
        let link_logs = |logs: &Path| std::os::unix::fs::symlink(&elsewhere, logs).unwrap();
        let link_log = |logs: &Path| {
            fs::create_dir(logs).unwrap();
            std::os::unix::fs::symlink(elsewhere.join("tetral.log"), logs.join("tetral.log"))
                .unwrap();
        };
        for (case, link) in [
            ("logs/", &link_logs as &dyn Fn(&Path)),
            ("tetral.log", &link_log),
        ] {
            let logs = root
                .path()
                .join(format!("logs-{}", case.trim_end_matches('/')));
            link(&logs);
            assert!(LogFile::open(&logs).is_err(), "{case} as a symbolic link");
            assert_eq!(fs::read_dir(&elsewhere).unwrap().count(), 0, "{case}");
        }
    }

    #[test]
    fn an_existing_log_is_counted_and_rotated_by_the_record_that_would_pass_the_limit() {
        let root = tempfile::tempdir().unwrap();
        let logs = root.path().join("logs");
        fs::create_dir(&logs).unwrap();
        fs::write(logs.join("tetral.log"), vec![b'x'; 10_485_000]).unwrap();
        let mut file = LogFile::open(&logs).unwrap();
        file.write_all(&record(760)).unwrap();
        assert_eq!(
            fs::metadata(logs.join("tetral.log")).unwrap().len(),
            10_485_760
        );
        assert!(!logs.join("tetral.log.1").exists());

        file.write_all(&record(1)).unwrap();
        assert_eq!(
            fs::metadata(logs.join("tetral.log.1")).unwrap().len(),
            10_485_760
        );
        assert_eq!(fs::metadata(logs.join("tetral.log")).unwrap().len(), 1);
    }

    #[test]
    fn a_log_file_that_cannot_rotate_drops_and_counts_while_stderr_goes_on() {
        let root = tempfile::tempdir().unwrap();
        let logs = root.path().join("logs");
        let registry = Registry::new(catalogue());
        let stderr = Written::default();
        let sinks = Sinks::new(Sink::spawn_stderr(Box::new(stderr.clone()), &registry));
        let file = LogFile::open_with_limit(&logs, 100).unwrap();
        sinks.attach_file(Sink::spawn_file(Box::new(file), &registry));
        fs::set_permissions(&logs, fs::Permissions::from_mode(0o500)).unwrap();

        for _ in 0..3 {
            sinks.write(record(60));
        }
        sinks.flush();
        fs::set_permissions(&logs, fs::Permissions::from_mode(0o700)).unwrap();

        assert_eq!(stderr.text().len(), 180);
        let file_failures =
            registry.counter("tetral_log_write_failures_total", &[("sink", "file")]);
        assert_eq!(file_failures.get(), 2);
        assert_eq!(fs::metadata(logs.join("tetral.log")).unwrap().len(), 60);
    }

    #[test]
    fn a_log_file_that_could_not_be_opened_counts_every_later_record() {
        let registry = Registry::new(catalogue());
        let stderr = Written::default();
        let sinks = Sinks::new(Sink::spawn_stderr(Box::new(stderr.clone()), &registry));
        sinks.attach_unopened_file(failures(&registry, "file"));

        for _ in 0..3 {
            sinks.write(record(60));
        }
        sinks.flush();

        assert_eq!(stderr.text().len(), 180);
        let file_failures =
            registry.counter("tetral_log_write_failures_total", &[("sink", "file")]);
        assert_eq!(file_failures.get(), 3);
    }

    /// A writer that blocks on a barrier before its first write.
    struct Held(Arc<Barrier>, bool);

    impl Write for Held {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if !self.1 {
                self.0.wait();
                self.1 = true;
            }
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_full_queue_drops_the_next_record_at_once_and_counts_it() {
        let registry = Registry::new(catalogue());
        let stderr_held = Arc::new(Barrier::new(2));
        let file_held = Arc::new(Barrier::new(2));
        let sinks = Sinks::new(Sink::spawn_stderr(
            Box::new(Held(stderr_held.clone(), false)),
            &registry,
        ));
        sinks.attach_file(Sink::spawn_file(
            Box::new(Held(file_held.clone(), false)),
            &registry,
        ));
        let stderr_failures =
            registry.counter("tetral_log_write_failures_total", &[("sink", "stderr")]);
        let file_failures =
            registry.counter("tetral_log_write_failures_total", &[("sink", "file")]);
        let sinks = Arc::new(sinks);
        let (counts, counted) = mpsc::channel();
        let writer = sinks.clone();
        let (stderr_count, file_count) = (stderr_failures.clone(), file_failures.clone());
        let timed = move || {
            let start = Instant::now();
            writer.write(record(10));
            start.elapsed()
        };
        thread::spawn(move || {
            let filling = (0..1_024).map(|_| timed()).max();
            let full = (stderr_count.get(), file_count.get());
            let dropping = timed();
            let _ = counts.send((
                filling,
                full,
                dropping,
                (stderr_count.get(), file_count.get()),
            ));
        });
        // Bounds the wait for a writer that hangs on a held sink, so that it
        // fails here instead of hanging the test.
        let (filling, full, dropping, after) = counted
            .recv_timeout(Duration::from_secs(10))
            .expect("no write waits for a held sink");
        // Taking a queue place costs microseconds; a write that waits for
        // room takes longer than this even on a loaded machine.
        let at_once = Duration::from_millis(500);
        assert!(
            filling.unwrap() < at_once,
            "a write to a queue with room took {filling:?}"
        );
        assert!(
            dropping < at_once,
            "the write to a full queue took {dropping:?}"
        );
        assert_eq!(full, (0, 0));
        assert_eq!(after, (1, 1));

        stderr_held.wait();
        file_held.wait();
        sinks.flush();
        assert_eq!((stderr_failures.get(), file_failures.get()), (1, 1));
    }
}
