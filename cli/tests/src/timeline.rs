//! What a failing test leaves besides its assertion: one timeline of the
//! processes the harness started, the reference control server's traffic
//! and the steps of the clocks the test moved, each line at its time since
//! the test's start.
//!
//! The harness notes what it sees as it happens, in memory: each line a
//! started process writes, each control the test sends the reference control
//! server, and each step, barrier and counter of a seam clock. At a
//! panic in any thread of the test, a hook writes the timeline, with what each
//! live part gives at that moment (the reference control server's traffic and
//! state, a seam clock's barriers and counters), beside the test's acceptance
//! record as `<record>.timeline.log` and `<record>.<part>.json`, and prints
//! where they are and the timeline's last lines. Under cargo-nextest each test
//! is a process of its own, so the timeline is the test's.
//!
//! The drive crate's tests include this file as a module of their support,
//! so it names nothing else of this package; a test without an acceptance
//! record gets its records in the acceptance directory under its own name.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime};

/// The most lines kept; past it, the oldest go first.
const MOST_LINES: usize = 100_000;
/// The longest line kept, in characters.
const LONGEST_LINE: usize = 2_000;
/// How many of the last lines a failure prints.
const PRINTED_LINES: usize = 20;

static TIMELINE: LazyLock<Timeline> = LazyLock::new(Timeline::start);

/// What a live part of the test gives when the test fails: lines for the
/// timeline, each at its time since the test's start, and files to write
/// beside it, each a name and its contents.
#[derive(Default)]
pub struct AtFailure {
    pub lines: Vec<(Duration, String)>,
    pub files: Vec<(String, String)>,
}

type Part = Box<dyn Fn() -> AtFailure + Send + Sync>;

struct Timeline {
    start: Instant,
    start_wall: SystemTime,
    lines: Mutex<Lines>,
    labels: Mutex<HashMap<String, u32>>,
    parts: Mutex<BTreeMap<String, Part>>,
    printed: AtomicBool,
    writing: Mutex<()>,
}

#[derive(Default)]
struct Lines {
    kept: VecDeque<Line>,
    dropped: u64,
}

struct Line {
    at: Duration,
    source: String,
    /// Lines with the same source and key in a row are kept as one.
    key: Option<String>,
    text: String,
    repeats: u64,
    last: Duration,
}

/// Starts the test's clock, when nothing has yet; the acceptance record
/// calls it at its creation.
pub fn start() {
    LazyLock::force(&TIMELINE);
}

/// The time since the test's start.
pub fn now() -> Duration {
    TIMELINE.start.elapsed()
}

/// `at`, a time of the wall clock, as a time since the test's start; zero
/// for a time before it.
pub fn since_start(at: SystemTime) -> Duration {
    at.duration_since(TIMELINE.start_wall).unwrap_or_default()
}

/// A name for the next part of `kind`: `<kind> 1`, `<kind> 2`, ...
pub fn label(kind: &str) -> String {
    let mut labels = lock(&TIMELINE.labels);
    let count = labels.entry(kind.to_owned()).or_default();
    *count += 1;
    format!("{kind} {count}")
}

/// Notes `text` from `source` now.
pub fn note(source: &str, text: impl Into<String>) {
    TIMELINE.push(source, None, text.into());
}

/// Notes `text` from `source` now, as one line with the line before when
/// that one has the same source and `key`.
pub fn note_repeated(source: &str, key: &str, text: impl Into<String>) {
    TIMELINE.push(source, Some(key), text.into());
}

/// Whether a line from `source` holds `text`.
pub fn holds(source: &str, text: &str) -> bool {
    lock(&TIMELINE.lines)
        .kept
        .iter()
        .any(|line| line.source == source && line.text.contains(text))
}

/// Asks `part` for what it gives at a failure until the returned guard is
/// dropped.
pub fn at_failure(label: &str, part: impl Fn() -> AtFailure + Send + Sync + 'static) -> Registered {
    lock(&TIMELINE.parts).insert(label.to_owned(), Box::new(part));
    Registered(label.to_owned())
}

/// Keeps a part in the failure's records while it lives.
pub struct Registered(String);

impl Drop for Registered {
    fn drop(&mut self) {
        lock(&TIMELINE.parts).remove(&self.0);
    }
}

/// The variable that names the directory of acceptance records, where a
/// test without a record of its own also gets its failure's records.
pub const DIRECTORY_VARIABLE: &str = "TETRAL_ACCEPTANCE_DIR";

/// The acceptance record of the test, once it has one.
static RECORD: OnceLock<PathBuf> = OnceLock::new();

/// Where the failure's records of this test go: beside its acceptance
/// record, else in the acceptance directory under the test binary's and the
/// test's names.
fn stem() -> Option<PathBuf> {
    if let Some(record) = RECORD.get() {
        return Some(record.with_extension(""));
    }
    let directory = std::env::var_os(DIRECTORY_VARIABLE)?;
    let test = std::env::var("NEXTEST_TEST_NAME")
        .unwrap_or_else(|_| std::thread::current().name().unwrap_or("test").to_owned());
    let binary = std::env::var("NEXTEST_BINARY_ID").unwrap_or_default();
    Some(PathBuf::from(directory).join(format!(
        "{}.{}_{}",
        binary.replace("::", "."),
        test.replace("::", "."),
        std::process::id()
    )))
}

/// Puts the failure's records beside the acceptance record at `record`.
pub fn beside(record: &Path) {
    let _ = RECORD.set(record.to_path_buf());
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Timeline {
    fn start() -> Timeline {
        // A hook cannot be set while the thread panics; a first use then
        // only notes.
        if !std::thread::panicking() {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |panic| {
                previous(panic);
                TIMELINE.write(&panic.to_string().replace('\n', " "));
            }));
        }
        Timeline {
            start: Instant::now(),
            start_wall: SystemTime::now(),
            lines: Mutex::default(),
            labels: Mutex::default(),
            parts: Mutex::default(),
            printed: AtomicBool::new(false),
            writing: Mutex::new(()),
        }
    }

    fn push(&self, source: &str, key: Option<&str>, mut text: String) {
        let at = self.start.elapsed();
        if let Some((cut, _)) = text.char_indices().nth(LONGEST_LINE) {
            text.truncate(cut);
            text.push_str(" [...]");
        }
        let mut lines = lock(&self.lines);
        if let (Some(key), Some(last)) = (key, lines.kept.back_mut())
            && last.source == source
            && last.key.as_deref() == Some(key)
        {
            last.repeats += 1;
            last.last = at;
            last.text = text;
            return;
        }
        if lines.kept.len() == MOST_LINES {
            lines.kept.pop_front();
            lines.dropped += 1;
        }
        lines.kept.push_back(Line {
            at,
            source: source.to_owned(),
            key: key.map(str::to_owned),
            text,
            repeats: 1,
            last: at,
        });
    }

    /// Writes the failure's records, again at each panic, and prints where
    /// they are at the first.
    // The drive crate's unit tests include this file under its deny of
    // prints; a failure's notice goes to the test's stderr.
    #[allow(clippy::print_stderr)]
    fn write(&self, panic: &str) {
        // A panic while the records are written, on this thread or another,
        // leaves them to the write under way.
        let Ok(_writing) = self.writing.try_lock() else {
            return;
        };
        let at = self.start.elapsed();
        let parts: Vec<(String, AtFailure)> = lock(&self.parts)
            .iter()
            .map(|(label, part)| (label.clone(), part()))
            .collect();
        let mut rows: Vec<(Duration, String, String)> = Vec::new();
        let dropped = {
            let lines = lock(&self.lines);
            for line in &lines.kept {
                let text = if line.repeats > 1 {
                    format!(
                        "{} (x{} until {:.3} s)",
                        line.text,
                        line.repeats,
                        line.last.as_secs_f64()
                    )
                } else {
                    line.text.clone()
                };
                rows.push((line.at, line.source.clone(), text));
            }
            lines.dropped
        };
        for (label, part) in &parts {
            for (time, text) in &part.lines {
                rows.push((*time, label.clone(), text.clone()));
            }
        }
        rows.push((at, "test".to_owned(), format!("panics: {panic}")));
        rows.sort_by_key(|(time, _, _)| *time);
        let width = rows
            .iter()
            .map(|(_, source, _)| source.len())
            .max()
            .unwrap_or(0);
        let mut text =
            String::from("# Time since the test's start, the source, and what it wrote or did.\n");
        if dropped > 0 {
            let _ = writeln!(text, "# The first {dropped} lines were dropped.");
        }
        let mut printed = Vec::new();
        for (time, source, line) in &rows {
            let row = format!("{:>10.3} s  {source:<width$}  {line}", time.as_secs_f64());
            let _ = writeln!(text, "{row}");
            printed.push(row);
        }
        let Some(stem) = stem() else {
            eprintln!("no acceptance directory for the failure's records");
            return;
        };
        let name = |suffix: &str| PathBuf::from(format!("{}.{suffix}", stem.display()));
        let mut written = vec![name("timeline.log")];
        let _ = std::fs::write(&written[0], text);
        for (label, part) in &parts {
            for (file, contents) in &part.files {
                let path = name(&format!("{}.{file}", label.replace(' ', "-")));
                let _ = std::fs::write(&path, contents);
                written.push(path);
            }
        }
        if self.printed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut notice = String::from("the failure's records:\n");
        for path in &written {
            let _ = writeln!(notice, "  {}", path.display());
        }
        let _ = writeln!(notice, "the timeline's last lines:");
        for row in &printed[printed.len().saturating_sub(PRINTED_LINES)..] {
            let _ = writeln!(notice, "  {row}");
        }
        eprint!("{notice}");
    }
}
