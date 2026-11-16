//! A test's view of what the kernel asks of a mounted drive and what it is
//! told: each operation that either mount adapter hands the drive sync owner,
//! the owner's reply, and each notice that drops the kernel's cached pages.
//! It only observes: nothing it does changes a reply, an order or a wait, and
//! a build without the feature `test-seams` has none of it.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use rustix::io::Errno;

use crate::ops::{Done, Op, Source};

type Watcher = Box<dyn Fn(String) + Send + Sync>;

static WATCHER: OnceLock<Watcher> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(1);

/// Gives `watcher` each operation, reply and notice of every drive of this
/// process from now on, as one line of text; the first call in a process
/// sets it.
pub fn watch(watcher: impl Fn(String) + Send + Sync + 'static) {
    let _ = WATCHER.set(Box::new(watcher));
}

/// Gives the watcher `line`, built only when there is one.
pub(crate) fn seen(line: impl FnOnce() -> String) {
    if let Some(watcher) = WATCHER.get() {
        watcher(line());
    }
}

/// Notes that the operation `op` of `drive` is handed to the owner, and
/// gives the number its reply is noted under; none without a watcher.
pub(crate) fn asked(drive: &str, op: &Op) -> Option<u64> {
    WATCHER.get()?;
    let number = NEXT.fetch_add(1, Ordering::Relaxed);
    seen(|| format!("#{number} {drive}: {}", describe(op)));
    Some(number)
}

/// Notes the owner's reply to the operation noted as `number`.
pub(crate) fn answered(number: Option<u64>, result: &Result<Done, Errno>) {
    let Some(number) = number else { return };
    seen(|| {
        let reply = match result {
            Ok(Done::Attr(attr)) => format!(
                "attributes of inode {}: {:?}, {} bytes, mode {:o}",
                attr.ino, attr.kind, attr.size, attr.mode
            ),
            Ok(Done::Entries(entries)) => format!(
                "{} entries: {}",
                entries.len(),
                entries
                    .iter()
                    .map(|entry| entry.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Ok(Done::Source(Source::File(_))) => "read from the content file".to_owned(),
            Ok(Done::Source(Source::Cache { needed, .. })) => {
                format!("read from cached blob {}", needed.sha256.as_str())
            }
            Ok(Done::Link(target)) => {
                format!("link to {}", String::from_utf8_lossy(target))
            }
            Ok(Done::Written(bytes)) => format!("{bytes} bytes written"),
            Ok(Done::NeedBlob(hash, _)) => format!("needs blob {hash} first"),
            Ok(Done::Retry) => "retry".to_owned(),
            Ok(Done::Unit) => "done".to_owned(),
            Err(errno) => format!("error {errno:?}"),
        };
        format!("#{number} answered: {reply}")
    });
}

fn describe(op: &Op) -> String {
    let name = |name: &[u8]| String::from_utf8_lossy(name).into_owned();
    let by = |task: &Option<String>| {
        task.as_deref()
            .map(|task| format!(" by {task}"))
            .unwrap_or_default()
    };
    match op {
        Op::Mounted { detached, op } => {
            let detached = if detached.load(Ordering::Acquire) {
                " (detached)"
            } else {
                ""
            };
            format!("{}{detached}", describe(op))
        }
        Op::Open(ino) => format!("open inode {ino}"),
        Op::Release(ino) => format!("release inode {ino}"),
        Op::ReadOpened { ino, hash, .. } => format!("read inode {ino} from opened blob {hash}"),
        Op::Lookup {
            parent,
            name: entry,
        } => {
            format!("look up {:?} in inode {parent}", name(entry))
        }
        Op::GetAttr(ino) => format!("get attributes of inode {ino}"),
        Op::ReadDir(ino) => format!("list inode {ino}"),
        Op::Read(ino) => format!("read inode {ino}"),
        Op::ReadLink(ino) => format!("read link inode {ino}"),
        Op::Create {
            open,
            parent,
            name: entry,
            mode,
            task,
        } => format!(
            "create {:?} in inode {parent}, mode {mode:o}{}{}",
            name(entry),
            if *open { ", open" } else { "" },
            by(task)
        ),
        Op::Write {
            ino,
            offset,
            data,
            task,
            ..
        } => format!(
            "write {} bytes at {offset} of inode {ino}{}",
            data.len(),
            by(task)
        ),
        Op::SetAttr { ino, set, task, .. } => {
            format!("set attributes of inode {ino}: {set:?}{}", by(task))
        }
        Op::MakeFolder {
            parent,
            name: entry,
            task,
        } => format!(
            "make folder {:?} in inode {parent}{}",
            name(entry),
            by(task)
        ),
        Op::Remove {
            parent,
            name: entry,
            task,
        } => format!("remove {:?} from inode {parent}{}", name(entry), by(task)),
        Op::RemoveFolder {
            parent,
            name: entry,
            task,
        } => format!(
            "remove folder {:?} from inode {parent}{}",
            name(entry),
            by(task)
        ),
        Op::Rename {
            parent,
            name: entry,
            new_parent,
            new_name,
            task,
        } => format!(
            "rename {:?} in inode {parent} to {:?} in inode {new_parent}{}",
            name(entry),
            name(new_name),
            by(task)
        ),
        Op::Symlink {
            parent,
            name: entry,
            target,
            task,
        } => format!(
            "link {:?} in inode {parent} to {}{}",
            name(entry),
            name(target),
            by(task)
        ),
        Op::MakeSpecial {
            parent,
            name: entry,
            special,
            task,
        } => format!(
            "make special {:?} {special:?} in inode {parent}{}",
            name(entry),
            by(task)
        ),
        Op::Sync(ino) => format!("sync inode {ino}"),
    }
}
