//! Bounded parsing of Git's progress stream. Only known stages and numeric
//! counts reach logs; arbitrary remote lines, URLs and command text do not.

use std::time::{Duration, Instant};

use observe::{RecordEnum, Stopwatch};

use super::Job;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stage(&'static str);

impl RecordEnum for Stage {
    fn record_str(&self) -> &'static str {
        self.0
    }
}

pub(super) struct Progress<'a> {
    job: &'a Job,
    line: Vec<u8>,
    discarded: bool,
    last: Option<Instant>,
    elapsed: Stopwatch,
}

impl<'a> Progress<'a> {
    pub fn new(job: &'a Job) -> Self {
        Self {
            job,
            line: Vec::new(),
            discarded: false,
            last: None,
            elapsed: Stopwatch::start(state::seams::now),
        }
    }

    pub fn read(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if matches!(byte, b'\r' | b'\n') {
                if !self.discarded {
                    self.emit();
                }
                self.line.clear();
                self.discarded = false;
            } else if self.line.len() < 1024 && !self.discarded {
                self.line.push(*byte);
            } else {
                self.discarded = true;
                self.line.clear();
            }
        }
    }

    fn emit(&mut self) {
        let Some((stage, completed, total)) = parse(&self.line) else {
            return;
        };
        if self
            .last
            .is_some_and(|last| last.elapsed() < Duration::from_secs(1))
        {
            return;
        }
        self.last = Some(Instant::now());
        let session = self
            .job
            .session_directory
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        observe::info!(
            "repository.progress",
            session.id = observe::Id(session),
            binding.id = observe::Id(self.job.binding_id.as_str()),
            repository.stage = stage,
            repository.completed = completed,
            repository.total = total,
            elapsed_ms = self.elapsed.elapsed()
        );
    }
}

fn parse(line: &[u8]) -> Option<(Stage, u64, u64)> {
    let line = std::str::from_utf8(line).ok()?.trim();
    let line = line.strip_prefix("remote: ").unwrap_or(line);
    let (name, rest) = line.split_once(": ")?;
    let stage = match name {
        "Enumerating objects" => "enumerating",
        "Counting objects" => "counting",
        "Compressing objects" => "compressing",
        "Receiving objects" => "receiving",
        "Resolving deltas" => "resolving",
        "Updating files" => "checkout",
        _ => return None,
    };
    let (completed, total) = if let Some((_, counts)) = rest.split_once('(') {
        let (counts, _) = counts.split_once(')')?;
        let (completed, total) = counts.split_once('/')?;
        (completed.parse().ok()?, total.parse().ok()?)
    } else {
        let count = rest.split(',').next()?.parse().ok()?;
        (count, count)
    };
    (completed <= total).then_some((Stage(stage), completed, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_progress_stages_and_numeric_counts_are_retained() {
        assert_eq!(
            parse(b"remote: Counting objects: 50% (4/8), done."),
            Some((Stage("counting"), 4, 8))
        );
        assert_eq!(
            parse(b"Receiving objects: 100% (8/8), 1.00 KiB | 3 KiB/s"),
            Some((Stage("receiving"), 8, 8))
        );
        assert_eq!(
            parse(b"Enumerating objects: 8, done."),
            Some((Stage("enumerating"), 8, 8))
        );
        for line in [
            b"remote: secret".as_slice(),
            b"Counting objects: (secret/8)",
            b"Counting objects: (9/8)",
            b"fatal: credential in a URL",
        ] {
            assert_eq!(parse(line), None);
        }
    }
}
