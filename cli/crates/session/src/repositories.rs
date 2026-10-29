//! A cloud computer's repositories: the path checks against the state root
//! and against the clones this Session made (`repositories.json`), the
//! clone or reuse of each repository through git-proxy, and the failures a
//! preparation report lists.
//!
//! Repositories are handled one at a time, each `git` a child process in its
//! own process group with the App Server's environment and
//! `LC_ALL=C.UTF-8` and `GIT_TERMINAL_PROMPT=0` set over it, so that the
//! machine's proxy and certificate settings apply and Git never waits for a
//! prompt. The ticket, the URL rewrite to git-proxy and the commit identity
//! go only into each repository's own configuration; `origin` stays the
//! repository's `url`. A clone that succeeds is recorded in
//! `repositories.json` even after its preparation stopped being current, so
//! that the next preparation reuses it instead of failing with
//! `repository_path_exists`.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use protocol::connection::{
    Checkout, GitTicketResponse, Repository, RepositoryFailure, RepositoryFailureCause,
};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

/// The file under the Session's directory that lists the paths of the
/// repositories this Session cloned on this machine.
pub const REPOSITORIES_FILE: &str = "repositories.json";

/// The bytes of a repository failure's `message`.
pub const MESSAGE_MAX_BYTES: usize = 1_024;

/// The bytes of a `git` child's stderr kept, its last ones, from which the
/// cause and the message are read.
pub const STDERR_KEPT_BYTES: usize = 65_536;

/// The paths `repositories.json` lists; none when it is missing or does not
/// parse.
pub fn listed(session_directory: &Path) -> Vec<String> {
    std::fs::read(session_directory.join(REPOSITORIES_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Adds `path` to `repositories.json` by atomic replacement.
fn record(session_directory: &Path, path: &str) -> Result<(), String> {
    let mut paths = listed(session_directory);
    if !paths.iter().any(|listed| listed == path) {
        paths.push(path.to_owned());
    }
    state::files::create_private_dirs(session_directory)
        .and_then(|()| {
            let bytes = serde_json::to_vec(&paths).expect("paths encode");
            state::files::replace(&session_directory.join(REPOSITORIES_FILE), &bytes)
        })
        .map_err(|error| format!("{}: {}", error.path.display(), error.source))
}

/// What a preparation's repositories need once the ticket came.
pub struct Job {
    pub session_directory: PathBuf,
    pub repositories: Vec<Repository>,
    pub ticket: GitTicketResponse,
    /// Whether the preparation is still current; the owner clears it when
    /// the preparation ends.
    pub current: Arc<AtomicBool>,
}

/// Clones or reuses each repository in order and gives the failures.
pub async fn run(job: Job) -> Vec<RepositoryFailure> {
    let mut failures = Vec::new();
    let listed = listed(&job.session_directory);
    for repository in &job.repositories {
        let path = repository.path.as_str();
        let outcome = if listed.iter().any(|each| each == path) {
            reuse(repository, &job.ticket).await
        } else {
            clone(&job, repository).await
        };
        if let Err((cause, message)) = outcome {
            failures.push(RepositoryFailure {
                url: repository.url.clone(),
                path: repository.path.clone(),
                cause,
                message: cut(&message, MESSAGE_MAX_BYTES).to_owned(),
            });
        }
    }
    failures
}

/// The failure every repository gets when the ticket fetch failed.
pub fn ticket_failures(repositories: &[Repository], message: &str) -> Vec<RepositoryFailure> {
    repositories
        .iter()
        .map(|repository| RepositoryFailure {
            url: repository.url.clone(),
            path: repository.path.clone(),
            cause: RepositoryFailureCause::Clone,
            message: cut(message, MESSAGE_MAX_BYTES).to_owned(),
        })
        .collect()
}

type Failure = (RepositoryFailureCause, String);

/// The four keys of the repository's local configuration: the URL rewrite to
/// git-proxy, the ticket's header on every request to it, and the commit
/// identity.
fn keys(repository: &Repository, ticket: &GitTicketResponse) -> [(String, String); 4] {
    let proxy = ticket.git_proxy_url.as_str();
    [
        (
            format!("url.{proxy}/github.com/.insteadOf"),
            "https://github.com/".to_owned(),
        ),
        (
            format!("http.{proxy}/.extraHeader"),
            format!("X-Tetral-Git-Ticket: {}", ticket.ticket.as_str()),
        ),
        ("user.name".to_owned(), repository.git_identity.name.clone()),
        (
            "user.email".to_owned(),
            repository.git_identity.email.clone(),
        ),
    ]
}

/// A clone this Session made: nothing is cloned, fetched or checked out;
/// the four keys are replaced, the new ticket among them.
async fn reuse(repository: &Repository, ticket: &GitTicketResponse) -> Result<(), Failure> {
    let path = repository.path.as_str();
    for (key, value) in keys(repository, ticket) {
        git(&[
            "-C",
            path,
            "config",
            "--local",
            "--replace-all",
            &key,
            &value,
        ])
        .await
        .map_err(|stderr| (RepositoryFailureCause::Clone, last_line(&stderr)))?;
    }
    Ok(())
}

async fn clone(job: &Job, repository: &Repository) -> Result<(), Failure> {
    let path = repository.path.as_str();
    let mut arguments = vec!["clone".to_owned()];
    for (key, value) in keys(repository, &job.ticket) {
        arguments.push("-c".to_owned());
        arguments.push(format!("{key}={value}"));
    }
    arguments.push(repository.url.as_str().to_owned());
    arguments.push(path.to_owned());
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    git(&arguments)
        .await
        .map_err(|stderr| (clone_cause(&stderr), last_line(&stderr)))?;
    let current = job.current.load(Ordering::Acquire);
    if current {
        record(&job.session_directory, path)
            .map_err(|error| (RepositoryFailureCause::Clone, error))?;
    }
    let checkout = match &repository.checkout.0 {
        None => Ok(()),
        Some(Checkout::Branch { name, .. }) => git(&["-C", path, "checkout", name.as_str()]).await,
        Some(Checkout::Commit { sha, .. }) => {
            git(&["-C", path, "checkout", "--detach", sha.as_str()]).await
        }
    };
    checkout.map_err(|stderr| (RepositoryFailureCause::Checkout, last_line(&stderr)))?;
    if !current {
        record(&job.session_directory, path)
            .map_err(|error| (RepositoryFailureCause::Clone, error))?;
    }
    Ok(())
}

/// Runs `git` with `arguments`, counted as the test seam counter
/// `git.spawn`; the kept end of its stderr when it fails or cannot start.
async fn git(arguments: &[&str]) -> Result<(), String> {
    state::seams::count("git.spawn");
    let mut child = Command::new("git")
        .args(arguments)
        .current_dir("/")
        .env("LC_ALL", "C.UTF-8")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("git could not start: {error}"))?;
    let mut stderr = child.stderr.take().expect("stderr is piped");
    let mut kept = Vec::new();
    let mut chunk = [0u8; 8_192];
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                kept.extend_from_slice(&chunk[..read]);
                if kept.len() > STDERR_KEPT_BYTES {
                    kept.drain(..kept.len() - STDERR_KEPT_BYTES);
                }
            }
        }
    }
    let status = child
        .wait()
        .await
        .map_err(|error| format!("git could not be waited for: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&kept).into_owned())
    }
}

/// The cause that a failed `git clone`'s stderr names, checked in this
/// order.
fn clone_cause(stderr: &str) -> RepositoryFailureCause {
    if stderr.contains("The requested URL returned error: 403") {
        RepositoryFailureCause::Forbidden
    } else if [
        "could not read Username for",
        "Authentication failed for",
        "The requested URL returned error: 401",
        "The requested URL returned error: 424",
    ]
    .iter()
    .any(|text| stderr.contains(text))
    {
        RepositoryFailureCause::Authentication
    } else if stderr
        .lines()
        .any(|line| line.starts_with("fatal: repository '") && line.ends_with("' not found"))
    {
        RepositoryFailureCause::NotFound
    } else {
        RepositoryFailureCause::Clone
    }
}

/// The last nonempty line of `stderr`.
fn last_line(stderr: &str) -> String {
    stderr
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
        .to_owned()
}

/// `text` cut to at most `max_bytes` at a UTF-8 character boundary.
pub fn cut(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Shortens `failures`' messages from the last failure backwards, each to
/// the bytes that still fit and down to empty, until `size` of them is at
/// most `max_bytes`.
pub fn fit(
    failures: &mut [RepositoryFailure],
    max_bytes: usize,
    mut size: impl FnMut(&[RepositoryFailure]) -> usize,
) {
    for index in (0..failures.len()).rev() {
        let over = size(failures).saturating_sub(max_bytes);
        if over == 0 {
            return;
        }
        let message = &failures[index].message;
        let keep = message.len().saturating_sub(over);
        failures[index].message = cut(message, keep).to_owned();
        // An escaped character may count more than its bytes: cut further
        // while the report still does not fit and the message has bytes.
        while size(failures) > max_bytes && !failures[index].message.is_empty() {
            let shorter = failures[index].message.len() - 1;
            failures[index].message = cut(&failures[index].message, shorter).to_owned();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(message: &str) -> RepositoryFailure {
        serde_json::from_value(serde_json::json!({
            "url": "https://github.com/acme/a", "path": "/w/a",
            "cause": "clone", "message": message,
        }))
        .unwrap()
    }

    #[test]
    fn a_report_over_its_limit_is_fitted_from_the_last_failure_backwards_to_exactly_what_fits() {
        let size = |failures: &[RepositoryFailure]| serde_json::to_vec(failures).unwrap().len();
        let mut failures = vec![failure(&"a".repeat(600)), failure(&"é".repeat(300))];
        let total = size(&failures);
        let limit = total - 700;
        fit(&mut failures, limit, size);
        assert_eq!(size(&failures), limit);
        assert_eq!(failures[1].message, "");
        assert_eq!(failures[0].message.len(), 600 - (700 - 600));
        let mut fitting = vec![failure("short")];
        let before = fitting.clone();
        fit(&mut fitting, 1_000, size);
        assert_eq!(fitting, before);
    }
}
