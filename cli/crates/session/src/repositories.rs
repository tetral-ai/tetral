//! A cloud computer's repositories: the clones this Session made
//! (`repositories.json`), the clone or reuse of each repository through
//! git-proxy, and the failures a preparation report lists.
//!
//! Repositories are handled one at a time through the existing OS isolation.
//! Ordinary Git configuration, hooks, filters and transport settings remain
//! active, while the shared mandatory protections deny private resources and
//! keep ordinary state read-only. Command environment is installed only inside
//! the sandbox, without CLI control variables; Git never waits for a
//! prompt. The ticket, the URL rewrite to git-proxy and the commit identity
//! go only into each repository's own configuration; `origin` stays the
//! repository's `url`, and a clone that receives nothing for 60 seconds
//! fails. A clone and its requested checkout complete in reserved sibling
//! staging before recording in `repositories.json` and publication. A failed
//! checkout covering cwd leaves no reusable default-branch clone. An independent
//! checkout failure retains the existing current-preparation reuse behavior.
//! The current repository
//! may finish after disconnect, but no next repository starts. Stop and close
//! kill the Git process group and PID namespace and wait for the job to end.

mod progress;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sha2::{Digest, Sha256};

use protocol::connection::{
    Checkout, GitTicketResponse, Repository, RepositoryFailure, RepositoryFailureCause,
};
use tokio::io::AsyncReadExt;
use tokio::sync::watch;

/// The file under the Session's directory that lists the paths of the
/// repositories this Session cloned on this machine.
pub const REPOSITORIES_FILE: &str = "repositories.json";

/// The bytes of a repository failure's `message`.
pub const MESSAGE_MAX_BYTES: usize = 1_024;

/// The failure of a `git` child that stop or close killed.
const KILLED: &str = "git was stopped";

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
    pub protections: execution::policy::PreparationInputs,
    pub tetral: PathBuf,
    pub drive_sync: Option<drive::DriveSync>,
    pub bound: Vec<drive::mount::Bound>,
    pub session_directory: PathBuf,
    pub binding_id: protocol::id::Id,
    pub cwd_repositories: Vec<protocol::field::AbsolutePath>,
    pub repositories: Vec<Repository>,
    pub ticket: GitTicketResponse,
    /// Whether the preparation is still current; the owner clears it when
    /// the preparation ends.
    pub current: Arc<AtomicBool>,
    /// True once stop or close kills the job.
    pub killed: watch::Receiver<bool>,
}

/// What the Session binding owner keeps of a job that runs.
pub struct Running {
    pub current: Arc<AtomicBool>,
    pub kill: watch::Sender<bool>,
}

/// Clones or reuses each repository in order and gives the failures; a
/// killed job stops.
pub async fn run(job: Job) -> Vec<RepositoryFailure> {
    let mut failures = Vec::new();
    let listed = listed(&job.session_directory);
    for repository in &job.repositories {
        if *job.killed.borrow() || !job.current.load(Ordering::Acquire) {
            break;
        }
        let path = repository.path.as_str();
        // A listed clone that is gone is cloned again.
        let outcome =
            if listed.iter().any(|each| each == path) && std::fs::symlink_metadata(path).is_ok() {
                reuse(repository, &job).await
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
async fn reuse(repository: &Repository, job: &Job) -> Result<(), Failure> {
    let path = repository.path.as_str();
    for (key, value) in keys(repository, &job.ticket) {
        git(
            &[
                "-C",
                path,
                "config",
                "--local",
                "--replace-all",
                &key,
                &value,
            ],
            job,
        )
        .await
        .map_err(|stderr| (RepositoryFailureCause::Clone, last_line(&stderr)))?;
    }
    Ok(())
}

/// The exact reserved sibling owned by this Session and target. Retry and
/// close remove only this path; no sibling scan or mutable registry is used.
pub fn staging(session_directory: &Path, target: &Path) -> std::io::Result<PathBuf> {
    let mut ancestor = target
        .parent()
        .ok_or_else(|| std::io::Error::other("repository has no parent"))?;
    let mut missing = Vec::new();
    let parent = loop {
        match std::fs::canonicalize(ancestor) {
            Ok(mut parent) => {
                for name in missing.iter().rev() {
                    parent.push(name);
                }
                break parent;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(
                    ancestor
                        .file_name()
                        .ok_or_else(|| std::io::Error::other("repository parent is unavailable"))?
                        .to_owned(),
                );
                ancestor = ancestor
                    .parent()
                    .ok_or_else(|| std::io::Error::other("repository parent is unavailable"))?;
            }
            Err(error) => return Err(error),
        }
    };
    let canonical = parent.join(
        target
            .file_name()
            .ok_or_else(|| std::io::Error::other("repository has no name"))?,
    );
    let digest = Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    let digest: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    let session = session_directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| std::io::Error::other("Session directory has no identity"))?;
    Ok(parent.join(format!(".tetral-clone-{session}-{digest}")))
}

async fn clone(job: &Job, repository: &Repository) -> Result<(), Failure> {
    let target = Path::new(repository.path.as_str());
    if std::fs::symlink_metadata(target).is_ok() {
        return Err((
            RepositoryFailureCause::Clone,
            "repository target already exists".into(),
        ));
    }
    let scratch = staging(&job.session_directory, target).map_err(|error| {
        (
            RepositoryFailureCause::Clone,
            format!("clone staging could not be resolved: {error}"),
        )
    })?;
    execution::store::remove_owned_tree(&scratch).map_err(|error| {
        (
            RepositoryFailureCause::Clone,
            format!("clone staging cleanup failed: {error}"),
        )
    })?;
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            (
                RepositoryFailureCause::Clone,
                format!("clone parent could not be created: {error}"),
            )
        })?;
    }
    let outcome = clone_staged(job, repository, &scratch).await;
    if let Err(error) = execution::store::remove_owned_tree(&scratch) {
        return Err((
            RepositoryFailureCause::Clone,
            format!("clone staging cleanup failed: {error}"),
        ));
    }
    outcome
}

async fn clone_staged(job: &Job, repository: &Repository, scratch: &Path) -> Result<(), Failure> {
    let path = scratch.to_str().ok_or_else(|| {
        (
            RepositoryFailureCause::Clone,
            "canonical repository path is not UTF-8".to_owned(),
        )
    })?;
    // Not persisted: git aborts a transfer that receives nothing for 60 s.
    let mut arguments = [
        "-c",
        "http.lowSpeedLimit=1",
        "-c",
        "http.lowSpeedTime=60",
        "clone",
        "--progress",
    ]
    .map(str::to_owned)
    .to_vec();
    for (key, value) in keys(repository, &job.ticket) {
        arguments.push("-c".to_owned());
        arguments.push(format!("{key}={value}"));
    }
    arguments.push(repository.url.as_str().to_owned());
    arguments.push(path.to_owned());
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    git(&arguments, job)
        .await
        .map_err(|stderr| (clone_cause(&stderr), last_line(&stderr)))?;
    let checkout = match &repository.checkout.0 {
        None => Ok(()),
        Some(Checkout::Branch { name, .. }) => {
            git(&["-C", path, "checkout", name.as_str(), "--"], job).await
        }
        Some(Checkout::Commit { sha, .. }) => {
            git(&["-C", path, "checkout", "--detach", sha.as_str()], job).await
        }
    };
    let checkout =
        checkout.map_err(|stderr| (RepositoryFailureCause::Checkout, last_line(&stderr)));
    if checkout.is_err()
        && (job.cwd_repositories.contains(&repository.path) || !job.current.load(Ordering::Acquire))
    {
        return checkout;
    }
    let target = Path::new(repository.path.as_str());
    if std::fs::symlink_metadata(target).is_ok() {
        return Err((
            RepositoryFailureCause::Clone,
            "repository target appeared during clone".into(),
        ));
    }
    // Record before publication: if the process dies between them, the
    // next preparation sees an absent target and retries its owned staging.
    record(&job.session_directory, repository.path.as_str())
        .map_err(|error| (RepositoryFailureCause::Clone, error))?;
    let mut killed = job.killed.clone();
    tokio::select! {
        () = state::seams::barrier("repository.recorded") => {},
        _ = killed.wait_for(|killed| *killed) => return Err((RepositoryFailureCause::Clone, KILLED.to_owned())),
    }
    std::fs::rename(scratch, target).map_err(|error| {
        (
            RepositoryFailureCause::Clone,
            format!("clone could not be published: {error}"),
        )
    })?;
    checkout
}

/// Runs `git` with `arguments`, counted as the test seam counter
/// `git.spawn`; the kept end of its stderr when it fails or cannot start.
/// A kill of the job kills its process group and reaps it.
async fn git(arguments: &[&str], job: &Job) -> Result<(), String> {
    let killed = &job.killed;
    if *killed.borrow() {
        return Err(KILLED.to_owned());
    }
    let mut protections = job.protections.clone();
    let mut killed_while_starting = killed.clone();
    let launch = async {
        if let Some(sync) = &job.drive_sync {
            let unmounted = sync.check_mount_access(job.bound.clone()).await;
            if let Some(drives) = &mut protections.drives {
                drives.mounts.retain(|mount| {
                    !unmounted
                        .iter()
                        .any(|id| mount.path == drives.directory.join(id.as_str()))
                });
            }
        }
        let permissions = execution::policy::TaskPermissions::preparation(&protections)
            .map_err(|error| error.message)?;
        execution::isolation::Isolation::new()
            .preparation(&permissions, &job.tetral, arguments)
            .await
            .map_err(|error| match error {
                execution::isolation::LaunchError::Rejected(error) => {
                    format!("preparation isolation unavailable: {}", error.message)
                }
                execution::isolation::LaunchError::Spawn(_) => {
                    "isolated git could not start".into()
                }
            })
    };
    let process = tokio::select! {
        result = launch => result?,
        _ = killed_while_starting.wait_for(|killed| *killed) => return Err(KILLED.into()),
    };
    state::seams::count("git.spawn");
    let mut child = process.child;
    // No command input is needed. Drain stdout so configured helpers cannot
    // block on a full pipe while stderr/exit are being collected.
    drop(child.stdin.take());
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let pid = process.pid;
    let mut stderr = child.stderr.take().expect("stderr is piped");
    let read_error = async {
        let mut progress = progress::Progress::new(job);
        let mut kept = Vec::new();
        let mut chunk = [0u8; 8_192];
        loop {
            match stderr.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    progress.read(&chunk[..read]);
                    kept.extend_from_slice(&chunk[..read]);
                    if kept.len() > STDERR_KEPT_BYTES {
                        kept.drain(..kept.len() - STDERR_KEPT_BYTES);
                    }
                }
            }
        }
        kept
    };
    let reap = async {
        let mut sink = tokio::io::sink();
        let (_, kept, status) = tokio::join!(
            tokio::io::copy(&mut stdout, &mut sink),
            read_error,
            child.wait()
        );
        (status, kept)
    };
    tokio::pin!(reap);
    let mut killed = killed.clone();
    let kill = async move {
        if killed.wait_for(|killed| *killed).await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    let (status, kept) = tokio::select! {
        biased;
        () = kill => {
            execution::terminate::Termination::kill(pid, execution::terminate::Steps::default());
            let _ = reap.await;
            return Err(KILLED.to_owned());
        }
        reaped = &mut reap => reaped,
    };
    let status = status.map_err(|error| format!("git could not be waited for: {error}"))?;
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
/// most `max_bytes`. Cutting a byte shrinks the encoding by at least a
/// byte, so a message with escaped characters may lose a few more bytes
/// than strictly needed.
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
    }
}
