//! Owned file removal after execution and binding access have ended. Each
//! failed cleanup root contributes its first error; this is not a recursive
//! inventory. Closed reports reserve room for one error before fitting lost
//! changes, then add remaining roots while the complete message fits.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use protocol::connection::{
    CleanupFailure, CleanupFailures, LostChange, SessionOutcome, SessionStatus,
};
use protocol::field::{AbsolutePath, List};
use protocol::id::SafeId;

pub(crate) fn remove(
    root: &Path,
    directory: &Path,
    session: &SafeId,
    staging: &[std::path::PathBuf],
) -> Vec<CleanupFailure> {
    let mut failures = BTreeMap::new();
    let detached = match drive::mount::detach_remaining(directory) {
        Ok(()) => true,
        Err(error) => {
            failures.insert(directory.to_path_buf(), std::io::Error::other(error));
            false
        }
    };
    for target in staging {
        match crate::repositories::staging(directory, target) {
            Ok(path) => remove_root(&path, &mut failures),
            Err(error) => {
                failures.entry(target.clone()).or_insert(error);
            }
        }
    }
    let (repositories, inventory_read) = match crate::repositories::read_listed(directory) {
        Ok(paths) => (paths, true),
        Err(error) => {
            failures.entry(directory.to_path_buf()).or_insert_with(|| {
                std::io::Error::other(format!("repository ownership inventory: {error}"))
            });
            (Vec::new(), false)
        }
    };
    for path in repositories {
        if detached || !Path::new(&path).starts_with(directory) {
            remove_root(Path::new(&path), &mut failures);
        }
    }
    let parent = execution::store::task_parent(root);
    match std::fs::read_dir(directory.join(execution::store::TASKS)) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry) => {
                        if let Some(name) = entry.file_name().to_str() {
                            remove_root(
                                &execution::store::task_temporary_directory(&parent, name),
                                &mut failures,
                            );
                        }
                    }
                    Err(error) => {
                        failures.entry(directory.to_path_buf()).or_insert(error);
                    }
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            failures.entry(directory.to_path_buf()).or_insert(error);
        }
    }
    if detached && inventory_read {
        remove_root(directory, &mut failures);
    }
    failures
        .into_iter()
        .map(|(path, error)| {
            let failure = failure(&path, &error.to_string());
            observe::warn!(
                "session.cleanup_failed",
                session.id = observe::Id(session.as_str()),
                cleanup.directory = observe::Id(failure.path.as_str()),
                error.os_code = i64::from(error.raw_os_error().unwrap_or(0)),
                error.code = observe::Code("session_cleanup_failed")
            );
            failure
        })
        .collect()
}

fn remove_root(path: &Path, failures: &mut BTreeMap<PathBuf, std::io::Error>) {
    if let Err(error) = execution::store::remove_owned_tree(path) {
        failures.entry(path.to_path_buf()).or_insert(error);
    }
}

/// Paths remain absolute; control characters are escaped for display.
/// Error text cannot inject terminal controls and is bounded in UTF-8 bytes.
pub(crate) fn failure(path: &Path, message: &str) -> CleanupFailure {
    let sanitize = |text: &str| {
        text.chars()
            .flat_map(|c| {
                if c.is_control() {
                    c.escape_default().collect::<Vec<_>>()
                } else {
                    vec![c]
                }
            })
            .collect::<String>()
    };
    let path = sanitize(&path.to_string_lossy());
    let message = sanitize(message);
    CleanupFailure {
        path: AbsolutePath::new(path).expect("cleanup root is absolute"),
        message: crate::repositories::cut(&message, 1024).to_owned(),
    }
}

pub(crate) fn fit(
    mut status: SessionStatus,
    lost: &[LostChange],
    failures: &[CleanupFailure],
) -> SessionStatus {
    let mut cleanup_failures: Option<CleanupFailures> =
        failures.first().cloned().map(|first| List(vec![first]));
    let build = |lost_changes, cleanup_failures| SessionStatus {
        outcome: SessionOutcome::Closed {
            lost_changes,
            cleanup_failures,
        },
        ..status.clone()
    };
    let lost_changes = drive::lost_changes::fit(lost, |candidate| {
        protocol::encode(&build(candidate.cloned(), cleanup_failures.clone())).len()
    });
    if let Some(kept) = &mut cleanup_failures {
        for failure in failures.iter().skip(1) {
            kept.0.push(failure.clone());
            if protocol::encode(&build(lost_changes.clone(), Some(kept.clone()))).len()
                > protocol::connection::MESSAGE_MAX_BYTES
            {
                kept.0.pop();
                break;
            }
        }
    }
    status.outcome = SessionOutcome::Closed {
        lost_changes,
        cleanup_failures,
    };
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::id::Id;

    #[test]
    fn failed_repository_inventory_keeps_ownership_state_and_reports_the_session_root() {
        for malformed in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let directory = root.path().join("sessions/sess_a");
            std::fs::create_dir_all(&directory).unwrap();
            let clone = root.path().join("external-clone");
            std::fs::create_dir(&clone).unwrap();
            std::fs::write(clone.join("content"), b"retained").unwrap();
            let inventory = directory.join(crate::repositories::REPOSITORIES_FILE);
            if malformed {
                std::fs::write(&inventory, b"[unfinished ownership record").unwrap();
            } else {
                // A real read failure independent of root/non-root test users.
                std::fs::create_dir(&inventory).unwrap();
            }
            let failures = remove(
                root.path(),
                &directory,
                &SafeId::new("sess_a").unwrap(),
                &[],
            );
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].path.as_str(), directory.to_str().unwrap());
            assert!(
                failures[0]
                    .message
                    .contains("repository ownership inventory")
            );
            assert!(inventory.exists(), "retain the only ownership record");
            assert_eq!(std::fs::read(clone.join("content")).unwrap(), b"retained");

            // Once the inventory is readable, the same cleanup removes both roots.
            if !malformed {
                std::fs::remove_dir(&inventory).unwrap();
            }
            std::fs::write(&inventory, serde_json::to_vec(&vec![clone]).unwrap()).unwrap();
            assert!(
                remove(
                    root.path(),
                    &directory,
                    &SafeId::new("sess_a").unwrap(),
                    &[]
                )
                .is_empty()
            );
            assert!(!directory.exists());
            assert!(!root.path().join("external-clone").exists());
        }
    }

    #[test]
    fn closed_keeps_a_root_error_and_lost_change_counts_inside_the_complete_envelope() {
        let id = |s: &str| Id::new(s).unwrap();
        let status = SessionStatus {
            connection_id: id("conn_a"),
            message_id: id("report_a"),
            request_message_id: id("close_a"),
            session_id: id("sess_a"),
            binding_id: id("bind_a"),
            outcome: SessionOutcome::Closed {
                lost_changes: None,
                cleanup_failures: None,
            },
        };
        let failures: Vec<_> = (0..40)
            .map(|i| {
                failure(
                    Path::new(&format!("/owned/{i:03}/{}", "界".repeat(900))),
                    &format!("denied\n\u{1b}[31m{}", "界".repeat(600)),
                )
            })
            .collect();
        assert!(
            failures
                .iter()
                .all(|f| f.message.len() <= 1024 && !f.message.chars().any(char::is_control))
        );
        let lost = drive::lost_changes::entries(
            &id("close_a"),
            (0..100).map(|i| {
                (
                    "drive_a".into(),
                    format!("{i:03}/{}", vec!["x".repeat(230); 4].join("/")),
                )
            }),
        );
        let fitted = fit(status, &lost, &failures);
        let bytes = protocol::encode(&fitted);
        assert!(bytes.len() <= protocol::connection::MESSAGE_MAX_BYTES);
        let decoded: SessionStatus = protocol::decode(&bytes).unwrap();
        let SessionOutcome::Closed {
            cleanup_failures: Some(kept),
            lost_changes: Some(lost),
        } = decoded.outcome
        else {
            panic!("both diagnostics remain")
        };
        assert_eq!(kept[0], failures[0]);
        assert!(kept.len() < failures.len());
        assert_eq!(lost[0].paths_total.get(), 100);
        assert!(!lost[0].paths.is_empty());
        assert!(lost[0].paths.len() < 100);
    }
}
