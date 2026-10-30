//! `lost_changes` entries and their fitting into the 65,536 bytes of the
//! `session-status` report that carries them.

use drive::lost_changes::{BODY_MAX_BYTES, entries, fit};
use protocol::connection::{LostChange, LostChanges, SessionOutcome, SessionStatus};
use protocol::id::Id;

fn id(text: &str) -> Id {
    Id::new(text).unwrap()
}

/// The `closed` report of a close, carrying `lost_changes`.
fn closed(lost_changes: Option<&LostChanges>) -> SessionStatus {
    SessionStatus {
        connection_id: id("conn_a"),
        message_id: id("msg_a"),
        request_message_id: id("close_a"),
        session_id: id("sess_a"),
        binding_id: id("bind_a"),
        outcome: SessionOutcome::Closed {
            lost_changes: lost_changes.cloned(),
        },
    }
}

fn size(lost_changes: Option<&LostChanges>) -> usize {
    protocol::encode(&closed(lost_changes)).len()
}

fn fitted(entries: &[LostChange]) -> Vec<LostChange> {
    let fitted = fit(entries, size).expect("an entry fits");
    let body = protocol::encode(&closed(Some(&fitted)));
    assert!(body.len() <= BODY_MAX_BYTES, "{}", body.len());
    protocol::decode::<SessionStatus>(&body).expect("a valid report");
    fitted.0
}

#[test]
fn two_drives_of_100_long_paths_fit_entry_after_entry() {
    // Segments stay within 255 bytes.
    let path = |index: usize| {
        let name = format!("{index:03}/{}/", vec!["p".repeat(250); 3].join("/"));
        format!("{name}{}", "p".repeat(1_000 - name.len()))
    };
    // A later entry with a short path gets no path either.
    let abandoned = ["drive_b", "drive_a"]
        .into_iter()
        .flat_map(|drive| (0..100).map(move |index| (drive.to_owned(), path(index))))
        .chain([("drive_c".to_owned(), "c".to_owned())]);
    let entries = entries(&id("close_a"), abandoned);
    assert_eq!(entries[0].drive_id.as_str(), "drive_a");
    assert_eq!(entries[0].paths.len(), 100);

    let fitted = fitted(&entries);
    assert_eq!(fitted.len(), 3);
    assert!(fitted[2].paths.is_empty());
    let kept = fitted[0].paths.len();
    assert!(kept > 0 && kept < 100, "{kept}");
    assert_eq!(&fitted[0].paths[..], &entries[0].paths[..kept]);
    // The next path would not fit.
    let mut more = fitted.clone();
    more[0].paths.0.push(entries[0].paths[kept].clone());
    assert!(size(Some(&protocol::field::List(more))) > BODY_MAX_BYTES);
    assert!(fitted[1].paths.is_empty());
    assert_eq!(fitted[0].paths_total.get(), 100);
    assert_eq!(fitted[1].paths_total.get(), 100);
}

#[test]
fn short_entries_list_at_most_100_paths_and_fitting_leaves_them_unchanged() {
    let many = (0..101).map(|index| ("drive_c".to_owned(), format!("{index:03}")));
    let abandoned = [
        ("drive_b", "b.txt"),
        ("drive_a", "z.txt"),
        ("drive_a", "a.txt"),
    ]
    .map(|(drive, path)| (drive.to_owned(), path.to_owned()))
    .into_iter()
    .chain(many);
    let entries = entries(&id("close_a"), abandoned);
    let paths: Vec<Vec<&str>> = entries
        .iter()
        .take(2)
        .map(|entry| entry.paths.iter().map(|path| path.as_str()).collect())
        .collect();
    assert_eq!(paths, [vec!["a.txt", "z.txt"], vec!["b.txt"]]);
    assert_eq!(entries[2].paths.len(), 100);
    assert_eq!(entries[2].paths[99].as_str(), "099");
    assert_eq!(entries[2].paths_total.get(), 101);
    assert_eq!(fitted(&entries), entries);
}
