//! The shared cache owner's queue gauges advance without new owner messages.
mod support;
use protocol::id::SafeId;
use std::sync::Arc;
use std::time::Duration;
use support::cache::*;

fn metric(name: &str) -> f64 {
    observe::render()
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("missing {name}"))
        .parse()
        .unwrap()
}
#[tokio::test]
async fn waiting_downloads_and_uploads_age_until_their_slots_are_granted() {
    let root = tempfile::tempdir().unwrap();
    let stub = Arc::new(Stub::held());
    let clock = TestClock::default();
    let cache = cache(root.path(), stub.clone(), &clock, plenty());
    let session = SafeId::new("sess_metrics").unwrap();
    let mut reads = Vec::new();
    for n in 0..9 {
        let bytes = format!("content {n}");
        let hash = sha256(bytes.as_bytes());
        stub.serve(&hash, bytes.as_bytes());
        reads.push(tokio::spawn(cache.read(needed(
            session.as_str(),
            &hash,
            bytes.len(),
        ))));
    }
    let mut grants = Vec::new();
    for n in 0..9 {
        grants.push(cache.upload_slot(session.clone(), n));
    }
    for grant in grants.drain(..8) {
        grant.await.unwrap();
    }
    within("both queues hold their ninth request", || {
        metric("tetral_downloads_queued") == 1.0 && metric("tetral_uploads_queued") == 1.0
    })
    .await;
    assert_eq!(metric("tetral_downloads_in_progress"), 8.0);
    assert_eq!(metric("tetral_uploads_in_progress"), 8.0);
    clock.advance(Duration::from_secs(3));
    // No handle method posts a cache message during this observation.
    within("both queue ages advance", || {
        metric("tetral_downloads_oldest_queued_seconds") == 3.0
            && metric("tetral_uploads_oldest_queued_seconds") == 3.0
    })
    .await;
    stub.permits.as_ref().unwrap().add_permits(1);
    cache.upload_slot_free(session.clone(), 0);
    grants.pop().unwrap().await.unwrap();
    within("both final grants clear their queue age", || {
        metric("tetral_downloads_queued") == 0.0 && metric("tetral_uploads_queued") == 0.0
    })
    .await;
    for family in ["tetral_downloads", "tetral_uploads"] {
        assert_eq!(metric(&format!("{family}_oldest_queued_seconds")), 0.0);
        assert_eq!(metric(&format!("{family}_queue_wait_seconds_sum")), 3.0);
        assert_eq!(metric(&format!("{family}_queue_wait_seconds_count")), 9.0);
    }
    stub.permits.as_ref().unwrap().add_permits(8);
    for read in reads {
        read.await.unwrap().unwrap();
    }
    for n in 1..9 {
        cache.upload_slot_free(session.clone(), n);
    }
    within("all capacity is released", || {
        metric("tetral_downloads_in_progress") == 0.0 && metric("tetral_uploads_in_progress") == 0.0
    })
    .await;
}
