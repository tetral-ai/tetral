//! Local download failures identify their actual Session, Drive and binding.
mod support;
use protocol::id::SafeId;
use std::fs;
use std::sync::Arc;
use support::cache::*;
#[tokio::test]
async fn local_download_failures_are_recorded_once_with_the_request_identities() {
    // Where the local file system fails: the download's temporary file, a
    // write into it, its store into the cache, or the open of a blob already
    // in the cache.
    for case in ["temporary", "write", "store", "read"] {
        let records = support::Records::start();
        let root = tempfile::tempdir().unwrap();
        let stub = Arc::new(Stub::held());
        let hash = sha256(b"content");
        stub.serve(&hash, b"content");
        let cache = cache(root.path(), stub.clone(), &TestClock::default(), plenty());
        let mut second = None;
        let read = if case == "store" {
            let read = tokio::spawn(cache.read(needed("sess_download", &hash, 7)));
            within("download reached the HTTP port", || {
                stub.started().len() == 1
            })
            .await;
            second = Some(cache.read(needed("sess_download", &hash, 7)));
            cache
                .downloading(&SafeId::new("sess_download").unwrap())
                .await;
            fs::remove_dir(root.path().join("cache/blobs")).unwrap();
            fs::write(root.path().join("cache/blobs"), b"unavailable directory").unwrap();
            stub.permits.as_ref().unwrap().add_permits(1);
            read
        } else if case == "write" {
            let read = tokio::spawn(cache.read(needed("sess_download", &hash, 7)));
            within("download reached the HTTP port", || {
                stub.started().len() == 1
            })
            .await;
            // The stub's write reopens the temporary file by its path.
            let temporary = fs::read_dir(root.path().join("cache/tmp"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            fs::remove_file(&temporary).unwrap();
            fs::create_dir(&temporary).unwrap();
            stub.permits.as_ref().unwrap().add_permits(1);
            read
        } else {
            let unavailable = if case == "read" {
                "cache/blobs"
            } else {
                "cache/tmp"
            };
            fs::remove_dir(root.path().join(unavailable)).unwrap();
            fs::write(root.path().join(unavailable), b"unavailable directory").unwrap();
            tokio::spawn(cache.read(needed("sess_download", &hash, 7)))
        };
        let (code, errno) = if case == "read" {
            ("content_read_failed", 20)
        } else {
            ("content_write_failed", 5)
        };
        let message = if case == "write" {
            "Is a directory (os error 21)"
        } else {
            "Not a directory (os error 20)"
        };
        assert_eq!(read.await.unwrap().unwrap_err().raw_os_error(), Some(errno));
        if let Some(second) = second {
            assert_eq!(second.await.unwrap_err().raw_os_error(), Some(5));
        }
        // An owner barrier ensures its record has been emitted after its reply.
        cache
            .downloading(&SafeId::new("sess_download").unwrap())
            .await;
        let failures: Vec<_> = records
            .lines()
            .into_iter()
            .filter(|line| line["event"] == "drive.download_failed")
            .collect();
        assert_eq!(failures.len(), 1, "{failures:#?}");
        for (field, value) in [
            ("session.id", "sess_download"),
            ("drive.id", "drive_a"),
            ("connection.id", "conn_a"),
            ("binding.id", "bind_a"),
            ("error.code", code),
            ("error.message", message),
            ("level", "WARN"),
        ] {
            assert_eq!(failures[0][field], value, "{field}: {:?}", failures[0]);
        }
        assert!(failures[0].get("trace_id").is_none());
    }
}
