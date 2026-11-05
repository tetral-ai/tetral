//! A real HTTP DriveTransfer and DriveSync publish pressure through the
//! App Server's actual metrics route. The peer is an HTTP fixture, not Engine.
use super::*;
use crate::test_support::{CONTENT, Fixture, HASH, id, within};
use axum::extract::{Request, State as HttpState};
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

#[derive(Clone)]
struct Peer {
    attempts: Arc<AtomicU64>,
    committed: Arc<tokio::sync::Notify>,
}
async fn answer(HttpState(peer): HttpState<Peer>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    match request.method().as_str() {
        "GET" if path.ends_with("/files") => {
            Json(json!({"files":[],"has_more":false,"start_seq":1})).into_response()
        }
        "GET" if path.ends_with("/changes") => {
            Json(json!({"records":[],"has_more":false})).into_response()
        }
        "PUT" => {
            assert!(path.ends_with(HASH));
            let body = axum::body::to_bytes(request.into_body(), 1024)
                .await
                .unwrap();
            assert_eq!(&body[..], CONTENT);
            if peer.attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "2")],
                    Json(json!({"code":"rate_limited","message":"retry later"})),
                )
                    .into_response()
            } else {
                Json(json!({"content_sha256":HASH,"size_bytes":CONTENT.len()})).into_response()
            }
        }
        "POST" if path.ends_with("/commits") => {
            peer.committed.notify_one();
            Json(json!({"seq":2,"results":[{"status":"accepted","version_id":"version_new"}]}))
                .into_response()
        }
        _ => panic!("unexpected fixture request {path}"),
    }
}
async fn scrape(path: &std::path::Path) -> String {
    let mut socket = tokio::net::UnixStream::connect(path).await.unwrap();
    socket
        .write_all(b"GET /v1/metrics HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = Vec::new();
    within(socket.read_to_end(&mut bytes)).await.unwrap();
    let response = String::from_utf8(bytes).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    response
}
fn rejected(text: &str) -> u64 {
    text.lines()
        .find_map(|line| {
            line.strip_prefix("tetral_uploads_rejected_total{cause=\"rate_limited\"} ")
        })
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn repeated_rate_limited_http_uploads_share_one_episode_and_count_every_rejection() {
    use tracing_subscriber::layer::SubscriberExt;
    let log = tempfile::NamedTempFile::new().unwrap();
    let sinks = Arc::new(observe::Sinks::new(
        observe::Sink::spawn_stderr(Box::new(log.reopen().unwrap()), observe::registry()),
        observe::FlushClock {
            now: state::seams::now,
            recheck: Duration::from_millis(1),
        },
    ));
    let _records = tracing::subscriber::set_default(tracing_subscriber::registry().with(
        observe::RecordLayer::new(observe::Level::Warn, sinks.clone()),
    ));
    let episodes = || {
        sinks.flush(Duration::from_secs(2));
        std::fs::read_to_string(log.path())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|record| record["pressure.point"] == "tetral_uploads")
            .collect::<Vec<_>>()
    };
    let mut fixture = Fixture::new();
    let peer = Peer {
        attempts: Arc::default(),
        committed: Arc::new(tokio::sync::Notify::new()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let remote = tokio::spawn(
        axum::serve(
            listener,
            Router::new().fallback(answer).with_state(peer.clone()),
        )
        .into_future(),
    );
    let key=protocol::bootstrap::Bootstrap::decode(br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_idle","credential_id":"cred_idle","connection_key":"tetral_ck_idle","expires_at":"2099-01-01T00:00:00Z"}"#).unwrap().connection_key;
    let (link, links) = connection::drive_transfer::DriveLink::new(key.clone());
    link.connection_started(id("conn_idle"), key);
    let (events, _answers) = mpsc::unbounded_channel();
    fixture.transfer = Arc::new(connection::drive_transfer::HttpDriveTransfer::new(
        &url,
        links,
        events,
        fixture.clock.clone(),
    ));
    let sync = fixture.session("sess_rate_limit").await;
    let socket_path = fixture.root.path().join("metrics.sock");
    let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
    let (lifecycle, _inbox) = crate::lifecycle::channel();
    let local = tokio::spawn(
        axum::serve(
            listener,
            router(Sources {
                lifecycle,
                connection: fixture.connection.clone(),
                leftovers: execution::cleanup::Leftovers::none(),
                fixed: FixedStatus {
                    instance_id: id("inst_idle"),
                    cli_version: env!("CARGO_PKG_VERSION").into(),
                },
                mcp: mcp::spawn(mcp::ManagerConfig {
                    root: state::paths::StateRoot::new(fixture.root.path().as_os_str()).unwrap(),
                    cli_version: env!("CARGO_PKG_VERSION").into(),
                    limits: Default::default(),
                    activity: std::sync::Arc::new(protocol::ports::NoActivity),
                }),
            }),
        )
        .into_future(),
    );
    let before = rejected(&scrape(&socket_path).await);
    let mut upload = sync.final_upload(drive::FinalUploadRequest {
        session_close: false,
        drives: vec![id("drive_idle")],
        end: Duration::from_secs(30),
        mode: drive::FinalMode::Keep,
    });
    within(async {
        while rejected(&scrape(&socket_path).await) != before + 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(peer.attempts.load(Ordering::SeqCst), 1);
    fixture.now.store(2, Ordering::SeqCst);
    within(async {
        while rejected(&scrape(&socket_path).await) != before + 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(peer.attempts.load(Ordering::SeqCst), 2);
    let records = episodes();
    assert_eq!(
        records.len(),
        1,
        "repeated rejections share one episode: {records:?}"
    );
    assert_eq!(records[0]["event"], "pressure.rejecting_started");
    fixture.now.store(4, Ordering::SeqCst);
    within(peer.committed.notified()).await;
    within(upload.ended()).await;
    assert_eq!(peer.attempts.load(Ordering::SeqCst), 3);
    assert_eq!(rejected(&scrape(&socket_path).await), before + 2);
    assert!(sync.tables().await.pending_changes.is_empty());
    let records = episodes();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["event"], "pressure.rejecting_started");
    assert_eq!(records[0]["pressure.cause"], "rate_limited");

    // A real upload admission after the quiet interval closes the episode.
    // The HTTP retry above retained its grant and did not itself end it.
    fixture.now.store(62, Ordering::SeqCst);
    let next = fixture.session("sess_after_quiet").await;
    let mut upload = next.final_upload(drive::FinalUploadRequest {
        session_close: false,
        drives: vec![id("drive_idle")],
        end: Duration::from_secs(90),
        mode: drive::FinalMode::Keep,
    });
    within(upload.ended()).await;
    assert!(next.tables().await.pending_changes.is_empty());
    assert_eq!(peer.attempts.load(Ordering::SeqCst), 4);
    assert_eq!(rejected(&scrape(&socket_path).await), before + 2);
    let records = episodes();
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(records[1]["event"], "pressure.rejecting_ended");
    assert_eq!(records[1]["pressure.cause"], "rate_limited");
    assert_eq!(records[1]["pressure.rejected"], 2);
    local.abort();
    remote.abort();
}
