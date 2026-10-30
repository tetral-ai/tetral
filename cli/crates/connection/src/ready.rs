//! The upstream messages that hold the connection: ready, built for each new
//! connection, and the check that a message passes its own decoder before
//! its first send. Also the grant an ACK of either gives.

use std::time::Duration;

use observe::{STARTUP, StartupPhases};
use protocol::connection::{
    Arch, Capabilities, GrantAcknowledgement, LostChange, Os, Ready, STARTUP_PHASE_MAX_MS, SemVer,
    Startup, StartupPhaseMs,
};
use protocol::id::Id;
use protocol::tool::ProtocolVersion;
use protocol::{Message, decode, encode};

#[cfg(target_os = "linux")]
const OS: Os = Os::Linux;
#[cfg(target_os = "macos")]
const OS: Os = Os::Macos;

#[cfg(target_arch = "x86_64")]
const ARCH: Arch = Arch::X86_64;
#[cfg(target_arch = "aarch64")]
const ARCH: Arch = Arch::Aarch64;

/// What every ready of this App Server shares.
#[derive(Debug, Clone)]
pub struct ReadyFields {
    pub cli_version: SemVer,
    pub capabilities: Capabilities,
}

/// The ready of `connection` first sent at `first_send`. It carries the
/// startup phases measured to that send until a ready of this process is
/// accepted, and the kept `lost_changes` entries, fitted with its other
/// fields.
pub fn ready(
    fields: &ReadyFields,
    connection: &Id,
    message_id: Id,
    first_send: Duration,
    kept: &[LostChange],
) -> Ready {
    let startup = STARTUP
        .accepted_phases()
        .is_none()
        .then(|| startup(STARTUP.phases(first_send)));
    let mut ready = Ready {
        protocol_version: ProtocolVersion::default(),
        connection_id: connection.clone(),
        message_id,
        os: OS,
        arch: ARCH,
        cli_version: fields.cli_version.clone(),
        capabilities: fields.capabilities.clone(),
        lost_changes: None,
        startup,
    };
    if !kept.is_empty() {
        ready.lost_changes = drive::lost_changes::fit(kept, |lost_changes| {
            encode(&Ready {
                lost_changes: lost_changes.cloned(),
                ..ready.clone()
            })
            .len()
        });
    }
    ready
}

fn startup(phases: StartupPhases) -> Startup {
    let phase = |ms: observe::Ms| {
        let held = i64::try_from(ms.as_u64())
            .unwrap_or(STARTUP_PHASE_MAX_MS)
            .min(STARTUP_PHASE_MAX_MS);
        StartupPhaseMs::new(held).expect("a phase is held to its bounds")
    };
    Startup {
        config_ms: phase(phases.config),
        connect_ms: phase(phases.connect),
        ready_ms: phase(phases.ready),
    }
}

/// The message's bytes once they pass its own decoder; otherwise records
/// the failure and gives nothing to send.
pub fn checked<M: Message>(message: &M, connection: &Id, message_id: &Id) -> Option<Vec<u8>> {
    let body = encode(message);
    if decode::<M>(&body).is_ok() {
        return Some(body);
    }
    observe::error!(
        "protocol.sender_check_failed",
        connection.id = observe::Id(connection.as_str()),
        message.id = observe::Id(message_id.as_str()),
        error.code = observe::Code("sender_check_failed")
    );
    None
}

/// The connection deadline that an ACK grants, when the ACK is valid for the
/// message `message_id` of `connection` first sent at `first_send`: it
/// passes its schema, names that message and yields a deadline that `now`
/// has not reached.
pub fn grant(
    body: &[u8],
    connection: &Id,
    message_id: &Id,
    first_send: Duration,
    now: Duration,
) -> Option<Duration> {
    let ack: GrantAcknowledgement = decode(body).ok()?;
    let valid_for = Duration::from_millis(ack.valid_for_ms.get().unsigned_abs());
    let deadline = first_send + valid_for;
    (ack.connection_id == *connection && ack.message_id == *message_id && deadline > now)
        .then_some(deadline)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(text: &str) -> Id {
        Id::new(text).unwrap()
    }

    fn ack(connection: &str, message: &str, valid_for_ms: i64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "connection_id": connection,
            "message_id": message,
            "accepted": true,
            "valid_for_ms": valid_for_ms,
        }))
        .unwrap()
    }

    #[test]
    fn an_ack_grants_from_the_first_send_only_for_its_message_and_before_the_deadline() {
        let connection = id("conn_a");
        let message = id("c1");
        let sent = Duration::from_secs(100);
        let grant_at = |body: &[u8], now: u64| {
            grant(body, &connection, &message, sent, Duration::from_secs(now))
        };
        assert_eq!(
            grant_at(&ack("conn_a", "c1", 45_000), 130),
            Some(Duration::from_secs(145))
        );
        assert_eq!(grant_at(&ack("conn_a", "c1", 45_000), 145), None);
        assert_eq!(grant_at(&ack("conn_b", "c1", 45_000), 101), None);
        assert_eq!(grant_at(&ack("conn_a", "c2", 45_000), 101), None);
        assert_eq!(grant_at(&ack("conn_a", "c1", 45_001), 101), None);
        assert_eq!(grant_at(&ack("conn_a", "c1", 0), 100), None);
    }

    #[test]
    fn a_ready_carries_the_kept_entries_that_fit_with_its_other_fields() {
        // The short fourth entry is left out with the third.
        let kept: Vec<LostChange> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|request| {
                let repeat = if request == "d" { 1 } else { 30_000 };
                serde_json::from_value(serde_json::json!({
                    "request_message_id": request.repeat(repeat),
                    "drive_id": "drive_a",
                    "paths": ["f.txt"],
                    "paths_total": 1,
                }))
                .unwrap()
            })
            .collect();
        let fields = ReadyFields {
            cli_version: serde_json::from_value(serde_json::json!("0.1.0")).unwrap(),
            capabilities: serde_json::from_value(
                serde_json::json!({"session_preparation": true, "drives": {"mount": false}}),
            )
            .unwrap(),
        };
        let built = ready(&fields, &id("conn_a"), id("c1"), Duration::ZERO, &kept);
        let body = checked(&built, &id("conn_a"), &id("c1")).expect("a valid ready");
        assert!(body.len() <= 65_536, "{}", body.len());
        let lost_changes = built.lost_changes.expect("kept entries");
        assert_eq!(&lost_changes[..], &kept[..2]);
    }
}
