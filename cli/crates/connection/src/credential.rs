//! Credential renewal: the fetch of the pending credential that
//! `credential.renew` names, the check of its answer and its replacement of
//! `credentials.json`. The Connection owner runs a renewal through these
//! steps (`owner`) and then resumes its stream with the new credential,
//! which every request carries once Engine confirms the resume.

use std::time::Duration;

use protocol::bootstrap::{Bootstrap, BootstrapKey, ServerUrl};
use protocol::connection::{CredentialRequest, CredentialResponse};
use protocol::id::Id;
use protocol::tool::ProtocolVersion;
use state::files::IoFailure;
use state::paths::StateRoot;

use crate::backoff::Backoff;
use crate::owner::Credential;
use crate::ready;

pub(crate) const CREDENTIAL_PATH: &str = "/v1/client/credential";

/// The code recorded for a `2xx` answer that fails its schema or names
/// another credential, which counts as no credential.
pub(crate) const INVALID_RESPONSE: &str = "invalid_response";

/// Where the held connection's renewal stands.
pub(crate) enum Renewal {
    /// The fetch of `credential_id`, whose body, `message_id` included, is
    /// sent again after a network failure, a timeout or a `5xx`.
    Fetching {
        credential_id: Id,
        body: Vec<u8>,
        backoff: Backoff,
        retry_at: Option<Duration>,
    },
    /// The fetched credential being written by the task tagged `tag`.
    Persisting { credential: Credential, tag: u64 },
    /// Resume attempts with the written credential, from the first until
    /// `connected` or the connection's end.
    Switching { credential: Credential },
}

impl Renewal {
    /// The credential this renewal is about.
    pub(crate) fn credential_id(&self) -> &Id {
        match self {
            Renewal::Fetching { credential_id, .. } => credential_id,
            Renewal::Persisting { credential, .. } | Renewal::Switching { credential } => {
                &credential.credential_id
            }
        }
    }
}

/// The fetch's body for `credential_id` on `connection`, once it passes its
/// own decoder.
pub(crate) fn request(connection: &Id, message_id: &Id, credential_id: &Id) -> Option<Vec<u8>> {
    let request = CredentialRequest {
        protocol_version: ProtocolVersion::default(),
        connection_id: connection.clone(),
        message_id: message_id.clone(),
        credential_id: credential_id.clone(),
    };
    ready::checked(&request, connection, message_id)
}

/// The credential of a `2xx` answer, when it passes the schema, its key
/// with the `tetral_ck_` prefix included, and names `requested`.
pub(crate) fn fetched(body: &[u8], requested: &Id) -> Option<Credential> {
    let response: CredentialResponse = protocol::decode(body).ok()?;
    (response.credential_id == *requested).then(|| Credential {
        credential_id: response.credential_id,
        key: BootstrapKey::from(&response.connection_key),
        expires_at: response.expires_at,
    })
}

/// Records a fetch that is never sent again; the current credential stays.
pub(crate) fn rejected(credential_id: &Id, code: &'static str) {
    observe::warn!(
        "credential.renewal_rejected",
        credential.id = observe::Id(credential_id.as_str()),
        error.code = observe::Code(code)
    );
}

/// Replaces `credentials.json` with `credential` and the bootstrap's
/// `server_url` and `instance_id`. A failure is recorded; once it comes
/// after the rename, the file already holds the new credential.
pub(crate) fn persist(
    root: &StateRoot,
    server_url: &str,
    instance_id: &Id,
    credential: &Credential,
) -> Result<(), IoFailure> {
    let bootstrap = Bootstrap {
        server_url: ServerUrl::new(server_url).expect("the server URL came from the bootstrap"),
        instance_id: instance_id.clone(),
        credential_id: credential.credential_id.clone(),
        connection_key: credential.key.clone(),
        expires_at: credential.expires_at.clone(),
    };
    state::credentials::replace(root, &bootstrap.encode()).inspect_err(|_| {
        observe::error!(
            "credential.renewal_not_saved",
            credential.id = observe::Id(credential.credential_id.as_str()),
            error.code = observe::Code("credential_not_saved")
        );
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(text: &str) -> Id {
        Id::new(text).unwrap()
    }

    #[test]
    fn only_an_answer_that_passes_the_schema_and_names_the_requested_credential_is_taken() {
        let answer = |credential: &str, key: &str| {
            serde_json::to_vec(&serde_json::json!({
                "credential_id": credential,
                "connection_key": key,
                "expires_at": "2099-01-01T00:00:00Z",
            }))
            .unwrap()
        };
        let taken = fetched(&answer("cred_b", "tetral_ck_b"), &id("cred_b")).unwrap();
        assert_eq!(
            (taken.credential_id.as_str(), taken.key.expose()),
            ("cred_b", "tetral_ck_b")
        );
        assert!(fetched(&answer("cred_c", "tetral_ck_b"), &id("cred_b")).is_none());
        assert!(fetched(&answer("cred_b", "ck_b"), &id("cred_b")).is_none());
        assert!(fetched(b"{}", &id("cred_b")).is_none());
    }
}
