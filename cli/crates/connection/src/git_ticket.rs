//! The git-ticket fetch that a Session binding owner asks for before it
//! clones a preparation's repositories (`FetchGitTicket`): one ordinary
//! request of the held connection, sent again with its `message_id` after a
//! network failure, a timeout or a `5xx`. Engine issues a new ticket at
//! every fetch, a retry included, so no answer is replayed.

use std::time::Duration;

use protocol::connection::{GitTicketRequest, GitTicketResponse};
use protocol::id::{Id, SafeId};
use protocol::ports::{TicketAnswer, TicketRequest};
use protocol::tool::ProtocolVersion;

use crate::backoff::Backoff;
use crate::http::Answer;
use crate::ready;

pub(crate) const GIT_TICKET_PATH: &str = "/v1/client/git-ticket";

/// The message of a `2xx` answer that fails its schema, which counts as a
/// failed fetch.
const INVALID_ANSWER: &str = "the git ticket answer does not match its schema";

/// One fetch of the held connection, until its answer.
pub(crate) struct TicketFetch {
    pub session_id: SafeId,
    pub request: TicketRequest,
    pub body: Vec<u8>,
    pub backoff: Backoff,
    pub retry_at: Option<Duration>,
}

/// The fetch's body for `binding_id` on `connection`, once it passes its
/// own decoder.
pub(crate) fn request(connection: &Id, message_id: &Id, binding_id: &Id) -> Option<Vec<u8>> {
    let request = GitTicketRequest {
        protocol_version: ProtocolVersion::default(),
        connection_id: connection.clone(),
        message_id: message_id.clone(),
        binding_id: binding_id.clone(),
    };
    ready::checked(&request, connection, message_id)
}

/// The answer the Session binding owner gets, or `None` for an answer that
/// is retried. A refusal that ends the connection is the Connection owner's.
pub(crate) fn answer(answer: Answer) -> Option<TicketAnswer> {
    match answer {
        Answer::Success(body) => Some(
            protocol::decode::<GitTicketResponse>(&body).map_err(|_| INVALID_ANSWER.to_owned()),
        ),
        Answer::Refused(error) => Some(Err(error.message)),
        Answer::Failed(_) => None,
    }
}
