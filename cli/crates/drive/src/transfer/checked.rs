//! [`Checked`], the wrapper every test's stand-in for the drive transfer
//! goes through. The crate compiles it for its unit tests and under the
//! feature `test-seams`; its integration tests include this file, since a
//! package cannot turn on a feature of its own for them.

use std::sync::Arc;

use drive::transfer::{
    Binding, BindingWatch, Commit, ContentSink, Download, DriveTransfer, Ended, ListChanges,
    ListFiles, Request, Transfer, Upload, answer,
};
use protocol::drive_transfer::{
    CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::id::Id;

/// A stand-in for the drive transfer held to what the HTTP implementation
/// can give: each `2xx` answer of a listing, an upload or a commit passes
/// [`answer`], and a request ends with an error only as its endpoint's error
/// answers end it. It panics otherwise, so that no test runs against an
/// answer Engine cannot give. A download's bytes are the caller's to check,
/// as they are behind the HTTP implementation.
pub struct Checked<T: ?Sized>(pub Arc<T>);

/// Panics when the HTTP implementation ends no request of `endpoint` as
/// `ended`: an error body passes the endpoint's decoder, each end that one
/// error code gives comes from an endpoint that sends the code, a permanent
/// end's code comes with its status and is one the client does not act on,
/// and no permanent end is a status the client tries again.
fn standin_ended(endpoint: protocol::errors::DriveTransferEndpoint, ended: Ended) -> Ended {
    use protocol::errors::ErrorCode;
    let sends = |code: ErrorCode| endpoint.spec().errors.contains(&code);
    match &ended {
        Ended::Decision(body) => {
            protocol::errors::decode_drive_transfer_error(
                endpoint,
                body.code.status(),
                &protocol::encode(body),
            )
            .expect("a stand-in's error answer follows the protocol");
            assert!(
                matches!(
                    body.code,
                    ErrorCode::ContentMissing | ErrorCode::FileTooLarge
                ),
                "a stand-in's {:?} is no decision",
                body.code
            );
        }
        Ended::ReRead => assert!(
            sends(ErrorCode::ContentHashMismatch),
            "a stand-in's {endpoint:?} asks for a re-read"
        ),
        Ended::CursorExpired => assert!(
            sends(ErrorCode::DriveCursorExpired),
            "a stand-in's {endpoint:?} answers an expired cursor"
        ),
        Ended::Unreadable(_) => assert!(
            endpoint == protocol::errors::DriveTransferEndpoint::UploadContent,
            "a stand-in's {endpoint:?} has a file to open"
        ),
        Ended::Permanent { status, code } => {
            let sent = endpoint
                .spec()
                .errors
                .iter()
                .find(|sent| sent.as_str() == *code)
                .unwrap_or_else(|| panic!("a stand-in's {endpoint:?} never sends {code}"));
            assert_eq!(
                *status,
                sent.status(),
                "a stand-in's {code} comes with its own status"
            );
            assert!(
                !matches!(
                    sent,
                    ErrorCode::ContentHashMismatch
                        | ErrorCode::DriveCursorExpired
                        | ErrorCode::ContentMissing
                        | ErrorCode::FileTooLarge
                        | ErrorCode::StaleConnection
                        | ErrorCode::StaleBinding
                        | ErrorCode::InstanceUnavailable
                ) && sent.status() != 401,
                "a stand-in's {code} does not end a request permanently"
            );
        }
        Ended::Invalid { status, .. } => assert!(
            !matches!(status, 500 | 502 | 503 | 504),
            "a stand-in's {endpoint:?} ends permanently on {status}, which is retried"
        ),
        Ended::Rebind(_) | Ended::Sink(_) => {}
    }
    ended
}

impl<T: DriveTransfer + ?Sized> DriveTransfer for Checked<T> {
    fn list_files(&self, request: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let (held, body) = (standin_binding(&request.binding), request.body.clone());
        let answered = self.0.list_files(request);
        Box::pin(async move {
            let page = answered.await.map_err(|ended| {
                standin_ended(protocol::errors::DriveTransferEndpoint::ListFiles, ended)
            })?;
            Ok(answer::list_files(&held, &body, &protocol::encode(&page))
                .expect("a stand-in's list files answer follows the protocol"))
        })
    }

    fn list_changes(&self, request: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        let (held, body) = (standin_binding(&request.binding), request.body.clone());
        let answered = self.0.list_changes(request);
        Box::pin(async move {
            let page = answered.await.map_err(|ended| {
                standin_ended(protocol::errors::DriveTransferEndpoint::ListChanges, ended)
            })?;
            Ok(answer::list_changes(&held, &body, &protocol::encode(&page))
                .expect("a stand-in is asked with a cursor that a query carries")
                .expect("a stand-in's list changes answer follows the protocol"))
        })
    }

    fn upload_content(&self, request: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        let (drive_id, sha256, size) = (
            request.drive_id.clone(),
            request.body.sha256.clone(),
            request.body.content.size(),
        );
        let answered = self.0.upload_content(request);
        Box::pin(async move {
            let stored = answered.await.map_err(|ended| {
                standin_ended(
                    protocol::errors::DriveTransferEndpoint::UploadContent,
                    ended,
                )
            })?;
            Ok(
                answer::upload(&drive_id, &sha256, size, &protocol::encode(&stored))
                    .expect("a stand-in's upload answer follows the protocol"),
            )
        })
    }

    fn commit(&self, request: Request<Commit>) -> Transfer<'_, CommitResponse> {
        let sent =
            answer::commit_request(&standin_binding(&request.binding), &request.body.changes);
        let answered = self.0.commit(request);
        Box::pin(async move {
            let results = answered.await.map_err(|ended| {
                standin_ended(protocol::errors::DriveTransferEndpoint::Commit, ended)
            })?;
            Ok(answer::commit(&sent, &protocol::encode(&results))
                .expect("a stand-in's commit answer follows the protocol"))
        })
    }

    fn download_content<'a>(
        &'a self,
        request: Request<Download>,
        sink: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        let answered = self.0.download_content(request, sink);
        Box::pin(async move {
            answered.await.map_err(|ended| {
                standin_ended(
                    protocol::errors::DriveTransferEndpoint::DownloadContent,
                    ended,
                )
            })
        })
    }
}

/// The binding a stand-in's request holds; the checks read a request's own
/// parameters, so a request without one is checked under a binding that
/// names nothing.
fn standin_binding(binding: &BindingWatch) -> Binding {
    binding.borrow().clone().unwrap_or_else(|| Binding {
        connection_id: Id::new("conn_standin").expect("an identity"),
        binding_id: Id::new("bind_standin").expect("an identity"),
    })
}
