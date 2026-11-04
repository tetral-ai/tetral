package computerhub

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// The drive transfer routes serve a binding's drives to its CLI. After the
// principal is verified at the edge and authorize passes, each request runs
// these checks in order, and the first failing one answers:
//
//  1. shape: path parameters, query, headers and a commit's body
//     (400 invalid_message; 416 range_not_satisfiable for more than one
//     range);
//  2. connection_id is the instance's live connection (409 stale_connection);
//  3. binding_id names a binding of that connection whose prepared report is
//     stored and whose closed report is not (409 stale_binding);
//  4. drive_id is in the binding's drive set, read_write for an upload and a
//     commit (403 drive_access_denied);
//  5. a commit's change count (413 request_too_large); an upload's
//     Content-Length (400 invalid_message, 413 file_too_large), then the
//     Workspace's upload budget (429 rate_limited with Retry-After).
//
// A commit's body is read first through a 16 MiB bound, and a longer one
// answers 413 request_too_large once authorize passes, before check 1.
// authorize and checks 1 to 4 run in one transaction (checkTransfer); the
// read or write that follows runs in internal/drive's own transactions, and
// no total write deadline bounds a listing or a download.
const (
	routeDriveFiles   = "/v1/client/drives/{drive_id}/files"
	routeDriveChanges = "/v1/client/drives/{drive_id}/changes"
	routeDriveContent = "/v1/client/drives/{drive_id}/content/{sha256}"
	routeDriveCommits = "/v1/client/drives/{drive_id}/commits"
)

// transferRequest is what checkTransfer checks: the request's drive,
// connection and binding, whether it writes, and its shape failure, or the
// 413 of a commit body past its bound, which it answers once authorize
// passes.
type transferRequest struct {
	driveID      string
	connectionID string
	bindingID    string
	write        bool
	refusal      error
}

// checkTransfer authorizes the request and runs checks 1 to 4, and returns
// the binding it names.
func (h *clientHandler) checkTransfer(ctx context.Context, id clientIdentity, request transferRequest) (computer.Binding, error) {
	var binding computer.Binding
	err := h.clientTx(ctx, id.workspace, "computer_hub.drive_transfer_check", func(tx *dbconnect.Tx) error {
		granted, err := authorize(ctx, tx, id, false, false, "")
		if err != nil {
			return err
		}
		if request.refusal != nil {
			return request.refusal
		}
		connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, request.connectionID, granted.now, false)
		if err != nil {
			return err
		}
		var found bool
		if binding, found, err = computer.TransferBinding(ctx, tx, id.workspace, connection.ID, request.bindingID); err != nil {
			return err
		}
		if !found {
			return refuse(protocol.ErrorStaleBinding, "binding_id names no prepared or closing binding of this connection")
		}
		drives, err := drive.BindingDriveSet(ctx, tx, id.workspace, binding.BindingID)
		if err != nil {
			return err
		}
		for _, bound := range drives {
			if bound.DriveID == request.driveID && (!request.write || bound.Access == drive.AccessReadWrite) {
				return nil
			}
		}
		if request.write {
			return refuse(protocol.ErrorDriveAccessDenied, "the binding's drive set does not give read_write access to this drive")
		}
		return refuse(protocol.ErrorDriveAccessDenied, "the binding's drive set does not include this drive")
	})
	return binding, err
}

// firstFailure is the first non-nil error of errs.
func firstFailure(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// writeTransferFailure answers a failure of the drive work after the checks:
// a drive deleted meanwhile as 403 drive_access_denied, an expired cursor as
// 410 drive_cursor_expired, a missing content as 404 content_not_found, and
// anything else as writeClientFailure does.
func writeTransferFailure(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, drive.ErrDriveNotFound):
		writeClientAnswer(w, errorAnswer(protocol.ErrorDriveAccessDenied, "the drive no longer exists"))
	case errors.Is(err, drive.ErrCursorExpired):
		writeClientAnswer(w, errorAnswer(protocol.ErrorDriveCursorExpired, "the cursor is older than the drive's oldest listed change"))
	case errors.Is(err, drive.ErrNotFound):
		writeClientAnswer(w, errorAnswer(protocol.ErrorContentNotFound, "no kept version of this drive names this content"))
	default:
		writeClientFailure(w, r, err)
	}
}

// listDriveFiles answers GET /v1/client/drives/{drive_id}/files.
func (h *clientHandler) listDriveFiles(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	params, paramsErr := protocol.DecodeDrivePathParams(map[string]string{"drive_id": chi.URLParam(r, "drive_id")})
	query, queryErr := protocol.DecodeListFilesQuery(r.URL.Query())
	if _, err := h.checkTransfer(ctx, id, transferRequest{driveID: params.DriveID, connectionID: query.ConnectionID, bindingID: query.BindingID,
		refusal: firstFailure(paramsErr, queryErr)}); err != nil {
		writeClientFailure(w, r, err)
		return
	}
	page, err := h.Drives.TransferFiles(ctx, id.workspace, params.DriveID, query.AfterPath)
	if err != nil {
		writeTransferFailure(w, r, err)
		return
	}
	writeClientAnswer(w, clientAnswer{status: http.StatusOK, body: protocol.Encode(page)})
}

// listDriveChanges answers GET /v1/client/drives/{drive_id}/changes.
func (h *clientHandler) listDriveChanges(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	params, paramsErr := protocol.DecodeDrivePathParams(map[string]string{"drive_id": chi.URLParam(r, "drive_id")})
	query, queryErr := protocol.DecodeListChangesQuery(r.URL.Query())
	if _, err := h.checkTransfer(ctx, id, transferRequest{driveID: params.DriveID, connectionID: query.ConnectionID, bindingID: query.BindingID,
		refusal: firstFailure(paramsErr, queryErr)}); err != nil {
		writeClientFailure(w, r, err)
		return
	}
	page, err := h.Drives.TransferChanges(ctx, id.workspace, params.DriveID, query.AfterSeq, query.AfterIndex)
	if err != nil {
		writeTransferFailure(w, r, err)
		return
	}
	writeClientAnswer(w, clientAnswer{status: http.StatusOK, body: protocol.Encode(page)})
}

// contentParams decodes the path parameters of a content route.
func contentParams(r *http.Request) (protocol.ContentPathParams, error) {
	return protocol.DecodeContentPathParams(map[string]string{"drive_id": chi.URLParam(r, "drive_id"), "sha256": chi.URLParam(r, "sha256")})
}

// uploadDriveContent answers PUT /v1/client/drives/{drive_id}/content/{sha256}:
// after the checks and the budget it streams the body into the drive, which
// keeps it only when its SHA-256 is the URL's.
func (h *clientHandler) uploadDriveContent(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	params, paramsErr := contentParams(r)
	query, queryErr := protocol.DecodeCommonQuery(r.URL.Query())
	// The server moves Content-Length out of the header map.
	header := r.Header.Clone()
	if r.ContentLength >= 0 {
		header.Set("Content-Length", strconv.FormatInt(r.ContentLength, 10))
	}
	headers, headersErr := protocol.DecodeUploadContentRequestHeaders(header)
	if _, err := h.checkTransfer(ctx, id, transferRequest{driveID: params.DriveID, connectionID: query.ConnectionID, bindingID: query.BindingID, write: true,
		refusal: firstFailure(paramsErr, queryErr, headersErr)}); err != nil {
		writeClientFailure(w, r, err)
		return
	}
	if err := protocol.CheckUploadLength(headers); err != nil {
		writeClientFailure(w, r, err)
		return
	}
	if err := h.Uploads.Take(ctx, id.workspace, headers.ContentLength.Value); err != nil {
		var refused *drive.UploadRefusedError
		if !errors.As(err, &refused) {
			writeClientFailure(w, r, err)
			return
		}
		w.Header().Set("Retry-After", strconv.Itoa(refused.RetryAfterSeconds))
		writeClientAnswer(w, errorAnswer(protocol.ErrorRateLimited, "the Workspace's upload budget is spent; retry after "+strconv.Itoa(refused.RetryAfterSeconds)+" seconds"))
		return
	}
	content, err := h.Drives.PutContent(ctx, id.workspace, params.DriveID, r.Body, headers.ContentLength.Value, params.SHA256)
	switch {
	case errors.Is(err, drive.ErrContentHashMismatch):
		writeClientAnswer(w, errorAnswer(protocol.ErrorContentHashMismatch, "the body's SHA-256 is not the URL's"))
		return
	case err != nil:
		writeTransferFailure(w, r, err)
		return
	}
	writeClientAnswer(w, clientAnswer{status: http.StatusOK, body: protocol.Encode(protocol.UploadContentResponse{ContentSHA256: content.SHA256, SizeBytes: content.SizeBytes})})
}

// downloadDriveContent answers GET /v1/client/drives/{drive_id}/content/{sha256}
// with the whole content, or with the one range of its Range header (206).
func (h *clientHandler) downloadDriveContent(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	params, paramsErr := contentParams(r)
	query, queryErr := protocol.DecodeCommonQuery(r.URL.Query())
	headers, headersErr := protocol.DecodeDownloadContentRequestHeaders(r.Header)
	if _, err := h.checkTransfer(ctx, id, transferRequest{driveID: params.DriveID, connectionID: query.ConnectionID, bindingID: query.BindingID,
		refusal: firstFailure(paramsErr, queryErr, headersErr)}); err != nil {
		writeClientFailure(w, r, err)
		return
	}
	held, err := h.Drives.FindContent(ctx, id.workspace, params.DriveID, params.SHA256)
	if err != nil {
		writeTransferFailure(w, r, err)
		return
	}
	first, last, status := int64(0), held.SizeBytes-1, http.StatusOK
	if headers.Range.Set {
		var ok bool
		if first, last, ok = headers.Range.Value.Answer(held.SizeBytes); !ok {
			writeClientAnswer(w, errorAnswer(protocol.ErrorRangeNotSatisfiable, "the range selects no byte of the content"))
			return
		}
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(first, 10)+"-"+strconv.FormatInt(last, 10)+"/"+strconv.FormatInt(held.SizeBytes, 10))
	}
	body, err := h.Drives.OpenRange(ctx, held, first, last-first+1)
	if err != nil {
		writeTransferFailure(w, r, err)
		return
	}
	defer func() { _ = body.Close() }()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(last-first+1, 10))
	w.WriteHeader(status)
	_, _ = io.Copy(w, body)
}

// commitDriveChanges answers POST /v1/client/drives/{drive_id}/commits with
// a machine commit written by the binding's Session and computer.
func (h *clientHandler) commitDriveChanges(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	params, paramsErr := protocol.DecodeDrivePathParams(map[string]string{"drive_id": chi.URLParam(r, "drive_id")})
	var body []byte
	if r.ContentLength <= protocol.CommitRequestMaxJSONBytes {
		var err error
		if body, err = io.ReadAll(io.LimitReader(r.Body, protocol.CommitRequestMaxJSONBytes+1)); err != nil {
			writeClientAnswer(w, errorAnswer(protocol.ErrorInvalidMessage, "the request body could not be read"))
			return
		}
	}
	var refusal error
	request, bodyErr := protocol.DecodeCommitRequest(body)
	if r.ContentLength > protocol.CommitRequestMaxJSONBytes || len(body) > protocol.CommitRequestMaxJSONBytes {
		refusal = refuse(protocol.ErrorRequestTooLarge, "the commit body exceeds 16 MiB")
	} else {
		refusal = firstFailure(paramsErr, bodyErr)
	}
	binding, err := h.checkTransfer(ctx, id, transferRequest{driveID: params.DriveID, connectionID: request.ConnectionID, bindingID: request.BindingID, write: true, refusal: refusal})
	if err == nil {
		err = protocol.CheckCommitChanges(request)
	}
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	result, err := h.Drives.Commit(ctx, id.workspace, params.DriveID, drive.Writer{Kind: drive.WriterSession, ID: binding.SessionID, ComputerID: binding.ComputerID},
		drive.ModeMachine, commitChanges(request.Changes))
	var missing *drive.ContentMissingError
	switch {
	case errors.As(err, &missing):
		writeClientAnswer(w, clientAnswer{status: http.StatusConflict, body: protocol.Encode(protocol.ErrorBody{Code: protocol.ErrorContentMissing,
			Message: "the drive does not hold the content of a write", MissingSHA256: protocol.Some(missing.MissingSHA256)})})
		return
	case err != nil:
		writeTransferFailure(w, r, err)
		return
	}
	writeClientAnswer(w, clientAnswer{status: http.StatusOK, body: protocol.Encode(commitResponse(result))})
}

// commitChanges maps a commit's changes to drive changes in request order.
func commitChanges(changes []protocol.CommitChange) []drive.Change {
	mapped := make([]drive.Change, 0, len(changes))
	for _, change := range changes {
		switch change := change.(type) {
		case protocol.WriteChange:
			base := drive.Base{Kind: drive.BaseMustNotExist}
			if !change.BaseSHA256.Null {
				base = drive.Base{Kind: drive.BaseHash, SHA256: change.BaseSHA256.Value}
			}
			mapped = append(mapped, drive.Change{Path: change.Path, Base: base, ContentSHA256: change.ContentSHA256, Executable: change.Executable})
		case protocol.DeleteChange:
			mapped = append(mapped, drive.Change{Path: change.Path, Delete: true, Base: drive.Base{Kind: drive.BaseHash, SHA256: change.BaseSHA256}})
		}
	}
	return mapped
}

// commitResponse maps a commit's outcome to its answer.
func commitResponse(result drive.CommitResult) protocol.CommitResponse {
	response := protocol.CommitResponse{Seq: protocol.Null[int64](), Results: make([]protocol.CommitResult, 0, len(result.Results))}
	if result.Seq != nil {
		response.Seq = protocol.Value(*result.Seq)
	}
	for _, outcome := range result.Results {
		if outcome.Accepted {
			version := protocol.Null[string]()
			if outcome.VersionID != nil {
				version = protocol.Value(*outcome.VersionID)
			}
			response.Results = append(response.Results, protocol.AcceptedResult{VersionID: version})
			continue
		}
		rejected := protocol.RejectedResult{Reason: string(outcome.Reason)}
		if outcome.Rule != "" {
			rejected.Rule = protocol.Some(outcome.Rule)
		}
		if current := outcome.Current; current != nil {
			content := protocol.Null[string]()
			if current.ContentSHA256 != nil {
				content = protocol.Value(*current.ContentSHA256)
			}
			rejected.Current = protocol.Some(protocol.CurrentVersion{VersionID: current.VersionID, ContentSHA256: content, SizeBytes: current.SizeBytes,
				Executable: current.Executable, Writer: driveWriter(current.Writer)})
		}
		response.Results = append(response.Results, rejected)
	}
	return response
}

// driveWriter is a version's writer as the drive transfer names it.
func driveWriter(writer drive.Writer) protocol.DriveWriter {
	mapped := protocol.DriveWriter{Type: string(writer.Kind)}
	switch writer.Kind {
	case drive.WriterAPIKey:
		mapped.APIKeyID = writer.ID
	case drive.WriterUser:
		mapped.UserID = writer.ID
	case drive.WriterServiceAccount:
		mapped.ServiceAccountID = writer.ID
	case drive.WriterSession:
		mapped.SessionID, mapped.ComputerID = writer.ID, writer.ComputerID
	}
	return mapped
}
