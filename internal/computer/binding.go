package computer

import (
	"context"
	"database/sql"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Binding kinds and states.
const (
	BindingPrepare   = "prepare"
	BindingLateClose = "late_close"

	BindingPreparing = "preparing"
	BindingPrepared  = "prepared"
	BindingFailed    = "failed"
	BindingInvalid   = "invalid"
	BindingClosing   = "closing"
	BindingClosed    = "closed"
)

// PrepareEnvelopeLimit is the largest session.prepare envelope Engine
// issues, in bytes: one SSE event's 64 KiB.
const PrepareEnvelopeLimit = 65536

// prepareTooLargeMessage answers a computer whose session.prepare would
// exceed PrepareEnvelopeLimit.
const prepareTooLargeMessage = "computer session.prepare would exceed the 65536-byte limit"

// platformIdentityName and the email session+<session_id>@agents.tetral.ai
// are the commit identity of a repository the developer gave none.
const (
	platformIdentityName        = "Tetral Agent"
	platformIdentityEmailDomain = "@agents.tetral.ai"
)

// Binding is one computer_bindings row.
type Binding struct {
	SessionID        string
	ComputerID       string
	BindingID        string
	InstanceID       string
	ConnectionID     string
	Kind             string
	State            string
	PrepareMessageID *string
	CWD              *string
	ErrorCode        *string
	CreatedAt        time.Time
	ReportedAt       *time.Time
}

const bindingColumns = `session_id,computer_id,binding_id,instance_id,connection_id,kind,state,prepare_message_id,cwd,error_code,created_at,reported_at`

// IssuePreparation issues one session.prepare for computerID of sessionID,
// through the connection's instruction store, when the computer is attached,
// its current instance has an online connection and the computer has no
// preparing, prepared or failed binding on that connection. The new binding
// is preparing, and so is the computer, with that connection and binding. It
// locks the connection before the computer, the order of the ready route,
// and returns the new binding, or nil when nothing was issued.
func IssuePreparation(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (*Binding, error) {
	var currentInstanceID sql.NullString
	err := tx.QueryRow(ctx, `SELECT current_instance_id FROM computers WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND association_state='attached'`,
		string(ws), sessionID, computerID).Scan(&currentInstanceID)
	if errors.Is(err, sql.ErrNoRows) || err == nil && !currentInstanceID.Valid {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return nil, err
	}
	connection, found, err := instance.LockLiveConnectionOf(ctx, tx, ws, currentInstanceID.String, "", now)
	if err != nil || !found || connection.ReadyAcceptedAt == nil {
		return nil, err
	}
	objects, err := scanObjects(tx.Query(ctx, `SELECT `+objectColumns+` FROM computers
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND association_state='attached' AND current_instance_id=$4 FOR UPDATE`,
		string(ws), sessionID, computerID, connection.InstanceID))
	if err != nil || len(objects) == 0 {
		return nil, err
	}
	var active int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM computer_bindings WHERE workspace_id=$1 AND computer_id=$2 AND connection_id=$3
		AND state IN ('preparing','prepared','failed')`, string(ws), computerID, connection.ID).Scan(&active); err != nil || active > 0 {
		return nil, err
	}
	return issue(ctx, tx, ws, connection, objects[0])
}

// IssuePreparationsOn issues, through IssuePreparation, a session.prepare
// for every attached computer whose current instance is instanceID, in the
// order the computers were added, returning only newly issued bindings.
func IssuePreparationsOn(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) ([]Binding, error) {
	rows, err := tx.Query(ctx, `SELECT session_id,id FROM computers WHERE workspace_id=$1 AND current_instance_id=$2 AND association_state='attached'
		ORDER BY storage_sequence`, string(ws), instanceID)
	if err != nil {
		return nil, err
	}
	var computers [][2]string
	for rows.Next() {
		var computer [2]string
		if err := rows.Scan(&computer[0], &computer[1]); err != nil {
			_ = rows.Close()
			return nil, err
		}
		computers = append(computers, computer)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	var issued []Binding
	for _, computer := range computers {
		binding, err := IssuePreparation(ctx, tx, ws, computer[0], computer[1])
		if err != nil {
			return nil, err
		}
		if binding != nil {
			issued = append(issued, *binding)
		}
	}
	return issued, nil
}

// PrepareAgain replaces the failed binding of computerID on its current
// instance's online connection: the binding becomes invalid and a new
// session.prepare with a new binding_id is issued. No session.close is
// issued. It reports whether it issued.
func PrepareAgain(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (bool, error) {
	binding, err := prepareAgain(ctx, tx, ws, sessionID, computerID)
	return binding != nil, err
}

// prepareAgain is PrepareAgain returning the new binding, or nil when it
// issued nothing.
func prepareAgain(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (*Binding, error) {
	if _, err := tx.Exec(ctx, `UPDATE computer_bindings b SET state='invalid'
		FROM computers c WHERE b.workspace_id=$1 AND b.computer_id=$2 AND b.state='failed'
		AND c.workspace_id=b.workspace_id AND c.session_id=$3 AND c.id=b.computer_id AND b.instance_id=c.current_instance_id`,
		string(ws), computerID, sessionID); err != nil {
		return nil, err
	}
	return IssuePreparation(ctx, tx, ws, sessionID, computerID)
}

// issue writes the session.prepare of object on connection, its binding and
// the computer's preparing state.
func issue(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, object Object) (*Binding, error) {
	objects := []Object{object}
	if err := attachRepositories(ctx, tx, ws, objects); err != nil {
		return nil, err
	}
	object = objects[0]
	bindingID := id.New("bind_")
	payload := preparePayload(object.sessionID, bindingID, object.Kind, preparationCWD(object), object.DriveExclude, object.Repositories, nil)
	messageID, err := instance.IssueInstruction(ctx, tx, ws, connection, payload, "")
	if err != nil {
		return nil, err
	}
	var created time.Time
	if err := tx.QueryRow(ctx, `INSERT INTO computer_bindings (workspace_id,session_id,computer_id,binding_id,instance_id,connection_id,kind,state,prepare_message_id)
		VALUES ($1,$2,$3,$4,$5,$6,'prepare','preparing',$7) RETURNING created_at`, string(ws), object.sessionID, object.ID, bindingID, connection.InstanceID, connection.ID, messageID).Scan(&created); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `UPDATE computers SET execution_state='preparing',connection_id=$4,binding_id=$5,error_code=NULL,error_message=NULL
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3`, string(ws), object.sessionID, object.ID, connection.ID, bindingID)
	if err != nil {
		return nil, err
	}
	return &Binding{SessionID: object.sessionID, ComputerID: object.ID, BindingID: bindingID, InstanceID: connection.InstanceID,
		ConnectionID: connection.ID, Kind: BindingPrepare, State: BindingPreparing, PrepareMessageID: &messageID, CreatedAt: created}, nil
}

// preparationCWD is the cwd a preparation names: the computer's confirmed
// cwd, otherwise its working directory.
func preparationCWD(object Object) string {
	if object.CWD != nil {
		return *object.CWD
	}
	return object.WorkingDirectory
}

// preparePayload is the session.prepare payload of a computer of kind with
// cwd, its drive exclusions and repositories and the drive bindings drives.
// A cloud computer creates its cwd; a user computer has no repositories.
func preparePayload(sessionID, bindingID, kind, cwd string, exclude []DriveExclude, repositories []Repository, drives []protocol.DriveBinding) protocol.SessionPreparePayload {
	payload := protocol.SessionPreparePayload{SessionID: sessionID, BindingID: bindingID, Cwd: cwd,
		Drives: append([]protocol.DriveBinding{}, drives...), DriveExclude: []protocol.DriveExclude{}, Repositories: []protocol.Repository{}}
	for _, entry := range exclude {
		payload.DriveExclude = append(payload.DriveExclude, protocol.DriveExclude(entry))
	}
	if kind != KindCloud {
		return payload
	}
	payload.CreateCwd = protocol.Some(true)
	for _, repository := range preparationRepositoryOrder(repositories) {
		prepared := protocol.Repository{URL: repository.URL, Path: repository.Path, Checkout: protocol.Null[protocol.Checkout](),
			GitIdentity: protocol.GitIdentity{Name: platformIdentityName, Email: "session+" + sessionID + platformIdentityEmailDomain}}
		if repository.Checkout != nil {
			checkout := protocol.Checkout{Type: repository.Checkout.Type}
			if repository.Checkout.Type == "branch" {
				checkout.Name = protocol.Some(repository.Checkout.Name)
			} else {
				checkout.SHA = protocol.Some(repository.Checkout.SHA)
			}
			prepared.Checkout = protocol.Nullable[protocol.Checkout]{Value: checkout, Present: true}
		}
		if repository.GitIdentity != nil {
			prepared.GitIdentity = protocol.GitIdentity{Name: repository.GitIdentity.Name, Email: repository.GitIdentity.Email}
		}
		payload.Repositories = append(payload.Repositories, prepared)
	}
	return payload
}

// preparationRepositoryOrder puts ancestor destinations before descendants.
// Among available entries, creation order wins. Only the payload is reordered;
// stored paths and the public computer object's order remain unchanged. Lexical
// comparison matches the CLI failed-ancestor guard; it does not resolve symlinks.
func preparationRepositoryOrder(repositories []Repository) []Repository {
	ancestor := func(a, b string) bool {
		a, b = path.Clean(a), path.Clean(b)
		return a != b && strings.HasPrefix(b, strings.TrimRight(a, "/")+"/")
	}
	dependencies := make([]int, len(repositories))
	for i, repository := range repositories {
		for _, other := range repositories {
			if ancestor(other.Path, repository.Path) {
				dependencies[i]++
			}
		}
	}
	ordered := make([]Repository, 0, len(repositories))
	for range repositories {
		for i, count := range dependencies {
			if count != 0 {
				continue
			}
			dependencies[i] = -1
			ordered = append(ordered, repositories[i])
			for j, other := range repositories {
				if ancestor(repositories[i].Path, other.Path) {
					dependencies[j]--
				}
			}
			break
		}
	}
	return ordered
}

// PrepareEnvelopeSize is the size in bytes of the compact session.prepare
// envelope issuing would write for a computer of kind in sessionID with
// working directory workingDirectory, its drive exclusions and repositories,
// and the Session's drive bindings drives, with every generated identifier at
// the length Engine generates it and a traceparent.
func PrepareEnvelopeSize(sessionID, kind, workingDirectory string, exclude []DriveExclude, repositories []Repository, drives []protocol.DriveBinding) int {
	payload := preparePayload(sessionID, "bind_"+strings.Repeat("0", 16), kind, workingDirectory, exclude, repositories, drives)
	envelope := protocol.Downstream{ConnectionID: "conn_" + strings.Repeat("0", 32), MessageID: "msg_" + strings.Repeat("0", 16),
		Traceparent: protocol.Some("00-" + strings.Repeat("0", 32) + "-" + strings.Repeat("0", 16) + "-00"), Payload: payload}
	return len(protocol.Encode(envelope))
}

// CheckPrepareSize refuses, as invalid, a checked computer of sessionID
// whose session.prepare, with its resolved repositories and the Session's
// drive bindings drives, would exceed PrepareEnvelopeLimit.
func CheckPrepareSize(sessionID string, checked Checked, repositories []Repository, drives []protocol.DriveBinding) error {
	if PrepareEnvelopeSize(sessionID, checked.Kind, checked.WorkingDirectory, checked.DriveExclude, repositories, drives) > PrepareEnvelopeLimit {
		return &InvalidError{Message: prepareTooLargeMessage}
	}
	return nil
}

// UnknownPreparationError answers a report or git ticket fetch whose
// request names no preparation or close instruction it may answer.
type UnknownPreparationError struct{ Message string }

func (e *UnknownPreparationError) Error() string { return e.Message }

// LockPreparation reads, FOR UPDATE, the binding bindingID of sessionID whose
// session.prepare prepareMessageID was issued on connectionID, the binding a
// preparation report answers; any other answers UnknownPreparationError.
func LockPreparation(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, sessionID, bindingID, prepareMessageID string) (Binding, error) {
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM computer_bindings
		WHERE workspace_id=$1 AND binding_id=$2 AND kind='prepare' AND connection_id=$3 AND session_id=$4 AND prepare_message_id=$5 FOR UPDATE`,
		string(ws), bindingID, connectionID, sessionID, prepareMessageID))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, &UnknownPreparationError{Message: "request_message_id names no session.prepare of this session and binding on this connection"}
	}
	return binding, err
}

// Report is a stored preparation report: prepared with CWD, the drive mount
// failures and the repository failures, or failed with Error.
type Report struct {
	Prepared           bool
	CWD                string
	DriveMountFailures []protocol.DriveMountFailure
	RepositoryFailures []protocol.RepositoryFailure
	Error              protocol.PreparationError
}

// StoreReport applies a preparation report to binding, locked by
// LockPreparation on connection, at the database clock now. A preparing
// binding becomes prepared, and its computer ready with the report's cwd, or
// failed with the report's error; the same transaction writes the report's
// Session errors and moves the tool calls waiting for the preparation: a
// prepared report releases them (runtimecontrol.ReleasePreparedTx), a failed
// one ends them with its error (runtimecontrol.EndPreparationWaitersTx). A
// binding in any other state changes nothing, so a late report never brings
// a binding back. It reports whether the binding changed.
func StoreReport(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, binding Binding, report Report, now time.Time) (bool, error) {
	if binding.State != BindingPreparing {
		return false, nil
	}
	var kind string
	var lastFailed sql.NullString
	if err := tx.QueryRow(ctx, `SELECT kind,last_failed_code FROM computers WHERE workspace_id=$1 AND session_id=$2 AND id=$3 FOR UPDATE`,
		string(ws), binding.SessionID, binding.ComputerID).Scan(&kind, &lastFailed); err != nil {
		return false, err
	}
	if !report.Prepared {
		if _, err := tx.Exec(ctx, `UPDATE computer_bindings SET state='failed',error_code=$3,error_message=$4,reported_at=$5 WHERE workspace_id=$1 AND binding_id=$2`,
			string(ws), binding.BindingID, report.Error.Code, report.Error.Message, now); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `UPDATE computers SET execution_state='failed',error_code=$4,error_message=$5,last_failed_code=$4
			WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND binding_id=$6`,
			string(ws), binding.SessionID, binding.ComputerID, report.Error.Code, report.Error.Message, binding.BindingID); err != nil {
			return false, err
		}
		if err := runtimecontrol.EndPreparationWaitersTx(ctx, tx, ws, binding.SessionID, binding.ComputerID, connection.ID, report.Error.Code, report.Error.Message); err != nil {
			return false, err
		}
		if lastFailed.Valid && lastFailed.String == report.Error.Code {
			return true, nil
		}
		return true, WriteSessionErrors(ctx, tx, ws, binding.SessionID, now, []SessionError{preparationError(binding.ComputerID, report.Error)})
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_bindings SET state='prepared',cwd=$3,reported_at=$4 WHERE workspace_id=$1 AND binding_id=$2`,
		string(ws), binding.BindingID, report.CWD, now); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE computers SET execution_state='ready',cwd=$4,error_code=NULL,error_message=NULL,last_failed_code=NULL
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND binding_id=$5`,
		string(ws), binding.SessionID, binding.ComputerID, report.CWD, binding.BindingID); err != nil {
		return false, err
	}
	if err := runtimecontrol.ReleasePreparedTx(ctx, tx, ws, binding.SessionID, binding.ComputerID, binding.BindingID, connection); err != nil {
		return false, err
	}
	errs := DriveMountErrors(binding.ComputerID, report.DriveMountFailures)
	if kind == KindCloud {
		errs = append(errs, repositoryErrors(report.RepositoryFailures)...)
	}
	return true, WriteSessionErrors(ctx, tx, ws, binding.SessionID, now, errs)
}

// ColdStart is a computer's cold start for its Session, ended by the stored
// report of a preparation: its start kind and its phases, from stored
// database clock times.
type ColdStart struct {
	StartKind string
	Phases    []workload.StoredPhase
}

// PreparationColdStart reads the cold start that binding's report, stored at
// reportedAt, ends. It is new for the computer's first preparation and
// reloaded for any later one. When this is the computer's first preparation on
// the binding's connection and that connection is the first of its instance
// whose ready was accepted after the first claim of a fulfilled request of the
// computer, its phases begin with request and instance; for a wake request
// fulfilled before any claim, with request alone, up to that ready. They
// always end with prepare.
func PreparationColdStart(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, binding Binding, reportedAt time.Time) (ColdStart, error) {
	var earlier, earlierOnConnection int
	if err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE connection_id=$3) FROM computer_bindings
		WHERE workspace_id=$1 AND computer_id=$2 AND kind='prepare' AND created_at<$4`,
		string(ws), binding.ComputerID, binding.ConnectionID, binding.CreatedAt).Scan(&earlier, &earlierOnConnection); err != nil {
		return ColdStart{}, err
	}
	coldStart := ColdStart{StartKind: "new"}
	if earlier > 0 {
		coldStart.StartKind = "reloaded"
	}
	if earlierOnConnection == 0 {
		var created, ready time.Time
		var claimed sql.NullTime
		err := tx.QueryRow(ctx, `SELECT r.created_at,r.first_claimed_at,c.ready_accepted_at FROM computer_requests r
			JOIN instance_connections c ON c.workspace_id=r.workspace_id AND c.id=$3 AND c.instance_id=r.instance_id
			WHERE r.workspace_id=$1 AND r.computer_id=$2 AND r.state='fulfilled' AND COALESCE(r.first_claimed_at,r.created_at)<=c.ready_accepted_at
			AND NOT EXISTS (SELECT 1 FROM instance_connections p WHERE p.workspace_id=c.workspace_id AND p.instance_id=c.instance_id
				AND p.storage_sequence<c.storage_sequence AND p.ready_accepted_at>=COALESCE(r.first_claimed_at,r.created_at))
			ORDER BY r.storage_sequence DESC LIMIT 1`, string(ws), binding.ComputerID, binding.ConnectionID).Scan(&created, &claimed, &ready)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return ColdStart{}, err
		}
		switch {
		case err != nil:
		case claimed.Valid:
			coldStart.Phases = append(coldStart.Phases, workload.StoredPhase{Name: "request", Start: created, End: claimed.Time},
				workload.StoredPhase{Name: "instance", Start: claimed.Time, End: ready})
		default:
			coldStart.Phases = append(coldStart.Phases, workload.StoredPhase{Name: "request", Start: created, End: ready})
		}
	}
	coldStart.Phases = append(coldStart.Phases, workload.StoredPhase{Name: "prepare", Start: binding.CreatedAt, End: reportedAt})
	return coldStart, nil
}

// LockGitTicketBinding reads, FOR SHARE, the binding bindingID with a
// session.prepare issued on connectionID for a computer that has
// repositories, which only a cloud computer can have: the binding a git
// ticket fetch names; any other answers UnknownPreparationError.
func LockGitTicketBinding(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, bindingID string) (Binding, error) {
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM computer_bindings b
		WHERE workspace_id=$1 AND binding_id=$2 AND kind='prepare' AND connection_id=$3
		AND EXISTS (SELECT 1 FROM session_github_repository_resources r WHERE r.workspace_id=b.workspace_id AND r.session_id=b.session_id AND r.computer_id=b.computer_id)
		FOR SHARE`, string(ws), bindingID, connectionID))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, &UnknownPreparationError{Message: "binding_id names no preparation of a cloud computer with repositories on this connection"}
	}
	return binding, err
}

func scanBinding(row rowScanner) (Binding, error) {
	var binding Binding
	var prepareMessageID, cwd, errorCode sql.NullString
	var reportedAt sql.NullTime
	if err := row.Scan(&binding.SessionID, &binding.ComputerID, &binding.BindingID, &binding.InstanceID, &binding.ConnectionID, &binding.Kind, &binding.State,
		&prepareMessageID, &cwd, &errorCode, &binding.CreatedAt, &reportedAt); err != nil {
		return Binding{}, err
	}
	binding.PrepareMessageID, binding.CWD, binding.ErrorCode = nullable(prepareMessageID), nullable(cwd), nullable(errorCode)
	if reportedAt.Valid {
		at := reportedAt.Time.UTC()
		binding.ReportedAt = &at
	}
	binding.CreatedAt = binding.CreatedAt.UTC()
	return binding, nil
}
