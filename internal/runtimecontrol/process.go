package runtimecontrol

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

const (
	ProcessStarting  = "starting"
	ProcessAccepting = "accepting"
	ProcessDraining  = "draining"
)

// Process belongs to an authenticated Pod and a single process boot. Database
// registration order, rather than caller clocks or UUID ordering, arbitrates it.
// It carries lifecycle facts only; report time lives in the separate liveness
// row, which no generic process lock reads.
type Process struct {
	Namespace           string
	PodUID              string
	ID                  string
	RegistrationOrder   int64
	RegistrationReceipt string
	Phase               string
	Current             bool
	RetiredAt           sql.NullTime
}

// ReportedProcess is the result of one successful report: the lifecycle
// snapshot the report was admitted against and the database time it recorded
// on the process's liveness row.
type ReportedProcess struct {
	Process
	ReportedAt time.Time
}

type ProcessIdentity struct{ Namespace, PodUID, ID string }

func LifecycleError(code codes.Code, reason, message string) error {
	result := status.New(code, message)
	detailed, err := result.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "tetral.runtime"})
	if err != nil {
		return result.Err()
	}
	return detailed.Err()
}

func ValidateProcessIdentity(identity ProcessIdentity) error {
	for _, value := range []string{identity.Namespace, identity.PodUID, identity.ID} {
		if value == "" || len(value) > 128 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n\t ") {
			return status.Error(codes.InvalidArgument, "runtime process identity is malformed")
		}
	}
	return nil
}

func processStale() error {
	return LifecycleError(codes.FailedPrecondition, "RUNTIME_PROCESS_STALE", "runtime process is stale")
}

// Registration creates the liveness row with its process. A registered process
// without that row violates the invariant; it is reported, never repaired by
// manufacturing a row.
func processLivenessMissing() error {
	return status.Error(codes.Internal, "runtime process liveness is missing")
}

func readProcessTx(ctx context.Context, tx *dbconnect.Tx, identity ProcessIdentity, lock string) (Process, bool, error) {
	var process Process
	err := tx.QueryRow(ctx, `SELECT namespace,pod_uid,runtime_process_id,registration_order,registration_receipt,phase,is_current,retired_at
 FROM runtime_processes WHERE namespace=$1 AND pod_uid=$2 AND runtime_process_id=$3 `+lock,
		identity.Namespace, identity.PodUID, identity.ID).Scan(&process.Namespace, &process.PodUID, &process.ID, &process.RegistrationOrder, &process.RegistrationReceipt, &process.Phase, &process.Current, &process.RetiredAt)
	if dbconnect.IsNoRows(err) {
		return Process{}, false, nil
	}
	return process, err == nil, err
}

// LockProcessTx holds a shared lock until the caller's Session transaction
// commits. The fixed lock-only definer does not grant Runner mutation rights.
func LockProcessTx(ctx context.Context, tx *dbconnect.Tx, identity ProcessIdentity) (Process, error) {
	if err := ValidateProcessIdentity(identity); err != nil {
		return Process{}, err
	}
	var process Process
	err := tx.QueryRow(ctx, `SELECT namespace,pod_uid,runtime_process_id,registration_order,registration_receipt,phase,is_current,retired_at
 FROM public.tetral_lock_runtime_process($1,$2,$3)`, identity.Namespace, identity.PodUID, identity.ID).Scan(&process.Namespace, &process.PodUID, &process.ID, &process.RegistrationOrder, &process.RegistrationReceipt, &process.Phase, &process.Current, &process.RetiredAt)
	if dbconnect.IsNoRows(err) {
		return Process{}, processStale()
	}
	return process, err
}

// LockProcessLivenessTx holds the process's liveness row FOR SHARE until the
// caller's transaction ends, through its lock-only definer, and returns the
// last report time. A report already holding the row commits or rolls back
// before this returns; a later report waits for the caller. A missing row and
// a NULL report both return an invalid value, which callers must treat as
// unavailable, never as proof of process loss.
func LockProcessLivenessTx(ctx context.Context, tx *dbconnect.Tx, identity ProcessIdentity) (sql.NullTime, error) {
	if err := ValidateProcessIdentity(identity); err != nil {
		return sql.NullTime{}, err
	}
	var reportedAt sql.NullTime
	err := tx.QueryRow(ctx, `SELECT reported_at FROM public.tetral_lock_runtime_process_liveness($1,$2,$3)`, identity.Namespace, identity.PodUID, identity.ID).Scan(&reportedAt)
	if dbconnect.IsNoRows(err) {
		return sql.NullTime{}, nil
	}
	return reportedAt, err
}

func RequireCurrentProcessTx(ctx context.Context, tx *dbconnect.Tx, identity ProcessIdentity) (Process, error) {
	process, err := LockProcessTx(ctx, tx, identity)
	if err != nil {
		return Process{}, err
	}
	if !process.Current || process.RetiredAt.Valid || process.Phase == ProcessStarting {
		return Process{}, ScopeSupersededError(processStale())
	}
	return process, nil
}

func RegisterProcess(ctx context.Context, client *dbconnect.Client, identity ProcessIdentity) (Process, error) {
	if err := ValidateProcessIdentity(identity); err != nil {
		return Process{}, err
	}
	if client == nil {
		return Process{}, status.Error(codes.Unavailable, "runtime registry is unavailable")
	}
	var result Process
	err := client.WithTx(ctx, "runtimecontrol.register_process", nil, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO runtime_process_pods(namespace,pod_uid) VALUES($1,$2) ON CONFLICT DO NOTHING`, identity.Namespace, identity.PodUID); err != nil {
			return err
		}
		var order int64
		if err := tx.QueryRow(ctx, `SELECT next_registration_order FROM runtime_process_pods WHERE namespace=$1 AND pod_uid=$2 FOR UPDATE`, identity.Namespace, identity.PodUID).Scan(&order); err != nil {
			return err
		}
		existing, found, err := readProcessTx(ctx, tx, identity, "FOR UPDATE")
		if err != nil {
			return err
		}
		if found {
			if existing.RetiredAt.Valid || existing.Phase == ProcessDraining {
				return processStale()
			}
			var live bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runtime_process_liveness WHERE namespace=$1 AND pod_uid=$2 AND runtime_process_id=$3)`, identity.Namespace, identity.PodUID, identity.ID).Scan(&live); err != nil {
				return err
			}
			if !live {
				return processLivenessMissing()
			}
			result = existing
			return nil
		}
		var receipt [32]byte
		if _, err := rand.Read(receipt[:]); err != nil {
			return status.Error(codes.Unavailable, "runtime registration receipt is unavailable")
		}
		if err := tx.QueryRow(ctx, `UPDATE runtime_process_pods SET next_registration_order=next_registration_order+1 WHERE namespace=$1 AND pod_uid=$2 RETURNING next_registration_order`, identity.Namespace, identity.PodUID).Scan(&order); err != nil {
			return err
		}
		result = Process{Namespace: identity.Namespace, PodUID: identity.PodUID, ID: identity.ID, RegistrationOrder: order, RegistrationReceipt: base64.RawURLEncoding.EncodeToString(receipt[:]), Phase: ProcessStarting}
		if _, err := tx.Exec(ctx, `INSERT INTO runtime_processes(namespace,pod_uid,runtime_process_id,registration_order,registration_receipt) VALUES($1,$2,$3,$4,$5)`, result.Namespace, result.PodUID, result.ID, result.RegistrationOrder, result.RegistrationReceipt); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO runtime_process_liveness(namespace,pod_uid,runtime_process_id) VALUES($1,$2,$3)`, result.Namespace, result.PodUID, result.ID)
		return err
	})
	return result, err
}

// errProcessReportLifecycle sends a report to the lifecycle path: the
// unchanged-report transaction found nothing it may acknowledge by itself.
var errProcessReportLifecycle = errors.New("runtime process report requires the lifecycle path")

// ReportProcess records one Runtime process report and never locks a Session.
//
// One READ COMMITTED transaction first reads the process row without locking
// it. A missing or retired process is stale and a different receipt is denied.
// An unchanged report of the current process is the fast path: it updates only
// the liveness row, conditioned on the process still being current with the
// same phase and receipt. It therefore never waits for Session mutations
// holding the process row FOR SHARE and never delays promotion. Its
// acknowledgment linearizes at that UPDATE statement's snapshot of the process
// row, so a promotion or drain committing during the statement can supersede
// the reply. The reply is an observation acknowledgment only; it confers no
// mutation authority, placement or custody, all of which use lifecycle fences.
//
// A candidate, a phase change, a draining process asking to accept, or a fast
// UPDATE that affected no row rolls back and takes the lifecycle path in a new
// transaction: Pod row, then process rows in registration order FOR UPDATE,
// then the liveness row, all committed together. A promotion waits for Session
// writers holding the old process's shared lock.
func ReportProcess(ctx context.Context, client *dbconnect.Client, identity ProcessIdentity, receipt, phase string) (ReportedProcess, bool, error) {
	if err := ValidateProcessIdentity(identity); err != nil {
		return ReportedProcess{}, false, err
	}
	if receipt == "" || (phase != ProcessAccepting && phase != ProcessDraining) {
		return ReportedProcess{}, false, status.Error(codes.InvalidArgument, "runtime process report is malformed")
	}
	if client == nil {
		return ReportedProcess{}, false, status.Error(codes.Unavailable, "runtime registry is unavailable")
	}
	reported, acknowledged, err := reportUnchangedProcess(ctx, client, identity, receipt, phase)
	if err != nil || acknowledged {
		return reported, false, err
	}
	return reportProcessLifecycle(ctx, client, identity, receipt, phase)
}

// reportUnchangedProcess commits only when its liveness UPDATE affects the one
// row of a current, unretired process whose stored phase and receipt match.
// Zero affected rows roll back and are not a permission decision.
func reportUnchangedProcess(ctx context.Context, client *dbconnect.Client, identity ProcessIdentity, receipt, phase string) (ReportedProcess, bool, error) {
	var result ReportedProcess
	err := client.WithTx(ctx, "runtimecontrol.report_process_liveness", &sql.TxOptions{Isolation: sql.LevelReadCommitted}, func(tx *dbconnect.Tx) error {
		process, found, err := readProcessTx(ctx, tx, identity, "")
		if err != nil {
			return err
		}
		if !found || process.RetiredAt.Valid {
			return processStale()
		}
		if subtle.ConstantTimeCompare([]byte(receipt), []byte(process.RegistrationReceipt)) != 1 {
			return status.Error(codes.PermissionDenied, "runtime registration receipt does not match")
		}
		if !process.Current || process.Phase != phase {
			return errProcessReportLifecycle
		}
		err = tx.QueryRow(ctx, `UPDATE public.runtime_process_liveness AS live
 SET reported_at=GREATEST(live.reported_at,clock_timestamp())
 WHERE live.namespace=$1 AND live.pod_uid=$2 AND live.runtime_process_id=$3
 AND EXISTS (
  SELECT 1 FROM public.runtime_processes AS process
  WHERE process.namespace=live.namespace AND process.pod_uid=live.pod_uid
    AND process.runtime_process_id=live.runtime_process_id
    AND process.is_current AND process.retired_at IS NULL
    AND process.phase=$4 AND process.registration_receipt=$5
 )
 RETURNING live.reported_at`, identity.Namespace, identity.PodUID, identity.ID, phase, receipt).Scan(&result.ReportedAt)
		if dbconnect.IsNoRows(err) {
			return errProcessReportLifecycle
		}
		if err != nil {
			return err
		}
		result.Process = process
		return nil
	})
	if errors.Is(err, errProcessReportLifecycle) {
		return ReportedProcess{}, false, nil
	}
	if err != nil {
		return ReportedProcess{}, false, err
	}
	return result, true, nil
}

// reportProcessLifecycle preserves registration-order promotion. A shutdown
// candidate is abandoned and never takes custody; draining never returns to
// accepting; a superseded registration order never promotes.
func reportProcessLifecycle(ctx context.Context, client *dbconnect.Client, identity ProcessIdentity, receipt, phase string) (ReportedProcess, bool, error) {
	var result ReportedProcess
	var promoted bool
	err := client.WithTx(ctx, "runtimecontrol.report_process", nil, func(tx *dbconnect.Tx) error {
		var lastPromoted int64
		err := tx.QueryRow(ctx, `SELECT last_promoted_order FROM runtime_process_pods WHERE namespace=$1 AND pod_uid=$2 FOR UPDATE`, identity.Namespace, identity.PodUID).Scan(&lastPromoted)
		if dbconnect.IsNoRows(err) {
			return processStale()
		}
		if err != nil {
			return err
		}
		// Lock old/candidate rows together in registration order. Pod arbitration
		// prevents a competing report from changing the set while they are locked.
		rows, err := tx.Query(ctx, `SELECT runtime_process_id FROM runtime_processes WHERE namespace=$1 AND pod_uid=$2 AND (is_current OR runtime_process_id=$3) ORDER BY registration_order FOR UPDATE`, identity.Namespace, identity.PodUID, identity.ID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var ignored string
			if err := rows.Scan(&ignored); err != nil {
				_ = rows.Close()
				return err
			}
		}
		err = rows.Err()
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		process, found, err := readProcessTx(ctx, tx, identity, "")
		if err != nil {
			return err
		}
		if !found || process.RetiredAt.Valid {
			return processStale()
		}
		if subtle.ConstantTimeCompare([]byte(receipt), []byte(process.RegistrationReceipt)) != 1 {
			return status.Error(codes.PermissionDenied, "runtime registration receipt does not match")
		}
		if process.Phase == ProcessDraining && phase != ProcessDraining {
			return processStale()
		}
		if !process.Current && phase == ProcessAccepting {
			if process.Phase != ProcessStarting || process.RegistrationOrder <= lastPromoted {
				return processStale()
			}
			if _, err := tx.Exec(ctx, `UPDATE runtime_processes SET is_current=FALSE,retired_at=clock_timestamp() WHERE namespace=$1 AND pod_uid=$2 AND is_current`, identity.Namespace, identity.PodUID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE runtime_process_pods SET last_promoted_order=$3 WHERE namespace=$1 AND pod_uid=$2`, identity.Namespace, identity.PodUID, process.RegistrationOrder); err != nil {
				return err
			}
			promoted = true
			process.Current = true
		}
		// A shutdown candidate is abandoned; it never promotes or takes custody.
		if _, err := tx.Exec(ctx, `UPDATE runtime_processes SET phase=$4,is_current=$5,retired_at=CASE WHEN NOT $5 AND $4='draining' THEN clock_timestamp() ELSE retired_at END WHERE namespace=$1 AND pod_uid=$2 AND runtime_process_id=$3`, identity.Namespace, identity.PodUID, identity.ID, phase, process.Current); err != nil {
			return err
		}
		// The lifecycle change and its report time commit together. The retired
		// previous process keeps its own last report.
		err = tx.QueryRow(ctx, `UPDATE public.runtime_process_liveness SET reported_at=GREATEST(reported_at,clock_timestamp()) WHERE namespace=$1 AND pod_uid=$2 AND runtime_process_id=$3 RETURNING reported_at`, identity.Namespace, identity.PodUID, identity.ID).Scan(&result.ReportedAt)
		if dbconnect.IsNoRows(err) {
			return processLivenessMissing()
		}
		if err != nil {
			return err
		}
		result.Process, _, err = readProcessTx(ctx, tx, identity, "")
		return err
	})
	return result, promoted, err
}
