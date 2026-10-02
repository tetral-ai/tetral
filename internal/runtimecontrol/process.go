package runtimecontrol

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"strings"

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
type Process struct {
	Namespace           string
	PodUID              string
	ID                  string
	RegistrationOrder   int64
	RegistrationReceipt string
	Phase               string
	Current             bool
	ReportedAt          sql.NullTime
	RetiredAt           sql.NullTime
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

func readProcessTx(ctx context.Context, tx *dbconnect.Tx, identity ProcessIdentity, lock string) (Process, bool, error) {
	var process Process
	err := tx.QueryRow(ctx, `SELECT namespace,pod_uid,runtime_process_id,registration_order,registration_receipt,phase,is_current,reported_at,retired_at
 FROM runtime_processes WHERE namespace=$1 AND pod_uid=$2 AND runtime_process_id=$3 `+lock,
		identity.Namespace, identity.PodUID, identity.ID).Scan(&process.Namespace, &process.PodUID, &process.ID, &process.RegistrationOrder, &process.RegistrationReceipt, &process.Phase, &process.Current, &process.ReportedAt, &process.RetiredAt)
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
	err := tx.QueryRow(ctx, `SELECT namespace,pod_uid,runtime_process_id,registration_order,registration_receipt,phase,is_current,reported_at,retired_at
 FROM public.tetral_lock_runtime_process($1,$2,$3)`, identity.Namespace, identity.PodUID, identity.ID).Scan(&process.Namespace, &process.PodUID, &process.ID, &process.RegistrationOrder, &process.RegistrationReceipt, &process.Phase, &process.Current, &process.ReportedAt, &process.RetiredAt)
	if dbconnect.IsNoRows(err) {
		return Process{}, processStale()
	}
	return process, err
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
		_, err = tx.Exec(ctx, `INSERT INTO runtime_processes(namespace,pod_uid,runtime_process_id,registration_order,registration_receipt) VALUES($1,$2,$3,$4,$5)`, result.Namespace, result.PodUID, result.ID, result.RegistrationOrder, result.RegistrationReceipt)
		return err
	})
	return result, err
}

// ReportProcess locks only Pod/process rows. It never locks a Session: a
// promotion waits for Session writers holding the old process's shared lock.
func ReportProcess(ctx context.Context, client *dbconnect.Client, identity ProcessIdentity, receipt, phase string) (Process, bool, error) {
	if err := ValidateProcessIdentity(identity); err != nil {
		return Process{}, false, err
	}
	if receipt == "" || (phase != ProcessAccepting && phase != ProcessDraining) {
		return Process{}, false, status.Error(codes.InvalidArgument, "runtime process report is malformed")
	}
	if client == nil {
		return Process{}, false, status.Error(codes.Unavailable, "runtime registry is unavailable")
	}
	var result Process
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
		if _, err := tx.Exec(ctx, `UPDATE runtime_processes SET phase=$4,is_current=$5,reported_at=clock_timestamp(),retired_at=CASE WHEN NOT $5 AND $4='draining' THEN clock_timestamp() ELSE retired_at END WHERE namespace=$1 AND pod_uid=$2 AND runtime_process_id=$3`, identity.Namespace, identity.PodUID, identity.ID, phase, process.Current); err != nil {
			return err
		}
		result, _, err = readProcessTx(ctx, tx, identity, "")
		return err
	})
	return result, promoted, err
}
