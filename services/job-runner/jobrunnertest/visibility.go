// Package jobrunnertest supplies Kubernetes observations for Job Runner
// delivery-store compositions. It derives them from the Runtime bindings that a
// test has committed, so tests drive the production process-aware target
// resolver without a Kubernetes API server or a process-unaware shortcut.
package jobrunnertest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/tetral-ai/tetral/internal/kubernetes"
)

// BindingVisibility observes the Runtime Pods named by committed Session
// bindings. Every bound Pod appears ready and serving at its bound UID and
// address, so the production classifier still decides reuse, unavailability
// or loss from the registered process facts. It never reports a Pod absent and
// therefore never proves loss; loss tests supply their own observations.
//
// Read it through a pool other than the delivery store's own: snapshots are
// taken while the store holds a transaction connection.
type BindingVisibility struct {
	db *sql.DB
}

// NewBindingVisibility reads bindings through db, which needs SELECT on
// workspaces and session_runtime_bindings.
func NewBindingVisibility(db *sql.DB) BindingVisibility {
	return BindingVisibility{db: db}
}

// Snapshot lists every bound Pod as a ready candidate. A read failure yields an
// unready snapshot, which the classifier treats as unavailable rather than lost.
func (v BindingVisibility) Snapshot() kubernetes.BindingVisibilitySnapshot {
	pods, err := v.boundPods(context.Background())
	if err != nil {
		return kubernetes.NewBindingVisibilitySnapshotForTest(false, nil)
	}
	return kubernetes.NewBindingVisibilitySnapshotForTest(true, pods)
}

// GetPod confirms a bound Pod as present, running and ready at its bound
// identity. A Pod named by no binding, or by bindings with different
// identities, is an observation failure, never proof of absence.
func (v BindingVisibility) GetPod(ctx context.Context, namespace, name string) (*kubernetes.PodObservation, error) {
	pods, err := v.boundPods(ctx)
	if err != nil {
		return nil, err
	}
	var found *kubernetes.BindingCandidate
	for index := range pods {
		pod := pods[index]
		if pod.Namespace != namespace || pod.PodName != name {
			continue
		}
		if found != nil && (found.PodUID != pod.PodUID || found.PodIP != pod.PodIP) {
			return nil, fmt.Errorf("bound Pod %s/%s has conflicting identities", namespace, name)
		}
		found = &pod
	}
	if found == nil {
		return nil, fmt.Errorf("no binding names Pod %s/%s", namespace, name)
	}
	return &kubernetes.PodObservation{Namespace: namespace, Name: name, UID: found.PodUID, IP: found.PodIP, Running: true, Ready: true}, nil
}

// UnavailableLoadClient refuses every placement load probe immediately, so a
// Session without a binding fails placement with a retryable error instead of
// probing real addresses or binding to a fixture Pod it never seeded.
func UnavailableLoadClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy: nil,
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("fixture Runtime load probes are unavailable")
		},
	}}
}

func (v BindingVisibility) boundPods(ctx context.Context) ([]kubernetes.BindingCandidate, error) {
	if v.db == nil {
		return nil, errors.New("binding visibility has no database")
	}
	rows, err := v.db.QueryContext(ctx, `SELECT id FROM workspaces ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var workspaces []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		workspaces = append(workspaces, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	var pods []kubernetes.BindingCandidate
	for _, workspaceID := range workspaces {
		bound, err := v.workspaceBoundPods(ctx, workspaceID)
		if err != nil {
			return nil, err
		}
		pods = append(pods, bound...)
	}
	return pods, nil
}

// Bindings are Workspace-RLS rows; each Workspace is read under its own
// transaction-local tenant setting.
func (v BindingVisibility) workspaceBoundPods(ctx context.Context, workspaceID string) ([]kubernetes.BindingCandidate, error) {
	tx, err := v.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var applied string
	if err := tx.QueryRowContext(ctx, `SELECT set_config('tetral.workspace_id', $1, true)`, workspaceID).Scan(&applied); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT agent_runtime_namespace, agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip
		FROM session_runtime_bindings WHERE workspace_id=$1 ORDER BY bound_at, session_id`, workspaceID)
	if err != nil {
		return nil, err
	}
	var pods []kubernetes.BindingCandidate
	for rows.Next() {
		var pod kubernetes.BindingCandidate
		if err := rows.Scan(&pod.Namespace, &pod.PodName, &pod.PodUID, &pod.PodIP); err != nil {
			_ = rows.Close()
			return nil, err
		}
		pods = append(pods, pod)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return pods, tx.Commit()
}
