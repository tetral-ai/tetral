package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

type placementAuthorityTrace struct {
	reached, release chan struct{}
	once             sync.Once
}

func (tr *placementAuthorityTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, placementTraceSQLKey{}, data.SQL)
}

type placementTraceSQLKey struct{}

func (tr *placementAuthorityTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	query, _ := ctx.Value(placementTraceSQLKey{}).(string)
	if data.Err == nil && strings.Contains(query, "FROM public.tetral_lock_runtime_process") {
		tr.once.Do(func() { close(tr.reached); <-tr.release })
	}
}

func TestPostgreSQLReplicaPlacementPromotionRaces(t *testing.T) {
	for _, bindingFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("binding_first_%t", bindingFirst), func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			sessionfixture.SeedBridgeAPISession(t, admin, "default", "placement-race-session", "placement-race-thread")
			seedBridgeAPIEvent(t, admin, "default", "placement-race-session", "placement-race-thread", "placement-race-source", 1, "session.status_rescheduled", "{}")
			if _, err := admin.Exec(`INSERT INTO session_runtime_status(workspace_id,session_id,status,created_at,updated_at) VALUES('default','placement-race-session','idle',clock_timestamp(),clock_timestamp())`); err != nil {
				t.Fatal(err)
			}
			candidate := kubernetes.BindingCandidate{Namespace: "tetral-agent-runtime", PodName: "placement-race-pod", PodUID: "placement-race-uid", PodIP: "10.0.0.10"}
			seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), candidate.Namespace, candidate.PodUID)
			promoter := serveReplicaBridge(t, bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB)), map[string]string{"placement-process-token": candidate.PodUID}, nil)
			rpcCtx := replicaRuntimeContext(ctx, "placement-process-token")
			next, err := promoter.Client.RegisterRuntimeProcess(rpcCtx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "replacement-placement-race"})
			if err != nil {
				t.Fatal(err)
			}
			enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest("default", "placement-race-session", "placement-race-thread", "placement-race-source", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			q := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin))
			if _, err := q.Enqueue(ctx, enqueue); err != nil {
				t.Fatal(err)
			}
			leased, err := q.Lease(ctx, queue.LeaseRequest{WorkspaceID: "default", Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "placement-race", MaxJobs: 1, LeaseDuration: time.Minute})
			if err != nil || len(leased) != 1 {
				t.Fatalf("actual Queue capability=%v/%v", leased, err)
			}
			job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
			if err != nil {
				t.Fatal(err)
			}
			sampled, resumeHTTP := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(sampled)
				select {
				case <-resumeHTTP:
				case <-r.Context().Done():
					return
				}
				_, _ = io.WriteString(w, "runtimepod_active_sessions 0\nruntimepod_session_capacity 16\nruntimepod_container_memory_usage_bytes 100\nruntimepod_container_memory_limit_bytes 1000\nruntimepod_ready 1\nruntimepod_accepting_commands 1\n")
			}))
			defer server.Close()
			transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}}
			defer transport.CloseIdleConnections()
			runnerRole := storagetest.OpenWorkloadDB(t, admin, "job_runner")
			trace := &placementAuthorityTrace{reached: make(chan struct{}), release: make(chan struct{})}
			pool := storagetest.OpenRuntimeRoleDBWithTracer(t, runnerRole.DB, trace)
			store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(pool), 19090, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: &http.Client{Transport: transport}, Snapshot: func() kubernetes.BindingVisibilitySnapshot {
				return kubernetes.NewBindingVisibilitySnapshotForTest(true, []kubernetes.BindingCandidate{candidate})
			}})
			type result struct {
				plan jobrunner.RuntimeCommandPlan
				err  error
			}
			done := make(chan result, 1)
			go func() { plan, err := store.ActivateRuntimeRecovery(ctx, job); done <- result{plan, err} }()
			select {
			case <-sampled:
			case <-ctx.Done():
				close(resumeHTTP)
				close(trace.release)
				t.Fatal("actual native sample barrier not reached")
			}
			promoted := make(chan error, 1)
			promote := func() {
				_, err := promoter.Client.ReportRuntimeProcess(rpcCtx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: next.RuntimeProcessId, RegistrationReceipt: next.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING})
				promoted <- err
			}
			if bindingFirst {
				close(resumeHTTP)
				select {
				case <-trace.reached:
				case <-ctx.Done():
					close(trace.release)
					t.Fatal("actual Runner process authority barrier not reached")
				}
				go promote()
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					var waiting bool
					if err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%ORDER BY registration_order FOR UPDATE%')`).Scan(&waiting); err != nil {
						close(trace.release)
						t.Fatal(err)
					}
					if waiting {
						break
					}
					select {
					case <-ticker.C:
					case <-ctx.Done():
						close(trace.release)
						t.Fatal("promotion did not wait for actual binding creation authority")
					}
				}
				var count int
				if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_bindings WHERE session_id='placement-race-session'`).Scan(&count); err != nil || count != 0 {
					close(trace.release)
					t.Fatalf("uncommitted binding visible=%d/%v", count, err)
				}
				close(trace.release)
			} else {
				go promote()
				if err := <-promoted; err != nil {
					close(resumeHTTP)
					close(trace.release)
					t.Fatal(err)
				}
				close(resumeHTTP)
				close(trace.release)
			}
			select {
			case got := <-done:
				var count int
				if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_bindings WHERE session_id='placement-race-session'`).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if bindingFirst {
					if got.err != nil || count != 1 || got.plan.Target.RuntimeProcessID != "process_"+candidate.PodUID {
						t.Fatalf("admitted binding creation=%+v/%v rows%d", got.plan, got.err, count)
					}
					if err := <-promoted; err != nil {
						t.Fatal(err)
					}
				} else if got.err == nil || count != 0 {
					t.Fatalf("retired chosen process created binding=%+v/%v rows%d", got.plan, got.err, count)
				}
			case <-ctx.Done():
				t.Fatal("actual binding/promotion owners failed to join")
			}
			var current string
			if err := admin.QueryRow(`SELECT runtime_process_id FROM runtime_processes WHERE pod_uid=$1 AND is_current`, candidate.PodUID).Scan(&current); err != nil || current != next.RuntimeProcessId {
				t.Fatalf("final winner=%s/%v", current, err)
			}
			// The original Queue capability remains independently accepted; no new
			// Runtime command or Session loss settlement is invented by arbitration.
			var status, token string
			if err := admin.QueryRow(`SELECT status,lease_token FROM queue_jobs WHERE id=$1`, job.JobID).Scan(&status, &token); err != nil || status != "leased" || token != job.LeaseToken {
				t.Fatalf("binding race changed Queue custody=%s/%v", status, err)
			}
		})
	}
}
