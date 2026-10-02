package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

type separatedBlockingInputSender struct {
	jobrunner.RuntimeCommandSender
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *separatedBlockingInputSender) AcceptInput(ctx context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return s.RuntimeCommandSender.AcceptInput(ctx, target, request)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type separatedReadyListener struct {
	queue.NotificationListener
	ready chan struct{}
	once  sync.Once
}

func (l *separatedReadyListener) Listen(ctx context.Context, channel string, onReady func(), onNotification func(string)) error {
	return l.NotificationListener.Listen(ctx, channel, func() { l.once.Do(func() { close(l.ready) }); onReady() }, onNotification)
}

type separatedOwnedRun struct {
	cancel context.CancelFunc
	done   chan error
	joined bool
}

func separatedStartRun(parent context.Context, t *testing.T, run func(context.Context) error) *separatedOwnedRun {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	owner := &separatedOwnedRun{cancel: cancel, done: make(chan error, 1)}
	// Cleanup is registered before work can start and before pools can close.
	t.Cleanup(func() { owner.join(t) })
	go func() { owner.done <- run(ctx) }()
	return owner
}
func (r *separatedOwnedRun) join(t *testing.T) {
	t.Helper()
	if r.joined {
		return
	}
	r.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case err := <-r.done:
		r.joined = true
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("actual production owner return: %v", err)
		}
	case <-ctx.Done():
		t.Error("production owner did not join before 10s cleanup bound")
	}
}

func TestPostgreSQLSeparatedOwnersResourceLifecycle(t *testing.T) {
	for _, cancelOwner := range []string{"runner", "bridge"} {
		t.Run("cancel_"+cancelOwner, func(t *testing.T) {
			f := newSeparatedOwners(t, "lifecycle_"+cancelOwner, false)
			scope := f.declare(t)
			job, lease := f.input(t)
			base := separatedSender()
			sender := &separatedBlockingInputSender{RuntimeCommandSender: base, entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(sender.release) }) })
			worker := f.worker(sender)
			worker.Config.DrainTimeout = 150 * time.Millisecond
			worker.Config.CancelJoinTimeout = time.Second
			// The already observed real lease enters the real Runner loop; every
			// transition and heartbeat still calls the real Queue service.
			worker.Queue = &issuedLeaseQueueFixture{QueueClient: worker.Queue, job: cleanupQueueJobProto(lease)}
			wake := queue.NewWakeSignal()
			runnerListener := &separatedReadyListener{NotificationListener: queue.PostgreSQLNotificationListener{Client: f.runner.Client}, ready: make(chan struct{})}
			runnerNotify := separatedStartRun(f.ctx, t, func(ctx context.Context) error {
				return queue.RunNotificationListener(ctx, runnerListener, queue.ConsumerClassJobRunner, wake, nil)
			})
			runnerWork := separatedStartRun(f.ctx, t, func(ctx context.Context) error { return jobrunner.RunJobRunnerLoop(ctx, worker, nil, wake) })
			bridgeListen := separatedStartRun(f.ctx, t, f.bridge.RunExecutionResultListener)
			separatedWait(f.ctx, t, runnerListener.ready)
			f.bridgeTrace.waitForSQLCount(t, awaitTraceListen, 1)
			separatedWait(f.ctx, t, sender.entered)
			// Park a real Bridge read on a relation lock that Runner finalization
			// does not own. pg_blocking_pids establishes that read is in flight.
			lock, err := f.admin.BeginTx(f.ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lock.Rollback() })
			var blocker int
			if err := lock.QueryRowContext(f.ctx, `SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
				t.Fatal(err)
			}
			if _, err := lock.ExecContext(f.ctx, `LOCK TABLE session_memory_store_resources IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			readCtx, readCancel := context.WithCancel(f.ctx)
			t.Cleanup(readCancel)
			readDone := make(chan error, 1)
			readOwner := separatedStartRun(readCtx, t, func(ctx context.Context) error {
				_, err := f.bridge.LoadContext(ctx, &bridgev1.LoadContextRequest{Scope: scope})
				readDone <- err
				return nil
			})
			separatedBlockedPIDs(t, f, blocker, 1)
			if cancelOwner == "runner" {
				runnerWork.join(t)
				runnerNotify.join(t)
				if err := f.runnerDB.Close(); err != nil {
					t.Fatal(err)
				}
				if err := lock.Commit(); err != nil {
					t.Fatal(err)
				}
				if err := separatedWait(f.ctx, t, readDone); err != nil {
					t.Fatalf("Bridge read after Runner cancellation=%v", err)
				}
				f.cold(t, scope)
				readOwner.join(t)
				if f.bridgeDB.Stats().InUse != 1 {
					t.Fatalf("Bridge listener independently owned connection count=%d; want1", f.bridgeDB.Stats().InUse)
				}
				bridgeListen.join(t)
				t.Log("actual Runner polling worker+Queue LISTEN returned before Runner pool close; parked Bridge read completed and Bridge LISTEN remained usable")
			} else {
				readCancel()
				if err := separatedWait(f.ctx, t, readDone); err == nil {
					t.Fatal("cancelled Bridge read unexpectedly succeeded")
				}
				readOwner.join(t)
				bridgeListen.join(t)
				if err := f.bridgeDB.Close(); err != nil {
					t.Fatal(err)
				}
				if err := lock.Commit(); err != nil {
					t.Fatal(err)
				}
				release.Do(func() { close(sender.release) })
				// Observe the committed Queue receipt, not a fixture completion flag.
				deadline := time.NewTimer(10 * time.Second)
				defer deadline.Stop()
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for f.count(t, `SELECT count(*) FROM queue_jobs WHERE id=$1 AND status='acknowledged'`, job.JobID) != 1 {
					select {
					case <-ticker.C:
					case <-deadline.C:
						t.Fatal("unaffected Runner did not commit input before cleanup bound")
					}
				}
				runnerWork.join(t)
				if len(base.requests) != 1 || f.count(t, `SELECT count(*) FROM session_runtime_inbox WHERE runtime_input_id=$1 AND status='accepted'`, job.RuntimeInputID) != 1 {
					t.Fatal("Bridge cancellation changed Runner delivery effect")
				}
				runnerNotify.join(t)
				t.Log("actual Bridge read cancelled and execution-result LISTEN returned before Bridge pool close; unaffected Runner delivery committed input=accepted Queue=acknowledged sends=1")
			}
		})
	}
	for _, sink := range []string{"healthy", "disabled", "throwing", "backpressured"} {
		t.Run("diagnostics_"+sink, func(t *testing.T) {
			f := newSeparatedOwners(t, "diagnostics_"+sink, true)
			cfg := workload.DefaultDiagnosticConfig()
			cfg.Burst = 1000
			if sink == "disabled" {
				cfg.Level = slog.LevelError
			}
			writers := []*separatedDiagnosticWriter{{mode: sink, entered: make(chan struct{}), release: make(chan struct{})}, {mode: sink, entered: make(chan struct{}), release: make(chan struct{})}}
			var owners []*workload.ProcessLogger
			for i, service := range []string{"job-runner", "bridge"} {
				owner := workload.NewProcessLogger(writers[i], service, "test", "test", cfg)
				owners = append(owners, owner)
				i, owner := i, owner
				t.Cleanup(func() {
					writers[i].unblock()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					owner.Close(ctx)
				})
			}
			f.runner.Logger = owners[0].Logger
			f.bridge.Logger = owners[1].Logger
			if sink == "backpressured" {
				for i, owner := range owners {
					owner.Logger.Info("sink barrier", "event.kind", "sink_barrier")
					separatedWait(f.ctx, t, writers[i].entered)
					for record := 0; record < 100; record++ {
						owner.Logger.Info("bounded diagnostic fill", "event.kind", "sink_fill", "operation", fmt.Sprintf("fill.%d", record))
					}
				}
			}
			f.bridge.MCPManifestLister = &separatedManifestGate{result: mcpManifestResult("etag_diagnostic", "github_search")}
			manifest, err := f.bridge.McpManifestChanged(f.ctx, &bridgev1.McpManifestChangedRequest{WorkspaceId: "default", SessionId: f.sessionID, McpServerName: "github", ManifestEtag: "etag_diagnostic"})
			if err != nil || manifest.GetCommitted() == nil {
				t.Fatalf("diagnostic invariant manifest=%v/%v", manifest, err)
			}
			sender := separatedSender()
			worker := f.worker(sender)
			worker.Logger = owners[0].Logger
			if err := worker.RunOnce(f.ctx); err != nil {
				t.Fatalf("deliver manifest Queue head: %v", err)
			}
			job, lease := f.input(t)
			if err := runIssuedLeaseThroughRunner(f.ctx, worker, cleanupQueueJobProto(lease), worker.Config); err != nil {
				t.Fatal(err)
			}
			scope := f.declare(t)
			committed, err := f.bridge.CommitInputs(f.ctx, &bridgev1.CommitInputsRequest{Scope: scope, RuntimeInputId: job.RuntimeInputID})
			if err != nil || committed.GetCommitted() == nil {
				t.Fatalf("sink invariant actual input commit=%v/%v", committed, err)
			}
			// An observed old lease cannot obtain authority after ACK. The actual
			// persisted input receipt remains replayable without another send.
			if err := runIssuedLeaseThroughRunner(f.ctx, worker, cleanupQueueJobProto(lease), worker.Config); err == nil {
				t.Fatal("old acknowledged lease unexpectedly retained authority")
			}
			if _, found, err := f.runner.ReplayRuntimeDeliveryFinalization(f.ctx, job); err != nil || !found {
				t.Fatalf("durable input finalization replay=%t/%v", found, err)
			}
			request := &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "rwrite_sink_receipt", ModelRequestId: "mreq_sink", EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_sink"}`, RequestKind: "agent_provider_request", ContextThroughMessageSequence: bridgeAPIInt64(1)}
			receipt, err := f.bridge.WriteEvent(f.ctx, request)
			if err != nil || receipt.GetCommitted() == nil {
				t.Fatalf("sink fault durable Bridge receipt=%v/%v", receipt, err)
			}
			replay, err := f.bridge.WriteEvent(f.ctx, request)
			if err != nil || replay.GetDuplicate().GetEventId() != receipt.GetCommitted().GetEventId() {
				t.Fatalf("sink fault replay=%v/%v", replay, err)
			}
			if len(sender.requests) != 2 || f.count(t, `SELECT count(*) FROM session_runtime_inbox WHERE runtime_input_id=$1 AND status='committed'`, job.RuntimeInputID) != 1 || f.count(t, `SELECT count(*) FROM queue_jobs WHERE id=$1 AND status='acknowledged'`, job.JobID) != 1 || f.count(t, `SELECT count(*) FROM session_events WHERE session_id=$1 AND runtime_write_id='rwrite_sink_receipt'`, f.sessionID) != 1 {
				t.Fatal("diagnostic sink changed business receipt or manifest+input send counts")
			}
			assertQueuedMCPManifestGenerations(t, f.admin, f.sessionID, []int64{1})
			for i, owner := range owners {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				started := time.Now()
				owner.Close(ctx)
				cancel()
				if time.Since(started) > time.Second {
					t.Fatal("diagnostic shutdown exceeded budget")
				}
				stats := owner.Stats()
				if sink == "backpressured" && stats.Dropped == 0 {
					t.Fatal("backpressure fault never saturated queue")
				}
				if sink == "throwing" && stats.SinkFailures == 0 {
					t.Fatal("throwing sink fault not observed")
				}
				if sink == "disabled" && stats.Filtered == 0 {
					t.Fatal("disabled diagnostic configuration not exercised")
				}
				writers[i].unblock()
				joinCtx, joinCancel := context.WithTimeout(context.Background(), 10*time.Second)
				owner.Close(joinCtx)
				joinCancel()
				t.Logf("sink=%s owner=%d durable event receipt=%s input=committed Queue=acknowledged manifest_generation=1 manifest_jobs=1 total_sends=2 replay_extra_sends=0 bounded_close stats=%+v", sink, i, receipt.GetCommitted().GetEventId(), stats)
			}
		})
	}
}

type separatedDiagnosticWriter struct {
	mode              string
	entered, release  chan struct{}
	once, releaseOnce sync.Once
}

func (w *separatedDiagnosticWriter) Write(p []byte) (int, error) {
	if w.mode == "throwing" {
		panic("injected diagnostic sink panic")
	}
	if w.mode == "backpressured" {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return io.Discard.Write(p)
}
func (w *separatedDiagnosticWriter) unblock() { w.releaseOnce.Do(func() { close(w.release) }) }
