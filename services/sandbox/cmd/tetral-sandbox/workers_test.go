package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"

	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	sandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// This inventory binds each typed registration to its actual production caller,
// including the context argument. A new or escaped producer cannot disappear
// behind the shared registry's generic lifecycle tests.
func TestSandboxCommandWorkerRegistrationInventory(t *testing.T) {
	expected := map[string]string{
		"workerQueueNotifications": "notificationLoop", "workerQueueOverLimit": "overLimitLoop",
		"workerEnvironmentBuild": "EnvironmentBuildJobRunner", "workerOutputCapture": "SandboxOutputCaptureJobRunner",
		"workerOutputCaptureCleanup": "SandboxOutputCaptureCleanupRunner", "workerOutputCaptureSweep": "SweepExpiredCaptures",
		"workerToolExecution": "toolExecutionLoop", "workerToolCancel": "SandboxToolCancelJobRunner",
		"workerBackgroundReconcile": "SandboxBackgroundReconcileJobRunner", "workerBackgroundCommand": "SandboxBackgroundCommandJobRunner",
		"workerMemoryProjection": "SandboxMemoryProjectionJobRunner", "workerActivation": "SandboxActivationJobRunner",
		"workerMaterialization": "SandboxMaterializationJobRunner", "workerRelease": "SandboxReleaseJobRunner",
		"workerEnvironmentReadyFanout": "EnvironmentReadyFanoutJobRunner", "workerResourcePrefixGC": "ResourcePrefixGCRunner",
	}
	source, err := parser.ParseFile(token.NewFileSet(), "worker_assembly.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	protectedQueue := false
	ast.Inspect(source, func(node ast.Node) bool {
		if statement, ok := node.(*ast.GoStmt); ok {
			joined := false
			ast.Inspect(statement.Call, func(n ast.Node) bool {
				if callee, ok := n.(*ast.SelectorExpr); ok && callee.Sel.Name == "JoinSandboxWorkers" {
					joined = true
				}
				return true
			})
			if !joined {
				t.Fatal("command producer escaped registered worker join")
			}
		}
		if assignment, ok := node.(*ast.AssignStmt); ok {
			for i, lhs := range assignment.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name == "queueClient" && i < len(assignment.Rhs) {
					if call, ok := assignment.Rhs[i].(*ast.CallExpr); ok {
						if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "WithQueueAcquisition" {
							protectedQueue = true
						}
					}
				}
			}
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selector.Sel.Name != "register" {
			switch selector.Sel.Name {
			case "notificationLoop", "overLimitLoop", "workspaceLoop", "workspaceGroup", "toolExecutionLoop":
				t.Fatal("production worker launched outside typed registration")
			}
			return true
		}
		owner, ok := selector.X.(*ast.Ident)
		if !ok || owner.Name != "workers" {
			t.Fatal("worker registered outside owning registry")
		}
		if len(call.Args) != 2 {
			t.Fatal("invalid registration shape")
		}
		id, ok := call.Args[0].(*ast.Ident)
		if !ok {
			t.Fatal("worker ID is not typed")
		}
		symbol, known := expected[id.Name]
		if !known || seen[id.Name] {
			t.Fatalf("unknown/duplicate registration %s", id.Name)
		}
		seen[id.Name] = true
		callback, ok := call.Args[1].(*ast.FuncLit)
		if !ok || len(callback.Type.Params.List) != 1 || callback.Type.Params.List[0].Names[0].Name != "workerCtx" {
			t.Fatalf("%s does not receive owning context", id.Name)
		}
		found, contextOwned := false, false
		ast.Inspect(callback.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.GoStmt); ok {
				t.Fatal("registered production callback launched an unjoined goroutine")
			}
			s, ok := n.(*ast.SelectorExpr)
			if ok && s.Sel.Name == symbol {
				found = true
			}
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			s, ok = c.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch s.Sel.Name {
			case "notificationLoop", "overLimitLoop", "workspaceLoop", "workspaceGroup", "toolExecutionLoop":
				if len(c.Args) == 0 {
					t.Fatal("worker context missing")
				}
				arg, ok := c.Args[0].(*ast.Ident)
				if !ok || arg.Name != "workerCtx" {
					t.Fatalf("%s escaped work context", id.Name)
				}
				contextOwned = true
			}
			return true
		})
		if !found || !contextOwned {
			t.Fatalf("%s lost production callback %s/context", id.Name, symbol)
		}
		return false
	})
	mainSource, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mainSource), "tetralsandbox.WithQueueAcquisition(") || !strings.Contains(string(mainSource), "launchSandboxWorkers(") {
		t.Fatal("command lost acquisition fence or production launcher")
	}
	_ = protectedQueue
	if len(seen) != len(expected) || len(sandboxWorkerCatalog) != len(expected) {
		t.Fatalf("worker registration census=%d/%d/%d", len(seen), len(expected), len(sandboxWorkerCatalog))
	}
}

type registryWorkspaceLister struct{}

func (registryWorkspaceLister) ListIDs(context.Context) ([]workspace.ID, error) {
	return []workspace.ID{workspace.DefaultID}, nil
}
func registerOtherWorkers(registry *sandboxWorkerRegistry, selected sandboxWorkerID) {
	for id := range sandboxWorkerCatalog {
		if id != selected {
			registry.register(id, func(ctx context.Context) { <-ctx.Done() })
		}
	}
}

func TestSandboxRegisteredWorkersDrainAndJoin(t *testing.T) {
	for id, class := range sandboxWorkerCatalog {
		for _, force := range []bool{false, true} {
			t.Run(string(id)+"/force_"+strconv.FormatBool(force), func(t *testing.T) {
				acquire, quiesce := context.WithCancel(context.Background())
				defer quiesce()
				work, cancel := context.WithCancel(context.Background())
				defer cancel()
				work = sandbox.WithAcquisitionContext(work, acquire)
				registry := newSandboxWorkerRegistry()
				registerOtherWorkers(registry, id)
				entered, released := make(chan struct{}), make(chan struct{})
				var calls atomic.Int32
				callback := func(ctx context.Context) {
					calls.Add(1)
					close(entered)
					select {
					case <-released:
					case <-ctx.Done():
					}
					time.Sleep(10 * time.Millisecond)
				}
				registry.register(id, func(ctx context.Context) {
					if class == workerConsumer {
						pool, err := sandbox.NewWorkspaceConsumerPool(1)
						if err != nil {
							panic(err)
						}
						_ = sandbox.RunWorkspaceConsumerGroup(ctx, 3, pool, registryWorkspaceLister{}, time.Millisecond, func(cycle context.Context, _ workspace.ID) (bool, error) { callback(cycle); return true, nil }, nil, nil)
					} else {
						callback(ctx)
					}
				})
				done, err := registry.start(work, acquire)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("registered worker did not start")
				}
				quiesce()
				if work.Err() != nil {
					t.Fatal("admission stop cancelled active work")
				}
				// Idle workers still own work context; cancellation happens only at the one
				// drain cutoff. The listener alone receives immediate acquisition cancellation.
				if !force {
					close(released)
				}
				if err := sandbox.JoinSandboxWorkers(done, cancel, 40*time.Millisecond, time.Second); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 1 {
					t.Fatalf("slot contender acquired after quiesce: %d", calls.Load())
				}
			})
		}
	}
}

type registryLateQueue struct {
	sandbox.SandboxQueueClient
	entered, reply chan struct{}
	deferred       *queuev1.DeferRequest
	workCancelled  bool
}

func (q *registryLateQueue) Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	close(q.entered)
	<-q.reply
	return &queuev1.LeaseResponse{Jobs: []*queuev1.QueueJob{{Id: "late_job", WorkspaceId: "default", LeaseToken: "exact_lease"}}}, nil
}
func (q *registryLateQueue) Defer(ctx context.Context, r *queuev1.DeferRequest) (*queuev1.TransitionResponse, error) {
	q.deferred = r
	q.workCancelled = ctx.Err() != nil
	return &queuev1.TransitionResponse{Updated: true}, nil
}
func TestSandboxRegisteredConsumersReturnLateLease(t *testing.T) {
	for id, class := range sandboxWorkerCatalog {
		if class != workerConsumer {
			continue
		}
		t.Run(string(id), func(t *testing.T) {
			acquire, quiesce := context.WithCancel(context.Background())
			defer quiesce()
			work, cancel := context.WithCancel(context.Background())
			defer cancel()
			work = sandbox.WithAcquisitionContext(work, acquire)
			registry := newSandboxWorkerRegistry()
			registerOtherWorkers(registry, id)
			raw := &registryLateQueue{entered: make(chan struct{}), reply: make(chan struct{})}
			client := sandbox.WithQueueAcquisition(raw)
			result := make(chan error, 1)
			registry.register(id, func(ctx context.Context) {
				response, err := client.Lease(ctx, &queuev1.LeaseRequest{})
				if len(response.GetJobs()) != 0 || !errors.Is(err, context.Canceled) {
					result <- errors.New("late lease escaped admission")
				} else {
					result <- nil
				}
			})
			done, err := registry.start(work, acquire)
			if err != nil {
				t.Fatal(err)
			}
			<-raw.entered
			quiesce()
			close(raw.reply)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			if raw.deferred == nil || raw.deferred.GetWorkspaceId() != "default" || raw.deferred.GetJobId() != "late_job" || raw.deferred.GetLeaseToken() != "exact_lease" || raw.workCancelled {
				t.Fatalf("exact capability disposition=%v work_cancelled=%t", raw.deferred, raw.workCancelled)
			}
			cancel()
			<-done
		})
	}
}

func TestSandboxWorkerRegistryRejectsIncompleteStartup(t *testing.T) {
	registry := newSandboxWorkerRegistry()
	if _, err := registry.start(context.Background(), context.Background()); err == nil {
		t.Fatal("missing consumers admitted")
	}
	registry.register(workerToolExecution, func(context.Context) {})
	registry.register(workerToolExecution, func(context.Context) {})
	if _, err := registry.start(context.Background(), context.Background()); err == nil {
		t.Fatal("duplicate consumer admitted")
	}
}

func TestSandboxRegisteredWorkersProcessBoundary(t *testing.T) {
	if mode := os.Getenv("TETRAL_SANDBOX_REGISTRY_CHILD"); mode != "" {
		dir := os.Getenv("TETRAL_SANDBOX_REGISTRY_DIRECTORY")
		root, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = root.Close() }()
		err = workload.RunProcess(func(ctx context.Context) error {
			owner := workload.NewProcessLogger(os.Stderr, "sandbox", "test", "test", workload.DefaultDiagnosticConfig())
			defer owner.CloseWithBudget()
			workload.ConfigureProcessShutdown(ctx, 5*time.Second, owner)
			defer workload.ProcessCleanup(ctx, func() {
				if err := root.WriteFile("closed", nil, 0600); err != nil {
					panic(err)
				}
			})
			work, cancel := context.WithCancel(context.WithoutCancel(ctx))
			defer cancel()
			work = sandbox.WithAcquisitionContext(work, ctx)
			var started, finished atomic.Int32
			loops := controlledSandboxWorkerLoops(func(workerCtx context.Context, entry string) error {
				if started.Add(1) == 16 {
					if err := root.WriteFile("ready", nil, 0600); err != nil {
						panic(err)
					}
				}
				heldEntry := map[string]string{string(workerToolExecution): "toolExecutionLoop", string(workerQueueOverLimit): "overLimitLoop", string(workerQueueNotifications): "notificationLoop"}[mode]
				if entry == heldEntry {
					select {}
				}
				<-workerCtx.Done()
				time.Sleep(10 * time.Millisecond)
				finished.Add(1)
				return workerCtx.Err()
			})
			done, err := launchSandboxWorkers(work, ctx, sandboxWorkerDependencies{loops: loops})
			if err != nil {
				return err
			}
			<-ctx.Done()
			workload.BeginProcessShutdown(ctx)
			err = sandbox.JoinSandboxWorkers(done, cancel, 2*time.Second, 3*time.Second)
			if finished.Load() != 16 {
				return errors.New("dependencies closed before all registrations joined")
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, mode := range []string{string(workerToolExecution), string(workerQueueOverLimit), string(workerQueueNotifications), "cooperative"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSandboxRegisteredWorkersProcessBoundary$")
			child.Env = append(os.Environ(), "TETRAL_SANDBOX_REGISTRY_CHILD="+mode, "TETRAL_SANDBOX_REGISTRY_DIRECTORY="+dir)
			var output strings.Builder
			child.Stdout = &output
			child.Stderr = &output
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- child.Wait() }()
			t.Cleanup(func() { _ = child.Process.Kill() })
			for {
				if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
					break
				}
				select {
				case err := <-exited:
					t.Fatalf("child died before ready: %v %s", err, output.String())
				case <-ctx.Done():
					t.Fatal("child not ready")
				case <-time.After(10 * time.Millisecond):
				}
			}
			start := time.Now()
			if err := child.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			err := <-exited
			elapsed := time.Since(start)
			_, closedErr := os.Stat(filepath.Join(dir, "closed"))
			if mode == "cooperative" {
				if err != nil || closedErr != nil {
					t.Fatalf("cooperative command did not join+close: %v/%v/%s", err, closedErr, output.String())
				}
			} else {
				if err == nil || child.ProcessState.ExitCode() != 1 || !os.IsNotExist(closedErr) || elapsed < 4500*time.Millisecond || elapsed > 8*time.Second {
					t.Fatalf("noncooperative command boundary: elapsed=%s exit=%v closed=%v %s", elapsed, err, closedErr, output.String())
				}
			}
			if ctx.Err() != nil {
				t.Fatal("external watchdog killed command")
			}
			t.Logf("actual Sandbox registry pid=%d producer=%s elapsed=%s exit=%d", child.Process.Pid, mode, elapsed, child.ProcessState.ExitCode())
		})
	}
}

func controlledSandboxWorkerLoops(enter func(context.Context, string) error) sandboxWorkerLoops {
	return sandboxWorkerLoops{
		notificationLoop: func(ctx context.Context, _ queue.NotificationListener, _ string, _ *queue.WakeSignal, _ *slog.Logger) error {
			return enter(ctx, "notificationLoop")
		},
		overLimitLoop: func(ctx context.Context, _ *sandbox.SandboxQueueOverLimitReconciler, _ time.Duration) {
			_ = enter(ctx, "overLimitLoop")
		},
		workspaceLoop: func(ctx context.Context, _ sandbox.WorkspaceLister, _ time.Duration, _ sandbox.WorkspaceConsumer, _ *queue.WakeSignal, _ *slog.Logger) error {
			return enter(ctx, "workspaceLoop")
		},
		workspaceGroup: func(ctx context.Context, _ int, _ *sandbox.WorkspaceConsumerPool, _ sandbox.WorkspaceLister, _ time.Duration, _ sandbox.WorkspaceConsumer, _ *queue.WakeSignal, _ *slog.Logger) error {
			return enter(ctx, "workspaceGroup")
		},
		toolExecutionLoop: func(ctx context.Context, _ int, _ *sandbox.WorkspaceConsumerPool, _ sandbox.WorkspaceLister, _ time.Duration, _ sandbox.SandboxQueueClient, _ sandbox.SandboxExecutionCoordinator, _ *sandbox.ProviderRegistry, _ sandbox.SandboxMediaMaterializer, _ sandbox.SandboxToolExecutionRunnerConfig, _ *queue.WakeSignal, _ *slog.Logger) error {
			return enter(ctx, "toolExecutionLoop")
		},
	}
}

type assemblyWorkerIdentityKey struct{}

// Expected classes and entries are independent literal assertions. Tests execute
// the builder's actual production closures; only the owning loop entry is held.
func TestSandboxProductionWorkerAssemblyContextAndJoin(t *testing.T) {
	expected := map[sandboxWorkerID]struct {
		class sandboxWorkerClass
		entry string
	}{
		workerQueueNotifications: {workerListener, "notificationLoop"}, workerQueueOverLimit: {workerMaintenance, "overLimitLoop"},
		workerOutputCaptureSweep: {workerMaintenance, "workspaceLoop"}, workerResourcePrefixGC: {workerMaintenance, "workspaceLoop"},
		workerEnvironmentBuild: {workerConsumer, "workspaceLoop"}, workerEnvironmentReadyFanout: {workerConsumer, "workspaceLoop"},
		workerToolExecution: {workerConsumer, "toolExecutionLoop"}, workerOutputCapture: {workerConsumer, "workspaceGroup"},
		workerOutputCaptureCleanup: {workerConsumer, "workspaceGroup"}, workerToolCancel: {workerConsumer, "workspaceGroup"},
		workerBackgroundReconcile: {workerConsumer, "workspaceGroup"}, workerBackgroundCommand: {workerConsumer, "workspaceGroup"},
		workerMemoryProjection: {workerConsumer, "workspaceGroup"}, workerActivation: {workerConsumer, "workspaceGroup"},
		workerMaterialization: {workerConsumer, "workspaceGroup"}, workerRelease: {workerConsumer, "workspaceGroup"},
	}
	for _, force := range []bool{false, true} {
		t.Run(strconv.FormatBool(force), func(t *testing.T) {
			acquire, quiesce := context.WithCancel(context.Background())
			defer quiesce()
			work, cancel := context.WithCancel(context.Background())
			defer cancel()
			work = sandbox.WithAcquisitionContext(work, acquire)
			type observed struct {
				id    sandboxWorkerID
				entry string
				ctx   context.Context
			}
			observedEntries := make(chan observed, 16)
			release := make(chan struct{})
			registry := buildSandboxWorkers(sandboxWorkerDependencies{loops: controlledSandboxWorkerLoops(func(ctx context.Context, entry string) error {
				id, _ := ctx.Value(assemblyWorkerIdentityKey{}).(sandboxWorkerID)
				observedEntries <- observed{id, entry, ctx}
				select {
				case <-release:
				case <-ctx.Done():
				}
				time.Sleep(10 * time.Millisecond)
				return nil
			})})
			if len(registry.callbacks) != 16 {
				t.Fatal("actual production builder omitted a registration")
			}
			for id, original := range registry.callbacks {
				registry.callbacks[id] = func(ctx context.Context) { original(context.WithValue(ctx, assemblyWorkerIdentityKey{}, id)) }
			}
			done, err := registry.start(work, acquire)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[sandboxWorkerID]context.Context{}
			for range 16 {
				select {
				case observed := <-observedEntries:
					contract, known := expected[observed.id]
					if !known || contract.entry != observed.entry || sandboxWorkerCatalog[observed.id] != contract.class {
						t.Fatalf("actual production wiring changed:%s/%s", observed.id, observed.entry)
					}
					seen[observed.id] = observed.ctx
				case <-time.After(time.Second):
					t.Fatal("actual production closure did not reach owning loop")
				}
			}
			quiesce()
			for id, ctx := range seen {
				wantCancelled := expected[id].class == workerListener
				if (ctx.Err() != nil) != wantCancelled {
					t.Fatalf("actual closure context ownership changed: %s cancelled=%t", id, ctx.Err() != nil)
				}
			}
			if !force {
				close(release)
			}
			joined := make(chan error, 1)
			go func() { joined <- sandbox.JoinSandboxWorkers(done, cancel, 40*time.Millisecond, time.Second) }()
			if err := <-joined; err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			default:
				t.Fatal("dependencies could close before actual closures joined")
			}
		})
	}
}

func TestSandboxWorkerLoopDefaultsUseProductionOwners(t *testing.T) {
	loops := defaultSandboxWorkerLoops()
	for _, pair := range []struct {
		name         string
		actual, want any
	}{
		{"notificationLoop", loops.notificationLoop, queue.RunNotificationListener},
		{"overLimitLoop", loops.overLimitLoop, sandbox.RunSandboxQueueOverLimitLoop},
		{"workspaceLoop", loops.workspaceLoop, sandbox.RunWorkspaceConsumerLoop},
		{"workspaceGroup", loops.workspaceGroup, sandbox.RunWorkspaceConsumerGroup},
		{"toolExecutionLoop", loops.toolExecutionLoop, sandbox.RunSandboxToolExecutionConsumerGroup},
	} {
		if reflect.ValueOf(pair.actual).Pointer() != reflect.ValueOf(pair.want).Pointer() {
			t.Fatalf("command %s lost production loop owner", pair.name)
		}
	}
}
