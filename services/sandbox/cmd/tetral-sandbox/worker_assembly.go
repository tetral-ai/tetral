package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// Loop entries are owning dependencies. The production builder below is also
// used by command tests, which hold these boundaries without replacing its
// actual registrations or business runner closures.
type sandboxWorkerLoops struct {
	notificationLoop  func(context.Context, queue.NotificationListener, string, *queue.WakeSignal, *slog.Logger) error
	overLimitLoop     func(context.Context, *tetralsandbox.SandboxQueueOverLimitReconciler, time.Duration)
	workspaceLoop     func(context.Context, tetralsandbox.WorkspaceLister, time.Duration, tetralsandbox.WorkspaceConsumer, *queue.WakeSignal, *slog.Logger) error
	workspaceGroup    func(context.Context, int, *tetralsandbox.WorkspaceConsumerPool, tetralsandbox.WorkspaceLister, time.Duration, tetralsandbox.WorkspaceConsumer, *queue.WakeSignal, *slog.Logger) error
	toolExecutionLoop func(context.Context, int, *tetralsandbox.WorkspaceConsumerPool, tetralsandbox.WorkspaceLister, time.Duration, tetralsandbox.SandboxQueueClient, tetralsandbox.SandboxExecutionCoordinator, *tetralsandbox.ProviderRegistry, tetralsandbox.SandboxMediaMaterializer, tetralsandbox.SandboxToolExecutionRunnerConfig, *queue.WakeSignal, *slog.Logger) error
}

func defaultSandboxWorkerLoops() sandboxWorkerLoops {
	return sandboxWorkerLoops{
		notificationLoop: queue.RunNotificationListener, overLimitLoop: tetralsandbox.RunSandboxQueueOverLimitLoop,
		workspaceLoop: tetralsandbox.RunWorkspaceConsumerLoop, workspaceGroup: tetralsandbox.RunWorkspaceConsumerGroup,
		toolExecutionLoop: tetralsandbox.RunSandboxToolExecutionConsumerGroup,
	}
}

type sandboxWorkerDependencies struct {
	cfg                    tetralsandbox.Config
	loops                  sandboxWorkerLoops
	queueClient            tetralsandbox.SandboxQueueClient
	queueStore             *queue.PostgreSQLQueueStore
	workspaceStore         tetralsandbox.WorkspaceLister
	client                 *dbconnect.Client
	providerAdapter        *tetralsandbox.DaytonaAdapter
	providerRegistry       *tetralsandbox.ProviderRegistry
	executionCoordinator   *tetralsandbox.PostgreSQLSandboxExecutionCoordinator
	mediaMaterializer      *tetralsandbox.PostgreSQLSandboxMediaMaterializer
	lifecycleStore         *tetralsandbox.PostgreSQLSandboxLifecycleStore
	backgroundCommandStore *tetralsandbox.PostgreSQLSandboxBackgroundCommandStore
	memoryProjectionStore  *tetralsandbox.PostgreSQLSandboxMemoryProjectionStore
	outputCaptureStore     *tetralsandbox.PostgreSQLSandboxOutputCaptureStore
	overLimitFinalizer     *tetralsandbox.PostgreSQLSandboxQueueOverLimitFinalizer
	environmentStore       *tetralsandbox.EnvironmentArtifactStore
	workerPool             *tetralsandbox.WorkspaceConsumerPool
	queueWake              *queue.WakeSignal
	logger                 *slog.Logger
}

func launchSandboxWorkers(work, acquisition context.Context, d sandboxWorkerDependencies) (<-chan struct{}, error) {
	return buildSandboxWorkers(d).start(work, acquisition)
}
func buildSandboxWorkers(d sandboxWorkerDependencies) *sandboxWorkerRegistry {
	cfg, loops := d.cfg, d.loops
	queueClient, queueStore, workspaceStore, client := d.queueClient, d.queueStore, d.workspaceStore, d.client
	providerAdapter, providerRegistry := d.providerAdapter, d.providerRegistry
	executionCoordinator, mediaMaterializer := d.executionCoordinator, d.mediaMaterializer
	lifecycleStore, backgroundCommandStore, memoryProjectionStore := d.lifecycleStore, d.backgroundCommandStore, d.memoryProjectionStore
	outputCaptureStore, overLimitFinalizer, environmentStore := d.outputCaptureStore, d.overLimitFinalizer, d.environmentStore
	workerPool, queueWake, logger := d.workerPool, d.queueWake, d.logger
	workers := newSandboxWorkerRegistry()
	workers.register(workerQueueNotifications, func(workerCtx context.Context) {
		_ = loops.notificationLoop(workerCtx, queue.PostgreSQLNotificationListener{Client: client}, queue.ConsumerClassSandbox, queueWake, logger)
	})
	workers.register(workerQueueOverLimit, func(workerCtx context.Context) {
		loops.overLimitLoop(workerCtx, &tetralsandbox.SandboxQueueOverLimitReconciler{
			Queue: queueStore, Finalizer: overLimitFinalizer,
		}, tetralsandbox.SandboxQueueOverLimitInterval)
	})
	workers.register(workerEnvironmentBuild, func(workerCtx context.Context) {
		_ = loops.workspaceLoop(workerCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.EnvironmentBuildJobRunner{
				Queue:     queueClient,
				Store:     environmentStore,
				Providers: providerRegistry,
				Logger:    logger,
				Config: tetralsandbox.EnvironmentRunnerConfig{
					BuildWarnAfter:    cfg.EnvironmentBuildWarnAfter,
					BuildTimeout:      cfg.EnvironmentBuildTimeout,
					WorkspaceID:       workspaceID.String(),
					LeaseOwner:        tetralsandbox.ServiceName,
					MaxJobs:           cfg.EnvironmentBuildConcurrency,
					LeaseDuration:     tetralsandbox.EnvironmentQueueLeaseDuration(cfg),
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerOutputCapture, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxOutputCaptureJobRunner{
				Queue: queueClient, Store: outputCaptureStore, Providers: providerRegistry, BlobStore: providerAdapter.BlobStore, Logger: logger,
				Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerOutputCaptureCleanup, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxOutputCaptureCleanupRunner{
				Queue: queueClient, Store: outputCaptureStore, BlobStore: providerAdapter.BlobStore,
				Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerOutputCaptureSweep, func(workerCtx context.Context) {
		_ = loops.workspaceLoop(workerCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			count, err := outputCaptureStore.SweepExpiredCaptures(cycleCtx, workspaceID.String(), storage.Now(), tetralsandbox.SandboxOutputCaptureCleanupBatchSize)
			return count > 0, err
		}, nil, logger)
	})
	workers.register(workerToolExecution, func(workerCtx context.Context) {
		_ = loops.toolExecutionLoop(
			workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval,
			queueClient, executionCoordinator, providerRegistry, mediaMaterializer,
			tetralsandbox.SandboxToolExecutionRunnerConfig{
				LeaseOwner: tetralsandbox.ServiceName, MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
				HeartbeatInterval: cfg.LeaseHeartbeatInterval, PreparationTimeout: cfg.ProviderCommandTimeout,
				LateCommandMargin: cfg.LateCommandMargin,
			},
			queueWake,
			logger,
		)
	})
	workers.register(workerToolCancel, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxToolCancelJobRunner{
				Queue: queueClient, Store: executionCoordinator, Providers: providerRegistry, Logger: logger,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerBackgroundReconcile, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxBackgroundReconcileJobRunner{
				Queue: queueClient, Store: backgroundCommandStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxBackgroundRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerBackgroundCommand, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxBackgroundCommandJobRunner{
				Queue: queueClient, Store: backgroundCommandStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxBackgroundRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerMemoryProjection, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxMemoryProjectionJobRunner{
				Queue: queueClient, Store: memoryProjectionStore, Providers: providerRegistry,
				Config: tetralsandbox.SandboxMemoryProjectionRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerActivation, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxActivationJobRunner{
				Queue: queueClient, Store: lifecycleStore, Providers: providerRegistry, Logger: logger,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerMaterialization, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxMaterializationJobRunner{
				Queue: queueClient, Store: lifecycleStore, Providers: providerRegistry, Logger: logger,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerRelease, func(workerCtx context.Context) {
		_ = loops.workspaceGroup(workerCtx, cfg.WorkerConcurrency, workerPool, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.SandboxReleaseJobRunner{
				Queue: queueClient, Store: lifecycleStore, Providers: providerRegistry, Logger: logger,
				Config: tetralsandbox.SandboxLifecycleRunnerConfig{
					WorkspaceID: workspaceID.String(), LeaseOwner: tetralsandbox.ServiceName,
					MaxJobs: 1, LeaseDuration: cfg.JobLeaseDuration,
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerEnvironmentReadyFanout, func(workerCtx context.Context) {
		_ = loops.workspaceLoop(workerCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			return (&tetralsandbox.EnvironmentReadyFanoutJobRunner{
				Queue:  queueClient,
				Store:  environmentStore,
				Logger: logger,
				Config: tetralsandbox.EnvironmentRunnerConfig{
					WorkspaceID:       workspaceID.String(),
					LeaseOwner:        tetralsandbox.ServiceName,
					MaxJobs:           cfg.EnvironmentReadyFanoutConcurrency,
					LeaseDuration:     tetralsandbox.EnvironmentQueueLeaseDuration(cfg),
					HeartbeatInterval: cfg.LeaseHeartbeatInterval,
				},
			}).RunOnceWithActivity(cycleCtx)
		}, queueWake, logger)
	})
	workers.register(workerResourcePrefixGC, func(workerCtx context.Context) {
		_ = loops.workspaceLoop(workerCtx, workspaceStore, cfg.JobPollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
			jobs, err := (&tetralsandbox.ResourcePrefixGCRunner{
				Client: client,
				Blobs:  providerAdapter.BlobStore,
				Config: tetralsandbox.ResourcePrefixGCRunnerConfig{
					WorkspaceID: workspaceID.String(),
					RetryAfter:  cfg.JobPollInterval,
				},
			}).RunOnce(cycleCtx)
			return len(jobs) > 0, err
		}, nil, logger)
	})

	return workers
}
