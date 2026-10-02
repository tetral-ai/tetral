package main

import (
	"context"
	"fmt"
	"sync"
)

type sandboxWorkerID string
type sandboxWorkerClass uint8

const (
	workerConsumer sandboxWorkerClass = iota
	workerMaintenance
	workerListener
)
const (
	workerQueueNotifications     sandboxWorkerID = "queue_notifications"
	workerQueueOverLimit         sandboxWorkerID = "queue_over_limit"
	workerEnvironmentBuild       sandboxWorkerID = "environment_build"
	workerOutputCapture          sandboxWorkerID = "output_capture"
	workerOutputCaptureCleanup   sandboxWorkerID = "output_capture_cleanup"
	workerOutputCaptureSweep     sandboxWorkerID = "output_capture_sweep"
	workerToolExecution          sandboxWorkerID = "tool_execution"
	workerToolCancel             sandboxWorkerID = "tool_cancel"
	workerBackgroundReconcile    sandboxWorkerID = "background_reconcile"
	workerBackgroundCommand      sandboxWorkerID = "background_command"
	workerMemoryProjection       sandboxWorkerID = "memory_projection"
	workerActivation             sandboxWorkerID = "activation"
	workerMaterialization        sandboxWorkerID = "materialization"
	workerRelease                sandboxWorkerID = "release"
	workerEnvironmentReadyFanout sandboxWorkerID = "environment_ready_fanout"
	workerResourcePrefixGC       sandboxWorkerID = "resource_prefix_gc"
)

var sandboxWorkerCatalog = map[sandboxWorkerID]sandboxWorkerClass{
	workerQueueNotifications: workerListener,
	workerQueueOverLimit:     workerMaintenance, workerOutputCaptureSweep: workerMaintenance, workerResourcePrefixGC: workerMaintenance,
	workerEnvironmentBuild: workerConsumer, workerOutputCapture: workerConsumer, workerOutputCaptureCleanup: workerConsumer,
	workerToolExecution: workerConsumer, workerToolCancel: workerConsumer, workerBackgroundReconcile: workerConsumer,
	workerBackgroundCommand: workerConsumer, workerMemoryProjection: workerConsumer, workerActivation: workerConsumer,
	workerMaterialization: workerConsumer, workerRelease: workerConsumer, workerEnvironmentReadyFanout: workerConsumer,
}

// Every command-owned producer is registered before any starts. Missing,
// duplicate or unknown registrations fail startup; all launches share one join.
type sandboxWorkerRegistry struct {
	callbacks       map[sandboxWorkerID]func(context.Context)
	registrationErr error
	started         bool
}

func newSandboxWorkerRegistry() *sandboxWorkerRegistry {
	return &sandboxWorkerRegistry{callbacks: make(map[sandboxWorkerID]func(context.Context))}
}
func (r *sandboxWorkerRegistry) register(id sandboxWorkerID, run func(context.Context)) {
	if _, known := sandboxWorkerCatalog[id]; !known || run == nil || r.callbacks[id] != nil || r.started {
		r.registrationErr = fmt.Errorf("invalid Sandbox worker registration %q", id)
		return
	}
	r.callbacks[id] = run
}
func (r *sandboxWorkerRegistry) start(work, acquisition context.Context) (<-chan struct{}, error) {
	if r.registrationErr != nil {
		return nil, r.registrationErr
	}
	if r.started {
		return nil, fmt.Errorf("sandbox workers already started")
	}
	for id := range sandboxWorkerCatalog {
		if r.callbacks[id] == nil {
			return nil, fmt.Errorf("missing Sandbox worker %q", id)
		}
	}
	r.started = true
	var workers sync.WaitGroup
	for id, run := range r.callbacks {
		workerCtx := work
		if sandboxWorkerCatalog[id] == workerListener {
			workerCtx = acquisition
		}
		workers.Add(1)
		go func() { defer workers.Done(); run(workerCtx) }()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	return done, nil
}
