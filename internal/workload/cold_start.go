package workload

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// processStarted is the earliest instant this package can observe; a service
// pod's cold start counts from it.
var processStarted = time.Now()

// ColdStart is one start of a kind of unit (a service pod, a Session computer,
// a thread) whose phases follow one another. Each phase records
// cold_start.phase_started and cold_start.phase_completed; the start records
// one cold_start.completed and observes cold_start_phase_seconds and
// cold_start_seconds, whose engine scope leaves out the phases that the
// cluster, a Controller or a model provider spends.
type ColdStart struct {
	mu           sync.Mutex
	telemetry    *Telemetry
	kind         string
	startKind    string
	phases       []string
	external     map[string]bool
	started      time.Time
	phaseStarted time.Time
	current      int
	durations    []time.Duration
	finished     bool
}

// BeginServiceColdStart starts this process's service_pod cold start, dated
// from process start, in its config phase. The service advances it once its
// configuration is loaded and once its dependencies are connected; its
// readiness first passing completes it.
func BeginServiceColdStart(ctx context.Context) *ColdStart {
	c := beginColdStartAt(ctx, CurrentTelemetry(), processStarted, "service_pod", "new", []string{"config", "dependencies", "ready"})
	processColdStart.Store(c)
	return c
}

// processColdStart is the service_pod cold start that readiness completes.
var processColdStart atomic.Pointer[ColdStart]

// failProcessColdStart ends the service_pod cold start with the startup
// failure's error code.
func failProcessColdStart(code string) {
	if c := processColdStart.Swap(nil); c != nil {
		c.Fail(context.Background(), code)
	}
}

// MarkProcessReady completes the service_pod cold start for a service that
// keeps its own readiness instead of a Readiness.
func MarkProcessReady() { completeProcessColdStart() }

func completeProcessColdStart() {
	if c := processColdStart.Swap(nil); c != nil {
		c.Complete(context.Background())
	}
}

// BeginColdStart starts a cold start of kind now, in its first phase.
func BeginColdStart(ctx context.Context, kind, startKind string, phases ...string) *ColdStart {
	return beginColdStartAt(ctx, CurrentTelemetry(), time.Now(), kind, startKind, phases)
}

func beginColdStartAt(ctx context.Context, t *Telemetry, started time.Time, kind, startKind string, phases []string) *ColdStart {
	c := &ColdStart{telemetry: t, kind: kind, startKind: startKind, phases: phases, external: map[string]bool{}, started: started, phaseStarted: started, durations: make([]time.Duration, len(phases))}
	c.recordPhaseStarted(ctx)
	return c
}

// External marks phases spent outside Engine, which cold_start_seconds
// leaves out of its engine scope.
func (c *ColdStart) External(phases ...string) *ColdStart {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, phase := range phases {
		c.external[phase] = true
	}
	return c
}

// Phase names the phase in progress, or "" once the cold start ended.
func (c *ColdStart) Phase() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished || c.current >= len(c.phases) {
		return ""
	}
	return c.phases[c.current]
}

// Advance completes the current phase and starts the next one; after the
// last phase it completes the cold start.
func (c *ColdStart) Advance(ctx context.Context) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	c.completePhase(ctx)
	if c.current == len(c.phases) {
		c.complete(ctx, "ok")
		return
	}
	c.recordPhaseStarted(ctx)
}

// Complete completes the current phase and every later one at this instant
// and records the cold start as ok.
func (c *ColdStart) Complete(ctx context.Context) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	for c.current < len(c.phases) {
		c.completePhase(ctx)
		if c.current < len(c.phases) {
			c.recordPhaseStarted(ctx)
		}
	}
	c.complete(ctx, "ok")
}

// Fail records the cold start as ended by code; it observes no histogram.
func (c *ColdStart) Fail(ctx context.Context, code string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	c.durations[min(c.current, len(c.phases)-1)] = time.Since(c.phaseStarted)
	c.complete(ctx, code)
}

func (c *ColdStart) completePhase(ctx context.Context) {
	now := time.Now()
	elapsed := now.Sub(c.phaseStarted)
	c.durations[c.current] = elapsed
	phase := c.phases[c.current]
	c.telemetry.logger.LogAttrs(ctx, slog.LevelInfo, "cold_start.phase_completed",
		slog.String("cold_start.kind", c.kind), slog.String("cold_start.phase", phase), slog.Int64(phase+"_ms", elapsed.Milliseconds()))
	c.current++
	c.phaseStarted = now
}

func (c *ColdStart) recordPhaseStarted(ctx context.Context) {
	c.telemetry.logger.LogAttrs(ctx, slog.LevelInfo, "cold_start.phase_started",
		slog.String("cold_start.kind", c.kind), slog.String("cold_start.phase", c.phases[c.current]))
}

func (c *ColdStart) complete(ctx context.Context, outcome string) {
	c.finished = true
	attrs := []slog.Attr{slog.String("cold_start.kind", c.kind), slog.String("cold_start.start_kind", c.startKind), slog.String("outcome", outcome)}
	var total, engine time.Duration
	for i, phase := range c.phases {
		attrs = append(attrs, slog.Int64(phase+"_ms", c.durations[i].Milliseconds()))
		total += c.durations[i]
		if !c.external[phase] {
			engine += c.durations[i]
		}
	}
	c.telemetry.logger.LogAttrs(ctx, slog.LevelInfo, "cold_start.completed", attrs...)
	if outcome != "ok" {
		return
	}
	service := c.telemetry.service
	for i, phase := range c.phases {
		c.telemetry.phases.Observe(c.durations[i].Seconds(), service, c.kind, phase)
	}
	c.telemetry.coldStarts.Observe(total.Seconds(), service, c.kind, "total")
	c.telemetry.coldStarts.Observe(engine.Seconds(), service, c.kind, "engine")
}
