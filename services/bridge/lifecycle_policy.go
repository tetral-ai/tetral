package agentruntimebridge

import (
	"context"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/workload"
)

// Bridge owns server phase limits. Runtime owns each enclosing RPC attempt;
// an earlier caller deadline always bounds these phases and does not undo an
// already committed admission or durable external operation.
type BridgeLifecyclePolicy struct {
	DrainTimeout, CancelJoinTimeout, AdmissionTimeout, ReleaseTimeout                time.Duration
	SandboxResultWait, BackgroundResultWait, MemoryProjectionWait, OutputCaptureWait time.Duration
}

func DefaultBridgeLifecyclePolicy() BridgeLifecyclePolicy {
	const phaseWait = 30 * time.Second
	return BridgeLifecyclePolicy{40 * time.Second, 5 * time.Second, 3 * time.Second, 5 * time.Second, phaseWait, phaseWait, phaseWait, phaseWait}
}
func BridgeLifecyclePolicyFromEnv(getenv func(string) string) (BridgeLifecyclePolicy, error) {
	p := DefaultBridgeLifecyclePolicy()
	fields := map[string]*time.Duration{
		"TETRAL_DRAIN_TIMEOUT_MS": &p.DrainTimeout, "TETRAL_CANCEL_JOIN_TIMEOUT_MS": &p.CancelJoinTimeout,
		"TETRAL_BRIDGE_ADMISSION_TIMEOUT_MS": &p.AdmissionTimeout, "TETRAL_BRIDGE_RELEASE_RUNTIME_BINDING_TIMEOUT_MS": &p.ReleaseTimeout,
		"TETRAL_BRIDGE_SANDBOX_RESULT_WAIT_TIMEOUT_MS": &p.SandboxResultWait, "TETRAL_BRIDGE_BACKGROUND_RESULT_WAIT_TIMEOUT_MS": &p.BackgroundResultWait,
		"TETRAL_BRIDGE_MEMORY_PROJECTION_WAIT_TIMEOUT_MS": &p.MemoryProjectionWait, "TETRAL_BRIDGE_OUTPUT_CAPTURE_WAIT_TIMEOUT_MS": &p.OutputCaptureWait,
	}
	for key, value := range fields {
		raw := strings.TrimSpace(getenv(key))
		if raw == "" {
			continue
		}
		ms, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || ms <= 0 || ms > int64((1<<63-1)/time.Millisecond) {
			return BridgeLifecyclePolicy{}, workload.NewConfigError(key + " must be a positive millisecond duration")
		}
		*value = time.Duration(ms) * time.Millisecond
	}
	if err := p.Validate(); err != nil {
		return BridgeLifecyclePolicy{}, err
	}
	return p, nil
}

// Validate applies the same phase constraints to environment-derived and in-process
// policies: every phase is positive, admission and release attempts fit inside the
// drain, and drain plus cancellation join fit the 50-second application allocation.
func (p BridgeLifecyclePolicy) Validate() error {
	for _, phase := range []time.Duration{p.DrainTimeout, p.CancelJoinTimeout, p.AdmissionTimeout, p.ReleaseTimeout, p.SandboxResultWait, p.BackgroundResultWait, p.MemoryProjectionWait, p.OutputCaptureWait} {
		if phase <= 0 {
			return workload.NewConfigError("Bridge lifecycle phases must be positive durations")
		}
	}
	if p.AdmissionTimeout >= p.DrainTimeout || p.ReleaseTimeout >= p.DrainTimeout {
		return workload.NewConfigError("Bridge admission and release attempts must fit within drain timeout")
	}
	if p.DrainTimeout > 50*time.Second || p.CancelJoinTimeout > 50*time.Second-p.DrainTimeout {
		return workload.NewConfigError("drain and cancellation join exceed the Pod application shutdown allocation")
	}
	return nil
}

// lifecyclePolicy returns the store's validated phase policy. An invalid policy fails
// the RPC closed instead of substituting defaults for a partially configured store.
func (s *PostgreSQLBridgeAPIStore) lifecyclePolicy() (BridgeLifecyclePolicy, error) {
	if err := s.LifecyclePolicy.Validate(); err != nil {
		return BridgeLifecyclePolicy{}, status.Error(codes.FailedPrecondition, "bridge lifecycle policy is unavailable")
	}
	return s.LifecyclePolicy, nil
}
func bridgeContextError(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	return err
}
