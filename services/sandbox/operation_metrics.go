package tetralsandbox

import (
	"context"

	"github.com/tetral-ai/tetral/internal/workload"
)

type operationMetricsContextKey struct{}

// NewOperationMetrics fixes the provider and materialization operation domain.
// Provider IDs, Queue identities and SDK error text never become labels.
func NewOperationMetrics() *workload.OperationMetrics {
	return workload.NewOperationMetrics("sandbox",
		"sandbox.materialization.base_directories",
		"sandbox.materialization.claim",
		"sandbox.materialization.complete",
		"sandbox.materialization.credential_mint",
		"sandbox.materialization.deleted_file_cleanup",
		"sandbox.materialization.fail",
		"sandbox.materialization.file_staging",
		"sandbox.materialization.helper_health",
		"sandbox.materialization.memory_projection",
		"sandbox.materialization.mount_bind_verify",
		"sandbox.materialization.mount_cleanup",
		"sandbox.materialization.mount_probe",
		"sandbox.materialization.repository_cleanup",
		"sandbox.materialization.repository_clone",
		"sandbox.materialization.skills",
		"sandbox.materialization.skills_cleanup",
		"sandbox.materialization.wait_activation",
		"sandbox.provider.build_artifact",
		"sandbox.provider.cancel_background",
		"sandbox.provider.capture_outputs",
		"sandbox.provider.create",
		"sandbox.provider.execute_tool",
		"sandbox.provider.inspect_execution",
		"sandbox.provider.inspect_release",
		"sandbox.provider.observe_tool",
		"sandbox.provider.operation_completed",
		"sandbox.provider.poll_background",
		"sandbox.provider.prepare_tool",
		"sandbox.provider.refresh_memory_projection",
		"sandbox.provider.release",
		"sandbox.provider.resolve_activation",
		"sandbox.provider.send_background_input",
		"sandbox.provider.start",
		"shutdown_workers_drain", "shutdown_workers_cancel_join",
	)
}

// WithOperationMetrics binds the process registry to admitted work contexts.
func WithOperationMetrics(ctx context.Context, metrics *workload.OperationMetrics) context.Context {
	return context.WithValue(ctx, operationMetricsContextKey{}, metrics)
}
func operationMetrics(ctx context.Context) *workload.OperationMetrics {
	metrics, _ := ctx.Value(operationMetricsContextKey{}).(*workload.OperationMetrics)
	return metrics
}
