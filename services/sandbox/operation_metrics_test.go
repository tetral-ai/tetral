package tetralsandbox

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSandboxOperationDurationsObserveProviderWithoutLogSink(t *testing.T) {
	metrics := NewOperationMetrics()
	ctx := WithOperationMetrics(context.Background(), metrics)
	called := false
	value, err := observeProviderCall(ctx, nil, "sandbox.provider.inspect_execution", providerOperationIdentity{sessionID: "session-must-not-label"}, func() (int, error) { called = true; return 7, context.DeadlineExceeded })
	if !called || value != 7 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("changed provider outcome: %v %d %v", called, value, err)
	}
	body := metrics.Text()
	if !strings.Contains(body, `tetral_operation_duration_seconds_count{operation="sandbox.provider.inspect_execution",outcome="timeout",service="sandbox"} 1`) {
		t.Fatalf("missing actual provider boundary: %s", body)
	}
	if strings.Contains(body, "session-must-not-label") {
		t.Fatal("private identity became a label")
	}
}
