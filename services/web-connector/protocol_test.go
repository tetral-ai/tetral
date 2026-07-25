package webconnector

import (
	"testing"

	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

func TestRunWebResponseCarriesCompletePerCallUsage(t *testing.T) {
	status := int32(204)
	response := &providergatewayv1.RunWebResponse{
		Usage: &providergatewayv1.WebUsage{
			Operation:         "mixed",
			BackendTokens:     42,
			TargetHttpStatus:  &status,
			StoredBytes:       512,
			DurationMs:        7,
			WebSearchRequests: 2,
			WebFetchRequests:  1,
		},
	}

	usage := response.GetUsage()
	if usage.GetOperation() != "mixed" ||
		usage.GetBackendTokens() != 42 ||
		usage.GetTargetHttpStatus() != 204 ||
		usage.GetStoredBytes() != 512 ||
		usage.GetDurationMs() != 7 ||
		usage.GetWebSearchRequests() != 2 ||
		usage.GetWebFetchRequests() != 1 {
		t.Fatalf("usage = %#v; want complete per-call accounting", usage)
	}
}
