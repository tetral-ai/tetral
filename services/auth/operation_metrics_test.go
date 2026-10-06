package tetralauth

import (
	"context"
	"errors"
	"testing"

	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestAuthCheckMetricOutcomesDistinguishTypedDenialAndTransport(t *testing.T) {
	// A nil gRPC error on DeniedHttpResponse must never enter the success population.
	for _, tc := range []struct {
		name     string
		response any
		err      error
		want     string
	}{
		{"denied credentials", deniedExternalAuthorization(&auth.AuthenticationError{}, ""), nil, "rejected"},
		{"denied metadata", deniedExternalAuthorization(&auth.ValidationError{}, ""), nil, "rejected"},
		{"dependency failure", deniedExternalAuthorization(&auth.UnavailableError{}, ""), nil, "error"},
		{"malformed response", &authv3.CheckResponse{}, nil, "error"},
		{"peer failure", nil, status.Error(codes.Unavailable, "transport unavailable"), "error"},
		{"transport cancellation", nil, status.Error(codes.Canceled, "cancelled"), "cancelled"},
		{"wrapped deadline", nil, errors.Join(errors.New("check"), context.DeadlineExceeded), "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := externalAuthorizationOutcome(context.Background(), tc.response, tc.err); got != tc.want {
				t.Fatalf("outcome=%s want%s", got, tc.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := externalAuthorizationOutcome(ctx, deniedExternalAuthorization(errors.New("cancelled database wait"), ""), nil); got != "cancelled" {
		t.Fatalf("cancelled owner returned typed500: %s", got)
	}
}

func authOperationSample(t *testing.T, metrics *workload.OperationMetrics, name, operation, outcome string) float64 {
	t.Helper()
	samples, err := metrics.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range samples {
		labels := map[string]string{}
		for _, label := range sample.Labels {
			labels[label.Name] = label.Value
		}
		if sample.Name == name && labels["operation"] == operation && labels["outcome"] == outcome {
			return sample.Value
		}
	}
	return 0
}
