package integration

import (
	"testing"

	"google.golang.org/grpc"

	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

// The production sender owns retained native channels and their TLS reloader.
// Keep it alive through fixture work, then join its explicit owner at cleanup.
func fixtureRuntimeCommandClient(t *testing.T, tokenSource internalgrpcauth.TokenSource, options ...grpc.DialOption) *jobrunner.RuntimePodCommandClient {
	t.Helper()
	client := jobrunner.NewRuntimePodCommandClient(tokenSource, options...)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Runtime command channels: %v", err)
		}
	})
	return client
}
