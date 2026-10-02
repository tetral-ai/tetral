package agentruntimebridge

import (
	"context"
	"testing"

	"github.com/tetral-ai/tetral/internal/queue"
)

func mustLeaseBridgeQueueJob(t *testing.T, store *queue.PostgreSQLQueueStore, request queue.LeaseRequest) *queue.Job {
	t.Helper()
	jobs, err := store.Lease(context.Background(), request)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lease Queue job = %#v/%v; want exactly one", jobs, err)
	}
	return jobs[0]
}
