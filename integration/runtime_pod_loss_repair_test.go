package integration

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func runtimePodLossSweepStore(t *testing.T, runtime *sql.DB, logs *bytes.Buffer, snapshot func() enginekubernetes.BindingVisibilitySnapshot) *jobrunner.PostgreSQLRuntimeDeliveryStore {
	clock := func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: fixtureRuntimeLoadClient(t), Snapshot: snapshot, Clock: clock, GetPod: fixtureConfirmedMissingRuntimePod})
	store.Clock = clock
	if logs != nil {
		store.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	}
	return store
}

// Callers use this only after killing their Runtime fixture or explicitly
// declaring the old Pod absent. A cache tombstone by itself is not loss proof.
func fixtureConfirmedMissingRuntimePod(_ context.Context, _, name string) (*enginekubernetes.PodObservation, error) {
	return &enginekubernetes.PodObservation{Absent: true}, nil
}
