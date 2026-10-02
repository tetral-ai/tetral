package integration

import (
	"bytes"
	"database/sql"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func runtimePodLossSweepStore(runtime *sql.DB, logs *bytes.Buffer, snapshot func() enginekubernetes.BindingVisibilitySnapshot) *jobrunner.PostgreSQLRuntimeDeliveryStore {
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	store.TargetResolver = jobrunner.KubernetesRuntimeTargetResolver{Snapshot: snapshot, Clock: store.Clock}
	if logs != nil {
		store.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	}
	return store
}
