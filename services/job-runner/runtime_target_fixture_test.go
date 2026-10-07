package jobrunner

import (
	"database/sql"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/services/job-runner/jobrunnertest"
)

// fixtureRuntimeTargetResolver is the production process-aware resolver over
// Kubernetes observations derived from the test's committed bindings. Bound
// Pods are visible, so delivery still requires the bound process to be current
// and accepting; placement probes fail, so an unbound Session stays unplaced.
// observer must be a pool other than the store's own, normally the admin pool.
func fixtureRuntimeTargetResolver(observer *sql.DB) KubernetesRuntimeTargetResolver {
	visibility := jobrunnertest.NewBindingVisibility(observer)
	return KubernetesRuntimeTargetResolver{Snapshot: visibility.Snapshot, GetPod: visibility.GetPod, LoadClient: jobrunnertest.UnavailableLoadClient()}
}

func fixtureRuntimeDeliveryStore(client *dbconnect.Client, observer *sql.DB, port int) *PostgreSQLRuntimeDeliveryStore {
	return NewPostgreSQLRuntimeDeliveryStore(client, port, fixtureRuntimeTargetResolver(observer))
}
