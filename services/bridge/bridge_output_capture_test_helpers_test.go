package agentruntimebridge

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func settleOutputCaptureGenerationForTest(db *sql.DB, sessionID string, writeID string, generation int, state string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var current string
		err := db.QueryRow(`SELECT state FROM sandbox_output_capture_operations
			WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=$3`,
			sessionID, writeID, generation,
		).Scan(&current)
		if err == nil && (current == "pending" || current == "running") {
			digest := strings.Repeat("a", 64)
			if state == "failed" {
				_, err = db.Exec(`UPDATE sandbox_output_capture_operations SET state='failed',
					failure_kind='capture_test_failure', failure_detail='capture test failure',
					outcome_state='failed', outcome_digest=$4, updated_at=clock_timestamp()
					WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=$3`,
					sessionID, writeID, generation, digest,
				)
			} else {
				_, err = db.Exec(`UPDATE sandbox_output_capture_operations SET state='staged',
					outcome_state='staged', outcome_digest=$4, staged_at=clock_timestamp(), updated_at=clock_timestamp()
					WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=$3`,
					sessionID, writeID, generation, digest,
				)
			}
			return err
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("capture generation was not created")
}

func finishIdleWithStagedCaptureForTest(
	t *testing.T,
	db *sql.DB,
	store *PostgreSQLBridgeAPIStore,
	request *bridgev1.FinishIdleRequest,
) (*bridgev1.FinishIdleResponse, error) {
	t.Helper()
	settled := make(chan error, 1)
	go func() {
		settled <- settleOutputCaptureGenerationForTest(
			db,
			request.GetScope().GetSessionId(),
			request.GetDurableTurnId(),
			1,
			"staged",
		)
	}()
	response, err := store.FinishIdle(context.Background(), request)
	if settleErr := <-settled; settleErr != nil {
		t.Fatalf("stage output capture: %v", settleErr)
	}
	return response, err
}
