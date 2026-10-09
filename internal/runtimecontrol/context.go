package runtimecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const loadOpenDurableTurnIDSQL = `WITH latest_running AS MATERIALIZED (
		SELECT event_id, sequence
		  FROM session_events
		 WHERE workspace_id=$1
		   AND session_id=$2
		   AND session_thread_id=$3
		   AND type IN ('session.status_running', 'session.thread_status_running')
		 ORDER BY sequence DESC
		 LIMIT 1
	)
	SELECT running.event_id
	  FROM latest_running running
	 WHERE NOT EXISTS (
	       SELECT 1
	         FROM session_events closeout
	        WHERE closeout.workspace_id=$1
	          AND closeout.session_id=$2
	          AND closeout.session_thread_id=$3
	          AND closeout.type IN (
	            'session.status_idle',
	            'session.thread_status_idle',
	            'session.status_terminated',
	            'session.thread_status_terminated'
	          )
	          AND closeout.sequence > (SELECT sequence FROM latest_running)
	        ORDER BY closeout.sequence ASC
	        LIMIT 1
	      )`

func LoadOpenDurableTurnIDTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) (*string, error) {
	var durableTurnID string
	err := tx.QueryRow(ctx,
		loadOpenDurableTurnIDSQL,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&durableTurnID)
	if dbconnect.IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &durableTurnID, nil
}

func Sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
