package agentruntimebridge

import (
	"context"
	"database/sql"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

type bridgeRuntimeMemoryStore struct {
	MemoryStoreID string  `json:"memoryStoreId"`
	Name          string  `json:"name"`
	Access        string  `json:"access"`
	Instructions  *string `json:"instructions"`
}

func bridgeRuntimeMemoryStoresTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
) ([]bridgeRuntimeMemoryStore, error) {
	rows, err := tx.Query(ctx,
		`SELECT smr.memory_store_id,
		        smr.name,
		        smr.access,
		        NULLIF(smr.instructions, '')
		   FROM session_memory_store_resources smr
		   JOIN session_resources sr
		     ON sr.workspace_id = smr.workspace_id
		    AND sr.session_id = smr.session_id
		    AND sr.resource_id = smr.resource_id
		    AND sr.type = 'memory_store'
		    AND sr.detached_at IS NULL
		    AND sr.delete_requested_at IS NULL
		  WHERE smr.workspace_id = $1
		    AND smr.session_id = $2
		  ORDER BY smr.resource_id ASC`,
		workspaceID,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	memoryStores := make([]bridgeRuntimeMemoryStore, 0)
	for rows.Next() {
		var memoryStore bridgeRuntimeMemoryStore
		var instructions sql.NullString
		if err := rows.Scan(&memoryStore.MemoryStoreID, &memoryStore.Name, &memoryStore.Access, &instructions); err != nil {
			return nil, err
		}
		if instructions.Valid {
			memoryStore.Instructions = &instructions.String
		}
		memoryStores = append(memoryStores, memoryStore)
	}
	return memoryStores, rows.Err()
}
