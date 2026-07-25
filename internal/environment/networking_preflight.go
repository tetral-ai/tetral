package environment

import (
	"context"
	"encoding/json"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// LegacyNetworkingConfigError blocks startup until retired networking rows
// are remediated explicitly by an operator.
type LegacyNetworkingConfigError struct{}

func (*LegacyNetworkingConfigError) Error() string {
	return "environment networking preflight failed: retired networking rows require operator remediation"
}

// VerifyNetworkingConfigRows fails closed when durable environments contain a
// networking shape that this Engine version cannot interpret.
func VerifyNetworkingConfigRows(ctx context.Context, client *dbconnect.Client) error {
	if client == nil {
		return &LegacyNetworkingConfigError{}
	}
	rows, err := client.Query(ctx, "environment.networking_preflight_workspaces",
		`SELECT id FROM workspaces ORDER BY id`,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	workspaceIDs := make([]string, 0)
	for rows.Next() {
		var workspaceID string
		if err := rows.Scan(&workspaceID); err != nil {
			return err
		}
		workspaceIDs = append(workspaceIDs, workspaceID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, workspaceID := range workspaceIDs {
		var configRows []json.RawMessage
		err := client.WithWorkspaceReadOnlyTx(ctx, workspaceID, "environment.networking_preflight", func(tx *dbconnect.Tx) error {
			rows, err := tx.QueryRows(ctx,
				`SELECT config_json
				   FROM environments
				  WHERE workspace_id = $1
				  ORDER BY id`,
				workspaceID,
			)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var raw []byte
				if err := rows.Scan(&raw); err != nil {
					return err
				}
				configRows = append(configRows, append(json.RawMessage(nil), raw...))
			}
			return rows.Err()
		})
		if err != nil {
			return err
		}
		for _, raw := range configRows {
			if _, err := decodeCreateConfig(raw); err != nil {
				return &LegacyNetworkingConfigError{}
			}
		}
	}
	return nil
}
