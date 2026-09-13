package storage

// Discovery attempts belong to a durable input, so lease replay cannot reset
// their count or wall-clock deadline. Existing inbox rows start with no attempt.
// This additive migration preserves the immutable baseline and existing RLS.
func postgresqlMCPDiscoverySteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"add_mcp_discovery_budget", `ALTER TABLE session_runtime_inbox
		ADD COLUMN mcp_discovery_attempts INTEGER NOT NULL DEFAULT 0,
		ADD COLUMN mcp_discovery_deadline_at TIMESTAMPTZ,
		ADD COLUMN mcp_discovery_diagnostic TEXT,
		ADD CONSTRAINT session_runtime_inbox_mcp_discovery_budget_shape CHECK (
			(mcp_discovery_attempts = 0 AND mcp_discovery_deadline_at IS NULL)
			OR (mcp_discovery_attempts > 0 AND mcp_discovery_deadline_at IS NOT NULL)
		),
		ADD CONSTRAINT session_runtime_inbox_mcp_discovery_diagnostic_shape CHECK (
			mcp_discovery_diagnostic IS NULL OR mcp_discovery_diagnostic IN (
				'credential_unavailable', 'discovery_unavailable', 'manifest_invalid', 'internal'
			)
		)`},
	}
}
