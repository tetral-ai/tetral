package storage

// Build timing belongs to the artifact, not to a worker invocation. Nullable
// columns leave terminal history untouched; the first live claim initializes
// the policy once. Provider progress survives while Queue custody is released.
func postgresqlEnvironmentBuildSteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"add_environment_build_observation", `ALTER TABLE environment_artifacts
		ADD COLUMN build_started_at TIMESTAMPTZ,
		ADD COLUMN build_warn_at TIMESTAMPTZ,
		ADD COLUMN build_deadline_at TIMESTAMPTZ,
		ADD COLUMN build_warned_at TIMESTAMPTZ,
		ADD COLUMN provider_build_ref TEXT,
		ADD COLUMN provider_build_state TEXT,
		ADD CONSTRAINT environment_artifacts_build_timing_shape CHECK (
			(build_started_at IS NULL AND build_warn_at IS NULL AND build_deadline_at IS NULL AND build_warned_at IS NULL)
			OR (build_started_at IS NOT NULL AND build_warn_at IS NOT NULL AND build_deadline_at IS NOT NULL
				AND build_warn_at > build_started_at AND build_deadline_at > build_warn_at)
		),
		ADD CONSTRAINT environment_artifacts_build_ref_shape CHECK (length(provider_build_ref) BETWEEN 1 AND 128),
		ADD CONSTRAINT environment_artifacts_build_state_shape CHECK (
			provider_build_state IN ('awaiting_visibility', 'pending', 'building', 'pulling', 'active', 'error', 'build_failed')
		)`},
	}
}
