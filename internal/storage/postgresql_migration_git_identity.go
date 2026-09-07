package storage

// postgresqlGitIdentitySteps upgrades the Alpha 1 repository resource table.
// NULL preserves the default identity for existing repositories. The migration
// executor applies both steps and their history stamp in one transaction.
func postgresqlGitIdentitySteps() []postgresqlSchemaStep {
	return []postgresqlSchemaStep{
		{"add_git_identity_columns", `ALTER TABLE session_github_repository_resources
		ADD COLUMN git_identity_name TEXT,
		ADD COLUMN git_identity_email TEXT`},
		{"add_git_identity_constraint", `ALTER TABLE session_github_repository_resources
		ADD CONSTRAINT session_github_repository_git_identity_shape CHECK (
			(git_identity_name IS NULL AND git_identity_email IS NULL)
			OR (git_identity_name IS NOT NULL AND git_identity_name <> '' AND git_identity_email IS NOT NULL AND git_identity_email <> '')
		)`},
	}
}
