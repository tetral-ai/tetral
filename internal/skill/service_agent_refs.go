package skill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/storage"
)

// ValidateAgentSkillReferences validates Agent Skill references inside
// the caller's workspace-scoped Agent transaction.
func (s *PostgreSQLSkillStore) ValidateAgentSkillReferences(ctx context.Context, tx agent.Transaction, workspaceID string, refs []agent.SkillReference) error {
	_, err := s.ResolveSkillReferences(ctx, tx, workspaceID, refs)
	return err
}

// ResolveSkillReferences validates and freezes references under the shared
// workspace registry lock. The caller saves its Agent or Session before
// committing this same transaction, so deletion cannot interleave with
// admission, while admissions do not wait for each other.
func (s *PostgreSQLSkillStore) ResolveSkillReferences(ctx context.Context, tx agent.Transaction, workspaceID string, refs []agent.SkillReference) ([]agent.SkillReference, error) {
	resolved := make([]agent.SkillReference, 0, len(refs))
	if len(refs) == 0 {
		return resolved, nil
	}
	if workspaceID == "" {
		return nil, &agent.ValidationError{Message: "workspace_id is required"}
	}
	if err := storage.AcquireWorkspaceSkillRegistryLockShared(ctx, tx, workspaceID); err != nil {
		return nil, err
	}
	for _, ref := range refs {
		version, err := s.resolveSingleReference(ctx, tx, workspaceID, ref)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, agent.SkillReference{SkillID: ref.SkillID, Version: version})
	}
	return resolved, nil
}

func (s *PostgreSQLSkillStore) resolveSingleReference(ctx context.Context, tx agent.Transaction, workspaceID string, ref agent.SkillReference) (string, error) {
	if ref.SkillID == "" {
		return "", &agent.ValidationError{Message: "skills[].skill_id is required"}
	}
	if ref.Version == "" {
		return "", &agent.ValidationError{Message: fmt.Sprintf("skills[].version for %q is required", ref.SkillID)}
	}
	var (
		latestVersion    sql.NullString
		currentPresent   sql.NullInt64
		requestedVersion = ref.Version
	)
	err := tx.QueryRowScanner(ctx,
		`SELECT s.latest_version, v.storage_sequence
		   FROM skills s
		   LEFT JOIN skill_versions v
		     ON v.workspace_id = s.workspace_id
		    AND v.skill_id = s.skill_id
		    AND v.version = s.latest_version
		    AND v.deleted_at IS NULL
		  WHERE s.workspace_id = $1 AND s.skill_id = $2 AND s.deleted_at IS NULL`,
		workspaceID, ref.SkillID,
	).Scan(&latestVersion, &currentPresent)
	if errors.Is(err, sql.ErrNoRows) {
		return "", &agent.ValidationError{Message: fmt.Sprintf("skills[].skill_id %q does not exist or is not active in this workspace", ref.SkillID)}
	}
	if err != nil {
		return "", err
	}
	if requestedVersion == "latest" {
		if !latestVersion.Valid || !currentPresent.Valid {
			return "", &agent.ValidationError{Message: fmt.Sprintf("skills[].version %q for skill %q does not exist or is not active", requestedVersion, ref.SkillID)}
		}
		return latestVersion.String, nil
	}
	var present int
	err = tx.QueryRowScanner(ctx,
		`SELECT 1 FROM skill_versions
		   WHERE workspace_id = $1 AND skill_id = $2 AND version = $3 AND deleted_at IS NULL`,
		workspaceID, ref.SkillID, requestedVersion,
	).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return "", &agent.ValidationError{Message: fmt.Sprintf("skills[].version %q for skill %q does not exist or is not active", requestedVersion, ref.SkillID)}
	}
	if err != nil {
		return "", err
	}
	return requestedVersion, nil
}
