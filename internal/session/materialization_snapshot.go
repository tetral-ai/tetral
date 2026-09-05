package session

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
)

// sessionResourceMaterializationSnapshot is the provider-neutral, immutable
// declaration set attached to one Sandbox materialization operation. Its JSON
// shape is consumed by Sandbox Service; it contains durable references only.
type sessionResourceMaterializationSnapshot struct {
	Files                     []sessionFileMount
	DeletedFiles              []sessionFileMount
	MemoryStores              []sessionMemoryStoreMount
	DeletedMemoryStores       []sessionMemoryStoreMount
	GitHubRepositories        []sessionGitHubRepositoryMount
	DeletedGitHubRepositories []sessionGitHubRepositoryMount
	Skills                    []sessionSkillMount
}

type sessionFileMount struct {
	ResourceID    string
	SourceFileID  string
	SessionFileID string
	ObjectID      string
	MountPath     string
	ReadOnly      bool
}

type sessionMemoryStoreMount struct {
	ResourceID    string
	MemoryStoreID string
	MountPath     string
	Access        string
	Instructions  string
	Name          string
	Description   string
}

type sessionGitHubRepositoryMount struct {
	ResourceID   string
	URL          string
	MountPath    string
	CheckoutType string
	CheckoutRef  string
	// GitIdentityName/GitIdentityEmail carry the declared repository-local
	// commit identity; both empty means the resource keeps the session-scoped
	// platform fallback.
	GitIdentityName  string
	GitIdentityEmail string
}

type sessionSkillMount struct {
	SkillID        string
	SkillVersionID string
	Version        string
	Name           string
	Description    string
	Directory      string
	BlobKey        string
	SHA256         string
	SizeBytes      int64
}

func (t *postgresqlTransaction) loadResourceMaterializationSnapshot(ctx context.Context, sessionID string) (sessionResourceMaterializationSnapshot, error) {
	var snapshot sessionResourceMaterializationSnapshot
	rows, err := t.tx.QueryRows(ctx,
		`SELECT sr.resource_id, sr.type, sr.detached_at, sr.delete_requested_at,
		        sfr.source_file_id, source_file.object_id, sfr.file_id, sfr.mount_path,
		        smr.memory_store_id, smr.access, smr.instructions, smr.name, smr.description, smr.mount_path,
		        sgr.url, sgr.mount_path, sgr.checkout_type, sgr.checkout_ref
		   FROM session_resources sr
		   LEFT JOIN session_file_resources sfr
		     ON sfr.workspace_id = sr.workspace_id
		    AND sfr.session_id = sr.session_id
		    AND sfr.resource_id = sr.resource_id
		   LEFT JOIN files source_file
		     ON source_file.workspace_id = sfr.workspace_id
		    AND source_file.file_id = sfr.source_file_id
		   LEFT JOIN session_memory_store_resources smr
		     ON smr.workspace_id = sr.workspace_id
		    AND smr.session_id = sr.session_id
		    AND smr.resource_id = sr.resource_id
		   LEFT JOIN session_github_repository_resources sgr
		     ON sgr.workspace_id = sr.workspace_id
		    AND sgr.session_id = sr.session_id
		    AND sgr.resource_id = sr.resource_id
		  WHERE sr.workspace_id = $1 AND sr.session_id = $2
		  ORDER BY sr.storage_sequence ASC`,
		string(t.workspaceID), sessionID,
	)
	if err != nil {
		return snapshot, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var resourceID, resourceType string
		var detachedAt, deleteRequestedAt sql.NullTime
		var sourceFileID, objectID, sessionFileID, fileMountPath sql.NullString
		var memoryStoreID, memoryAccess, memoryInstructions, memoryName, memoryDescription, memoryMountPath sql.NullString
		var githubURL, githubMountPath, checkoutType, checkoutRef sql.NullString
		var gitIdentityName, gitIdentityEmail sql.NullString
		if err := rows.Scan(
			&resourceID, &resourceType, &detachedAt, &deleteRequestedAt,
			&sourceFileID, &objectID, &sessionFileID, &fileMountPath,
			&memoryStoreID, &memoryAccess, &memoryInstructions, &memoryName, &memoryDescription, &memoryMountPath,
			&githubURL, &githubMountPath, &checkoutType, &checkoutRef,
			&gitIdentityName, &gitIdentityEmail,
		); err != nil {
			return snapshot, err
		}
		if detachedAt.Valid {
			continue
		}
		switch ResourceType(resourceType) {
		case ResourceTypeFile:
			mount := sessionFileMount{
				ResourceID: resourceID, SourceFileID: sourceFileID.String,
				SessionFileID: sessionFileID.String, ObjectID: objectID.String,
				MountPath: fileMountPath.String, ReadOnly: true,
			}
			if deleteRequestedAt.Valid {
				snapshot.DeletedFiles = append(snapshot.DeletedFiles, mount)
			} else {
				snapshot.Files = append(snapshot.Files, mount)
			}
		case ResourceTypeMemoryStore:
			mount := sessionMemoryStoreMount{
				ResourceID: resourceID, MemoryStoreID: memoryStoreID.String,
				MountPath: memoryMountPath.String, Access: memoryAccess.String,
				Instructions: memoryInstructions.String, Name: memoryName.String,
				Description: memoryDescription.String,
			}
			if deleteRequestedAt.Valid {
				snapshot.DeletedMemoryStores = append(snapshot.DeletedMemoryStores, mount)
			} else {
				snapshot.MemoryStores = append(snapshot.MemoryStores, mount)
			}
		case ResourceTypeGitHubRepository:
			mount := sessionGitHubRepositoryMount{
				ResourceID: resourceID, URL: githubURL.String, MountPath: githubMountPath.String,
				CheckoutType: checkoutType.String, CheckoutRef: checkoutRef.String,
				GitIdentityName: gitIdentityName.String, GitIdentityEmail: gitIdentityEmail.String,
			}
			if deleteRequestedAt.Valid {
				snapshot.DeletedGitHubRepositories = append(snapshot.DeletedGitHubRepositories, mount)
			} else {
				snapshot.GitHubRepositories = append(snapshot.GitHubRepositories, mount)
			}
		default:
			return snapshot, &ValidationError{Message: "session resource type is invalid"}
		}
	}
	if err := rows.Err(); err != nil {
		return snapshot, err
	}
	skills, err := t.loadMaterializationSkills(ctx, sessionID)
	if err != nil {
		return snapshot, err
	}
	snapshot.Skills = skills
	return snapshot, nil
}

func (t *postgresqlTransaction) loadMaterializationSkills(ctx context.Context, sessionID string) ([]sessionSkillMount, error) {
	var configJSON string
	if err := t.tx.QueryRowScanner(ctx,
		`SELECT av.config_json
		   FROM sessions s
		   JOIN agent_versions av
		     ON av.workspace_id = s.workspace_id AND av.id = s.agent_version_id
		  WHERE s.workspace_id = $1 AND s.id = $2`,
		string(t.workspaceID), sessionID,
	).Scan(&configJSON); err != nil {
		return nil, err
	}
	var config struct {
		Skills []struct {
			SkillID string `json:"skill_id"`
			Version string `json:"version"`
		} `json:"skills"`
	}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, &ValidationError{Message: "session agent config is invalid"}
	}
	out := make([]sessionSkillMount, 0, len(config.Skills))
	for _, ref := range config.Skills {
		if ref.SkillID == "" || ref.Version == "" {
			return nil, &ValidationError{Message: "session agent skill reference is invalid"}
		}
		var mount sessionSkillMount
		if err := t.tx.QueryRowScanner(ctx,
			`SELECT v.skill_id, v.skill_version_id, v.version, v.name, v.description,
			        v.directory, v.blob_key, v.sha256, v.size_bytes
			   FROM skills s
			   JOIN skill_versions v
			     ON v.workspace_id = s.workspace_id
			    AND v.skill_id = s.skill_id
			    AND v.version = CASE WHEN $3 = 'latest' THEN s.latest_version ELSE $3 END
			    AND v.deleted_at IS NULL
			  WHERE s.workspace_id = $1 AND s.skill_id = $2 AND s.deleted_at IS NULL`,
			string(t.workspaceID), ref.SkillID, ref.Version,
		).Scan(
			&mount.SkillID, &mount.SkillVersionID, &mount.Version, &mount.Name,
			&mount.Description, &mount.Directory, &mount.BlobKey, &mount.SHA256, &mount.SizeBytes,
		); err != nil {
			return nil, err
		}
		out = append(out, mount)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SkillID != out[j].SkillID {
			return out[i].SkillID < out[j].SkillID
		}
		return out[i].SkillVersionID < out[j].SkillVersionID
	})
	return out, nil
}
