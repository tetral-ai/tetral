package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// PostgreSQLSandboxStore reads durable Session resources and memory snapshots
// for provider materialization. Sandbox lifecycle state is owned by the
// service-level binding and operation stores, not by this reader.
type PostgreSQLSandboxStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLStore(client *dbconnect.Client) *PostgreSQLSandboxStore {
	return &PostgreSQLSandboxStore{client: client}
}

func (s *PostgreSQLSandboxStore) ListSessionResources(ctx context.Context, ws workspace.ID, sessionID string) (ResourceSetup, error) {
	if s == nil || s.client == nil {
		return ResourceSetup{}, &ValidationError{Message: "session resource store is required"}
	}
	var setup ResourceSetup
	err := s.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "sandbox.list_session_resources", func(tx *dbconnect.Tx) error {
		var err error
		setup, err = listSessionResourcesTx(ctx, tx, ws, sessionID)
		return err
	})
	return setup, err
}

// ListSessionResourcesTx resolves one immutable materialization snapshot while
// its caller holds the Session and target-revision transaction fences.
func (s *PostgreSQLSandboxStore) ListSessionResourcesTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) (ResourceSetup, error) {
	if s == nil || s.client == nil || tx == nil {
		return ResourceSetup{}, &ValidationError{Message: "session resource transaction is required"}
	}
	return listSessionResourcesTx(ctx, tx, ws, sessionID)
}

func listSessionResourcesTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) (ResourceSetup, error) {
	var setup ResourceSetup
	rows, err := tx.Query(ctx,
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
		  WHERE sr.workspace_id = $1
		    AND sr.session_id = $2
		  ORDER BY sr.storage_sequence ASC`,
		string(ws), sessionID,
	)
	if err != nil {
		return ResourceSetup{}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			resourceID         string
			resourceType       string
			detachedAt         sql.NullTime
			deleteRequestedAt  sql.NullTime
			sourceFileID       sql.NullString
			objectID           sql.NullString
			sessionFileID      sql.NullString
			fileMountPath      sql.NullString
			memoryStoreID      sql.NullString
			memoryAccess       sql.NullString
			memoryInstructions sql.NullString
			memoryName         sql.NullString
			memoryDescription  sql.NullString
			memoryMountPath    sql.NullString
			githubURL          sql.NullString
			githubMountPath    sql.NullString
			checkoutType       sql.NullString
			checkoutRef        sql.NullString
			gitIdentityName    sql.NullString
			gitIdentityEmail   sql.NullString
		)
		if err := rows.Scan(
			&resourceID, &resourceType, &detachedAt, &deleteRequestedAt,
			&sourceFileID, &objectID, &sessionFileID, &fileMountPath,
			&memoryStoreID, &memoryAccess, &memoryInstructions, &memoryName, &memoryDescription, &memoryMountPath,
			&githubURL, &githubMountPath, &checkoutType, &checkoutRef,
			&gitIdentityName, &gitIdentityEmail,
		); err != nil {
			return ResourceSetup{}, err
		}
		switch resourceType {
		case "file":
			mount := FileMount{
				ResourceID:    resourceID,
				SourceFileID:  nullableStringValue(sourceFileID),
				SessionFileID: nullableStringValue(sessionFileID),
				ObjectID:      nullableStringValue(objectID),
				MountPath:     nullableStringValue(fileMountPath),
				ReadOnly:      true,
			}
			if deleteRequestedAt.Valid && !detachedAt.Valid {
				setup.DeletedFiles = append(setup.DeletedFiles, mount)
			} else if !detachedAt.Valid {
				setup.Files = append(setup.Files, mount)
			}
		case "memory_store":
			mount := MemoryStoreMount{
				ResourceID:    resourceID,
				MemoryStoreID: nullableStringValue(memoryStoreID),
				MountPath:     nullableStringValue(memoryMountPath),
				Access:        nullableStringValue(memoryAccess),
				Instructions:  nullableStringValue(memoryInstructions),
				Name:          nullableStringValue(memoryName),
				Description:   nullableStringValue(memoryDescription),
			}
			if deleteRequestedAt.Valid && !detachedAt.Valid {
				setup.DeletedMemoryStores = append(setup.DeletedMemoryStores, mount)
			} else if !detachedAt.Valid {
				setup.MemoryStores = append(setup.MemoryStores, mount)
			}
		case "github_repository":
			mount := GitHubRepositoryMount{
				ResourceID:       resourceID,
				URL:              nullableStringValue(githubURL),
				MountPath:        nullableStringValue(githubMountPath),
				CheckoutType:     nullableStringValue(checkoutType),
				CheckoutRef:      nullableStringValue(checkoutRef),
				GitIdentityName:  nullableStringValue(gitIdentityName),
				GitIdentityEmail: nullableStringValue(gitIdentityEmail),
			}
			if deleteRequestedAt.Valid && !detachedAt.Valid {
				setup.DeletedGitHubRepositories = append(setup.DeletedGitHubRepositories, mount)
			} else if !detachedAt.Valid {
				setup.GitHubRepositories = append(setup.GitHubRepositories, mount)
			}
		default:
			return ResourceSetup{}, &ValidationError{Message: "session resource type is invalid"}
		}
	}
	if err := rows.Err(); err != nil {
		return ResourceSetup{}, err
	}
	skills, err := listSessionSkills(ctx, tx, ws, sessionID)
	if err != nil {
		return ResourceSetup{}, err
	}
	setup.Skills = skills
	return setup, nil
}

type sessionSkillRef struct {
	SkillID string `json:"skill_id"`
	Version string `json:"version"`
}

func listSessionSkills(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) ([]SkillMount, error) {
	var configJSON string
	err := tx.QueryRow(ctx,
		`SELECT av.config_json
		   FROM sessions s
		   JOIN agent_versions av
		     ON av.workspace_id = s.workspace_id
		    AND av.id = s.agent_version_id
		  WHERE s.workspace_id = $1
		    AND s.id = $2`,
		string(ws), sessionID,
	).Scan(&configJSON)
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "session not found"}
	}
	if err != nil {
		return nil, err
	}
	var config struct {
		Skills []sessionSkillRef `json:"skills"`
	}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, &ValidationError{Message: "session agent config is invalid"}
	}
	if len(config.Skills) == 0 {
		return nil, nil
	}
	out := make([]SkillMount, 0, len(config.Skills))
	for _, ref := range config.Skills {
		if ref.SkillID == "" || ref.Version == "" {
			return nil, &ValidationError{Message: "session agent skill reference is invalid"}
		}
		versionExpr := `CASE WHEN $3 = 'latest' THEN s.latest_version ELSE $3 END`
		var mount SkillMount
		err := tx.QueryRow(ctx,
			`SELECT v.skill_id, v.skill_version_id, v.version, v.name, v.description, v.directory, v.blob_key, v.sha256, v.size_bytes
			   FROM skills s
			   JOIN skill_versions v
			     ON v.workspace_id = s.workspace_id
			    AND v.skill_id = s.skill_id
			    AND v.version = `+versionExpr+`
			    AND v.deleted_at IS NULL
			  WHERE s.workspace_id = $1
			    AND s.skill_id = $2
			    AND s.deleted_at IS NULL`,
			string(ws), ref.SkillID, ref.Version,
		).Scan(&mount.SkillID, &mount.SkillVersionID, &mount.Version, &mount.Name, &mount.Description, &mount.Directory, &mount.BlobKey, &mount.SHA256, &mount.SizeBytes)
		if dbconnect.IsNoRows(err) {
			return nil, &ValidationError{Message: "session agent skill reference is not active"}
		}
		if err != nil {
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

// SessionSkillIndexEntry is the Runtime-visible identity and metadata for one
// resolved Session skill. Provider-only Blob and integrity fields remain in
// SkillMount and are not exposed to the conversation runtime.
type SessionSkillIndexEntry struct {
	SkillID        string `json:"skill_id"`
	SkillVersionID string `json:"skill_version_id"`
	Version        string `json:"version"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	Directory      string `json:"directory"`
}

// ResolveSessionSkillIndex resolves the same immutable Agent skill references
// used by Sandbox materialization into Runtime context metadata.
func ResolveSessionSkillIndex(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) ([]SessionSkillIndexEntry, error) {
	mounts, err := listSessionSkills(ctx, tx, ws, sessionID)
	if err != nil {
		return nil, err
	}
	index := make([]SessionSkillIndexEntry, 0, len(mounts))
	for _, mount := range mounts {
		index = append(index, SessionSkillIndexEntry{
			SkillID:        mount.SkillID,
			SkillVersionID: mount.SkillVersionID,
			Version:        mount.Version,
			Name:           mount.Name,
			Description:    mount.Description,
			Directory:      mount.Directory,
		})
	}
	return index, nil
}

func (s *PostgreSQLSandboxStore) ReadMemoryStoreSnapshot(ctx context.Context, ws workspace.ID, memoryStoreID string) ([]MemorySnapshotFile, error) {
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "memory snapshot store is required"}
	}
	var files []MemorySnapshotFile
	if err := s.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "sandbox.read_memory_store_snapshot", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT m.path, v.content, m.content_sha256
			   FROM memories m
			   JOIN memory_versions v
			     ON v.workspace_id = m.workspace_id
			    AND v.memory_store_id = m.memory_store_id
			    AND v.memory_id = m.memory_id
			    AND v.memory_version_id = m.current_version_id
			  WHERE m.workspace_id = $1
			    AND m.memory_store_id = $2
			    AND m.deleted_at IS NULL
			  ORDER BY m.path ASC`,
			string(ws), memoryStoreID,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var file MemorySnapshotFile
			if err := rows.Scan(&file.Path, &file.Content, &file.ContentSHA256); err != nil {
				return err
			}
			files = append(files, file)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return files, nil
}

func (s *PostgreSQLSandboxStore) WithMemoryStoreMutationLocks(ctx context.Context, ws workspace.ID, memoryStoreIDs []string, fn func(context.Context) error) error {
	if s == nil || s.client == nil {
		return &ValidationError{Message: "memory store mutation locker is required"}
	}
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if fn == nil {
		return &ValidationError{Message: "memory store mutation callback is required"}
	}
	ids := uniqueMemoryStoreIDs(memoryStoreIDs)
	if len(ids) == 0 {
		return &ValidationError{Message: "memory_store_id is required"}
	}
	return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.memory_projection_mutation_locks", func(tx *dbconnect.Tx) error {
		for _, memoryStoreID := range ids {
			if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), memoryStoreID); err != nil {
				return err
			}
		}
		return fn(ctx)
	})
}

func uniqueMemoryStoreIDs(memoryStoreIDs []string) []string {
	seen := map[string]struct{}{}
	ids := make([]string, 0, len(memoryStoreIDs))
	for _, memoryStoreID := range memoryStoreIDs {
		if memoryStoreID == "" {
			continue
		}
		if _, ok := seen[memoryStoreID]; ok {
			continue
		}
		seen[memoryStoreID] = struct{}{}
		ids = append(ids, memoryStoreID)
	}
	sort.Strings(ids)
	return ids
}

func nullableStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}
