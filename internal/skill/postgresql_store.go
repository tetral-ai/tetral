package skill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type PostgreSQLSkillStore struct {
	client *dbconnect.Client
	blob   blob.BlobStore

	clock           func() time.Time
	versionStrategy func(time.Time) string
	maxVersionRetry int
	txAndCleanup    func(context.Context, string, func(Transaction) error, func()) error
}

// Transaction is the Skill-owned transaction capability used by test
// seams. Production code adapts dbconnect.Tx into this interface so
// provider-owned transaction types do not leak through Skill boundaries.
type Transaction interface {
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
}

// Row is the Skill-owned row scanner capability.
type Row interface {
	Scan(dest ...any) error
}

// Rows is the Skill-owned row iteration capability.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type runtimeTransaction struct {
	tx *dbconnect.Tx
}

func (t runtimeTransaction) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.Exec(ctx, query, args...)
}

func (t runtimeTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t runtimeTransaction) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	return t.tx.Query(ctx, query, args...)
}

func (t runtimeTransaction) QueryRow(ctx context.Context, query string, args ...any) Row {
	return t.tx.QueryRow(ctx, query, args...)
}

const MaxSkillsPerWorkspace = 1000
const MaxVersionsPerSkill = 100
const MaxRetainedCompressedBytesPerWorkspace int64 = 1 << 30

const defaultMaxVersionRetry = 8

func NewPostgreSQLStore(runtimeClient *dbconnect.Client, blobStore blob.BlobStore) *PostgreSQLSkillStore {
	return &PostgreSQLSkillStore{
		client:          runtimeClient,
		blob:            blobStore,
		clock:           func() time.Time { return storage.Now() },
		versionStrategy: defaultVersionStrategy,
		maxVersionRetry: defaultMaxVersionRetry,
		txAndCleanup: func(ctx context.Context, workspaceID string, fn func(Transaction) error, onCommitFailure func()) error {
			return runtimeClient.WithWorkspaceTxAndCleanup(ctx, workspaceID, "skill.transaction", func(tx *dbconnect.Tx) error {
				return fn(runtimeTransaction{tx: tx})
			}, onCommitFailure)
		},
	}
}

func (s *PostgreSQLSkillStore) SetClock(clock func() time.Time) { s.clock = clock }

func (s *PostgreSQLSkillStore) SetVersionStrategy(strategy func(time.Time) string) {
	s.versionStrategy = strategy
}

func (s *PostgreSQLSkillStore) SetTxRunner(runner func(context.Context, string, func(Transaction) error, func()) error) {
	s.txAndCleanup = runner
}

func defaultVersionStrategy(now time.Time) string {
	return strconv.FormatInt(now.UnixMicro(), 10)
}

func packageBlobKey(ws workspace.ID, skillID, version string) string {
	return fmt.Sprintf("skills/%s/%s/versions/%s/package.zip", ws, skillID, version)
}

func (s *PostgreSQLSkillStore) CreateSkill(ctx context.Context, ws workspace.ID, input CreateSkillInput) (*Skill, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	if input.Package == nil {
		return nil, errors.New("skill: normalized package is required")
	}
	now := s.clock()
	timestamp := now.Format(time.RFC3339)
	skillID := id.New(IDPrefix)
	var result *Skill
	var blobPutOK string
	err := s.txAndCleanup(ctx, string(ws), func(tx Transaction) error {
		if err := storage.AcquireWorkspaceSkillRegistryLock(ctx, tx, string(ws)); err != nil {
			return err
		}
		if err := s.assertSkillCountQuota(ctx, tx, ws); err != nil {
			return err
		}
		if err := s.assertRetainedBytesQuota(ctx, tx, ws, input.Package.SizeBytes); err != nil {
			return err
		}
		versionID, version, blobKey, err := s.insertVersionWithRetry(ctx, tx, ws, skillID, input.Package, input.DisplayTitle, timestamp, true)
		if err != nil {
			return err
		}
		if err := s.putPackageBlob(ctx, blobKey, input.Package); err != nil {
			return s.packagePutFailure(ctx, blobKey, err)
		}
		blobPutOK = blobKey
		result = &Skill{
			ID: skillID, Type: "skill", Source: "custom", DisplayTitle: input.DisplayTitle,
			LatestVersion: &version, CreatedAt: now, UpdatedAt: now,
		}
		_ = versionID
		return nil
	}, func() { s.bestEffortDelete(ctx, blobPutOK) })
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLSkillStore) CreateVersion(ctx context.Context, ws workspace.ID, skillID string, input CreateVersionInput) (*SkillVersion, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	if input.Package == nil {
		return nil, errors.New("skill: normalized package is required")
	}
	now := s.clock()
	timestamp := now.Format(time.RFC3339)
	var result *SkillVersion
	var blobPutOK string
	err := s.txAndCleanup(ctx, string(ws), func(tx Transaction) error {
		if err := storage.AcquireWorkspaceSkillRegistryLock(ctx, tx, string(ws)); err != nil {
			return err
		}
		if _, err := s.loadActiveSkillForUpdate(ctx, tx, ws, skillID); err != nil {
			return err
		}
		if err := s.assertVersionCountQuota(ctx, tx, ws, skillID); err != nil {
			return err
		}
		if err := s.assertRetainedBytesQuota(ctx, tx, ws, input.Package.SizeBytes); err != nil {
			return err
		}
		versionID, version, blobKey, err := s.insertVersionWithRetry(ctx, tx, ws, skillID, input.Package, nil, timestamp, false)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE skills SET latest_version = $1, updated_at = $2 WHERE workspace_id = $3 AND skill_id = $4`,
			version, timestamp, string(ws), skillID); err != nil {
			return err
		}
		if err := s.putPackageBlob(ctx, blobKey, input.Package); err != nil {
			return s.packagePutFailure(ctx, blobKey, err)
		}
		blobPutOK = blobKey
		result = skillVersionDTO(versionID, skillID, version, input.Package, now, 0)
		return nil
	}, func() { s.bestEffortDelete(ctx, blobPutOK) })
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLSkillStore) insertVersionWithRetry(ctx context.Context, tx Transaction, ws workspace.ID, skillID string, pkg *StagedPackage, displayTitle *string, timestamp string, initial bool) (string, string, string, error) {
	const savepoint = "skill_version_attempt"
	for attempt := 0; attempt < s.maxVersionRetry; attempt++ {
		versionID := id.New(VersionIDPrefix)
		version := s.versionStrategy(s.clock())
		blobKey := packageBlobKey(ws, skillID, version)
		if _, err := tx.Exec(ctx, "SAVEPOINT "+savepoint); err != nil {
			return "", "", "", err
		}
		if initial {
			if _, err := tx.Exec(ctx,
				`INSERT INTO skills (workspace_id, skill_id, display_title, latest_version, created_at, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $5)`,
				string(ws), skillID, displayTitle, version, timestamp); err != nil {
				_ = rollbackSavepoint(ctx, tx, savepoint)
				return "", "", "", mapPGError(err)
			}
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO skill_versions (workspace_id, skill_id, skill_version_id, version, name, description, directory, blob_key, size_bytes, sha256, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			string(ws), skillID, versionID, version, pkg.Name, pkg.Description, pkg.Directory, blobKey, pkg.SizeBytes, pkg.SHA256, timestamp)
		if err == nil {
			if _, releaseErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+savepoint); releaseErr != nil {
				return "", "", "", releaseErr
			}
			return versionID, version, blobKey, nil
		}
		if rollbackErr := rollbackSavepoint(ctx, tx, savepoint); rollbackErr != nil {
			return "", "", "", rollbackErr
		}
		if !isUniqueViolation(err) {
			return "", "", "", mapPGError(err)
		}
	}
	return "", "", "", &ConflictError{Message: "skill version generator exhausted retry budget"}
}

func rollbackSavepoint(ctx context.Context, tx Transaction, savepoint string) error {
	_, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+savepoint)
	return err
}

func (s *PostgreSQLSkillStore) putPackageBlob(ctx context.Context, key string, pkg *StagedPackage) error {
	rc, err := pkg.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	return s.blob.Put(ctx, key, rc, pkg.SizeBytes)
}

func (s *PostgreSQLSkillStore) packagePutFailure(ctx context.Context, key string, err error) error {
	var duplicate *blob.DuplicateKeyError
	if errors.As(err, &duplicate) {
		return &ConflictError{Message: "skill package already exists"}
	}
	s.bestEffortDelete(ctx, key)
	return err
}

func (s *PostgreSQLSkillStore) GetSkill(ctx context.Context, ws workspace.ID, skillID string) (*Skill, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	var result *Skill
	err := s.client.WithWorkspaceTx(ctx, string(ws), "skill.transaction", func(tx *dbconnect.Tx) error {
		skill, err := s.selectActiveSkill(ctx, runtimeTransaction{tx: tx}, ws, skillID, false)
		if err != nil {
			return err
		}
		result = skill
		return nil
	})
	return result, err
}

func (s *PostgreSQLSkillStore) GetVersion(ctx context.Context, ws workspace.ID, skillID, version string) (*SkillVersion, error) {
	if err := requireWorkspace(ws); err != nil {
		return nil, err
	}
	var result *SkillVersion
	err := s.client.WithWorkspaceTx(ctx, string(ws), "skill.transaction", func(tx *dbconnect.Tx) error {
		skillTx := runtimeTransaction{tx: tx}
		if _, err := s.selectActiveSkill(ctx, skillTx, ws, skillID, false); err != nil {
			return err
		}
		versionRow, err := s.selectActiveVersion(ctx, skillTx, ws, skillID, version)
		if err != nil {
			return err
		}
		result = versionRow
		return nil
	})
	return result, err
}

func (s *PostgreSQLSkillStore) OpenVersionContent(ctx context.Context, ws workspace.ID, skillID, version string) (io.ReadCloser, error) {
	if _, err := s.GetVersion(ctx, ws, skillID, version); err != nil {
		return nil, err
	}
	return s.blob.Get(ctx, packageBlobKey(ws, skillID, version))
}

func (s *PostgreSQLSkillStore) ListSkills(ctx context.Context, ws workspace.ID, options ListSkillsOptions) (SkillListResult, error) {
	if err := requireWorkspace(ws); err != nil {
		return SkillListResult{}, err
	}
	limit := normalizeSkillListLimit(options.Limit)
	result := SkillListResult{Data: []*Skill{}}
	err := s.client.WithWorkspaceTx(ctx, string(ws), "skill.transaction", func(tx *dbconnect.Tx) error {
		query := `SELECT storage_sequence, skill_id, display_title, latest_version, created_at, updated_at
		            FROM skills
		           WHERE workspace_id = $1 AND deleted_at IS NULL AND storage_sequence > $2
		           ORDER BY storage_sequence ASC LIMIT $3`
		rows, err := tx.Query(ctx, query, string(ws), options.cursorSequence, limit+1)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			item, err := scanSkill(rows)
			if err != nil {
				return err
			}
			result.Data = append(result.Data, item)
		}
		return rows.Err()
	})
	if err != nil {
		return SkillListResult{}, err
	}
	if len(result.Data) > limit {
		result.HasMore = true
		result.Data = result.Data[:limit]
	}
	return result, nil
}

func (s *PostgreSQLSkillStore) ListVersions(ctx context.Context, ws workspace.ID, skillID string, options ListVersionsOptions) (SkillVersionListResult, error) {
	if err := requireWorkspace(ws); err != nil {
		return SkillVersionListResult{}, err
	}
	limit := normalizeVersionListLimit(options.Limit)
	result := SkillVersionListResult{Data: []*SkillVersion{}}
	err := s.client.WithWorkspaceTx(ctx, string(ws), "skill.transaction", func(tx *dbconnect.Tx) error {
		if _, err := s.selectActiveSkill(ctx, runtimeTransaction{tx: tx}, ws, skillID, false); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`SELECT storage_sequence, skill_version_id, skill_id, version, name, description, directory, created_at
			   FROM skill_versions
			  WHERE workspace_id = $1 AND skill_id = $2 AND deleted_at IS NULL AND storage_sequence > $3
			  ORDER BY storage_sequence ASC LIMIT $4`,
			string(ws), skillID, options.cursorSequence, limit+1)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			version, err := scanSkillVersion(rows)
			if err != nil {
				return err
			}
			result.Data = append(result.Data, version)
		}
		return rows.Err()
	})
	if err != nil {
		return SkillVersionListResult{}, err
	}
	if len(result.Data) > limit {
		result.HasMore = true
		result.Data = result.Data[:limit]
	}
	return result, nil
}

func (s *PostgreSQLSkillStore) DeleteSkill(ctx context.Context, ws workspace.ID, skillID string) error {
	if err := requireWorkspace(ws); err != nil {
		return err
	}
	deletedAt := s.clock().Format(time.RFC3339)
	return s.client.WithWorkspaceTx(ctx, string(ws), "skill.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireWorkspaceSkillRegistryLock(ctx, tx, string(ws)); err != nil {
			return err
		}
		if _, err := s.loadActiveSkillForUpdate(ctx, runtimeTransaction{tx: tx}, ws, skillID); err != nil {
			return err
		}
		var activeVersions int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM skill_versions WHERE workspace_id = $1 AND skill_id = $2 AND deleted_at IS NULL`,
			string(ws), skillID).Scan(&activeVersions); err != nil {
			return err
		}
		if activeVersions > 0 {
			return &ValidationError{Message: "delete Skill versions before deleting the Skill"}
		}
		res, err := tx.Exec(ctx,
			`UPDATE skills SET deleted_at = $1, updated_at = $1 WHERE workspace_id = $2 AND skill_id = $3 AND deleted_at IS NULL`,
			deletedAt, string(ws), skillID)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "skill not found"}
		}
		return nil
	})
}

func (s *PostgreSQLSkillStore) DeleteVersion(ctx context.Context, ws workspace.ID, skillID, version string) error {
	if err := requireWorkspace(ws); err != nil {
		return err
	}
	deletedAt := s.clock().Format(time.RFC3339)
	return s.client.WithWorkspaceTx(ctx, string(ws), "skill.transaction", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireWorkspaceSkillRegistryLock(ctx, tx, string(ws)); err != nil {
			return err
		}
		parent, err := s.loadActiveSkillForUpdate(ctx, runtimeTransaction{tx: tx}, ws, skillID)
		if err != nil {
			return err
		}
		res, err := tx.Exec(ctx,
			`UPDATE skill_versions SET deleted_at = $1 WHERE workspace_id = $2 AND skill_id = $3 AND version = $4 AND deleted_at IS NULL`,
			deletedAt, string(ws), skillID, version)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "skill version not found"}
		}
		if parent.latestVersion.Valid && parent.latestVersion.String != version {
			_, err := tx.Exec(ctx, `UPDATE skills SET updated_at = $1 WHERE workspace_id = $2 AND skill_id = $3`, deletedAt, string(ws), skillID)
			return err
		}
		var next sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT version FROM skill_versions
			  WHERE workspace_id = $1 AND skill_id = $2 AND deleted_at IS NULL
			  ORDER BY storage_sequence DESC LIMIT 1`,
			string(ws), skillID).Scan(&next); errors.Is(err, sql.ErrNoRows) {
			next.Valid = false
		} else if err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE skills SET latest_version = $1, updated_at = $2 WHERE workspace_id = $3 AND skill_id = $4`,
			next, deletedAt, string(ws), skillID)
		return err
	})
}

type activeParent struct {
	latestVersion sql.NullString
}

func (s *PostgreSQLSkillStore) loadActiveSkillForUpdate(ctx context.Context, tx Transaction, ws workspace.ID, skillID string) (*activeParent, error) {
	var latest sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT latest_version FROM skills WHERE workspace_id = $1 AND skill_id = $2 AND deleted_at IS NULL FOR UPDATE`,
		string(ws), skillID).Scan(&latest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &NotFoundError{Message: "skill not found"}
	}
	if err != nil {
		return nil, err
	}
	return &activeParent{latestVersion: latest}, nil
}

func (s *PostgreSQLSkillStore) selectActiveSkill(ctx context.Context, tx Transaction, ws workspace.ID, skillID string, forUpdate bool) (*Skill, error) {
	query := `SELECT storage_sequence, skill_id, display_title, latest_version, created_at, updated_at
	            FROM skills WHERE workspace_id = $1 AND skill_id = $2 AND deleted_at IS NULL`
	if forUpdate {
		query += " FOR UPDATE"
	}
	row := tx.QueryRow(ctx, query, string(ws), skillID)
	return scanSkill(row)
}

func (s *PostgreSQLSkillStore) selectActiveVersion(ctx context.Context, tx Transaction, ws workspace.ID, skillID, version string) (*SkillVersion, error) {
	row := tx.QueryRow(ctx,
		`SELECT storage_sequence, skill_version_id, skill_id, version, name, description, directory, created_at
		   FROM skill_versions
		  WHERE workspace_id = $1 AND skill_id = $2 AND version = $3 AND deleted_at IS NULL`,
		string(ws), skillID, version)
	return scanSkillVersion(row)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSkill(row rowScanner) (*Skill, error) {
	var (
		sequence      int64
		skillID       string
		displayTitle  sql.NullString
		latestVersion sql.NullString
		createdAt     time.Time
		updatedAt     time.Time
	)
	if err := row.Scan(&sequence, &skillID, &displayTitle, &latestVersion, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &NotFoundError{Message: "skill not found"}
		}
		return nil, err
	}
	created := createdAt.UTC()
	updated := updatedAt.UTC()
	result := &Skill{ID: skillID, Type: "skill", Source: "custom", CreatedAt: created, UpdatedAt: updated, storageSequence: sequence}
	if displayTitle.Valid {
		result.DisplayTitle = &displayTitle.String
	}
	if latestVersion.Valid {
		result.LatestVersion = &latestVersion.String
	}
	return result, nil
}

func scanSkillVersion(row rowScanner) (*SkillVersion, error) {
	var (
		sequence    int64
		versionID   string
		skillID     string
		version     string
		name        string
		description string
		directory   string
		createdAt   time.Time
	)
	if err := row.Scan(&sequence, &versionID, &skillID, &version, &name, &description, &directory, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &NotFoundError{Message: "skill version not found"}
		}
		return nil, err
	}
	created := createdAt.UTC()
	return &SkillVersion{
		ID: versionID, Type: "skill_version", SkillID: skillID, Version: version,
		Name: name, Description: description, Directory: directory, CreatedAt: created,
		storageSequence: sequence,
	}, nil
}

func skillVersionDTO(versionID, skillID, version string, pkg *StagedPackage, created time.Time, sequence int64) *SkillVersion {
	return &SkillVersion{
		ID: versionID, Type: "skill_version", SkillID: skillID, Version: version,
		Name: pkg.Name, Description: pkg.Description, Directory: pkg.Directory,
		CreatedAt: created, storageSequence: sequence,
	}
}

func (s *PostgreSQLSkillStore) assertSkillCountQuota(ctx context.Context, tx Transaction, ws workspace.ID) error {
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM skills WHERE workspace_id = $1`, string(ws)).Scan(&count); err != nil {
		return err
	}
	if count >= MaxSkillsPerWorkspace {
		return &QuotaError{Kind: QuotaKindCount, Message: "workspace Skill quota exceeded"}
	}
	return nil
}

func (s *PostgreSQLSkillStore) assertVersionCountQuota(ctx context.Context, tx Transaction, ws workspace.ID, skillID string) error {
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM skill_versions WHERE workspace_id = $1 AND skill_id = $2`, string(ws), skillID).Scan(&count); err != nil {
		return err
	}
	if count >= MaxVersionsPerSkill {
		return &QuotaError{Kind: QuotaKindCount, Message: "Skill version quota exceeded"}
	}
	return nil
}

func (s *PostgreSQLSkillStore) assertRetainedBytesQuota(ctx context.Context, tx Transaction, ws workspace.ID, addBytes int64) error {
	var sum int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM(size_bytes), 0) FROM skill_versions WHERE workspace_id = $1`, string(ws)).Scan(&sum); err != nil {
		return err
	}
	if sum+addBytes > MaxRetainedCompressedBytesPerWorkspace {
		return &QuotaError{Kind: QuotaKindRetainedBytes, Message: "workspace retained-bytes Skill quota exceeded"}
	}
	return nil
}

func normalizeSkillListLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func normalizeVersionListLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func (s *PostgreSQLSkillStore) bestEffortDelete(_ context.Context, key string) {
	if key == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.blob.Delete(cleanupCtx, key)
}

func requireWorkspace(ws workspace.ID) error {
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func mapPGError(err error) error {
	return err
}
