package tetralsandbox

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/workspace"
	"github.com/tetral-ai/tetral/services/sandbox/internal/resourceprojection"
)

const (
	defaultResourcePreparationTimeout = 45 * time.Second
	defaultRcloneVFSCacheMaxSize      = "2G"
	defaultRcloneVFSMinFree           = "1G"
	skillProjectionStagingRoot        = "/tmp/tetral-runtime/skill-projection"
)

// ResourceProjectionLevel selects how projected files reach the sandbox.
//
// fuse_bind (L1) is the privileged path: an rclone read-only mount plus a
// per-file bind. It requires the minted read-only credential (enforced in
// NewResourceProjectionPreparer) and the bytes are read directly from R2 by the
// in-sandbox rclone, never through this process.
//
// local_copy (L2) has the driver stream the session-copy bytes into a 0444
// local file (stageLocalCopyResources reads them through this process into the
// sandbox — the one exception to bytes never transiting our process). It does
// not inject the bounded materialization credential into the sandbox, and its
// read-only-ness is soft afterward (a 0444 file the runtime user can chmod
// back).
//
// UPDATE-WITH: mount.go (fuse_bind command), verify.go (local_copy command),
// credential.go (the minter fuse_bind requires).
type ResourceProjectionLevel string

const (
	ResourceProjectionLevelFUSEBind  ResourceProjectionLevel = "fuse_bind"
	ResourceProjectionLevelLocalCopy ResourceProjectionLevel = "local_copy"
)

type ResourceProjectionPreparerConfig struct {
	Blob                    blob.BlobStore
	CredentialMinter        resourceCredentialMinter
	CommandRunner           driver.PreparationCommandRunner
	Bucket                  string
	AccountID               string
	CredentialTTL           time.Duration
	CredentialRefreshMargin time.Duration
	CommandTimeout          time.Duration
	RcloneVFSCacheMaxSize   string
	RcloneVFSMinFree        string
	ProjectionLevel         ResourceProjectionLevel
	Clock                   func() time.Time
}

type resourceCredentialMinter interface {
	Mint(context.Context, resourceprojection.CredentialMintRequest) (resourceprojection.CredentialMintResult, error)
}

type ResourceProjectionPreparer struct {
	blob                    blob.BlobStore
	copyExecutor            resourceprojection.CopyExecutor
	credentialMinter        resourceCredentialMinter
	commandRunner           driver.PreparationCommandRunner
	bucket                  string
	accountID               string
	credentialTTL           time.Duration
	credentialRefreshMargin time.Duration
	commandTimeout          time.Duration
	rcloneVFSCacheMaxSize   string
	rcloneVFSMinFree        string
	projectionLevel         ResourceProjectionLevel
	clock                   func() time.Time
}

func NewResourceProjectionPreparer(config ResourceProjectionPreparerConfig) (*ResourceProjectionPreparer, error) {
	if config.Blob == nil {
		return nil, &ConfigError{Message: "resource projection blob store is required"}
	}
	if config.CommandRunner == nil {
		return nil, &ConfigError{Message: "resource projection command runner is required"}
	}
	if config.ProjectionLevel == "" {
		config.ProjectionLevel = ResourceProjectionLevelFUSEBind
	}
	switch config.ProjectionLevel {
	case ResourceProjectionLevelFUSEBind, ResourceProjectionLevelLocalCopy:
	default:
		return nil, &ConfigError{Message: "resource projection level must be fuse_bind or local_copy"}
	}
	if config.CredentialMinter == nil {
		return nil, &ConfigError{Message: "resource projection credential minter is required"}
	}
	if strings.TrimSpace(config.Bucket) == "" {
		return nil, &ConfigError{Message: "resource projection bucket is required"}
	}
	if strings.TrimSpace(config.AccountID) == "" {
		return nil, &ConfigError{Message: "resource projection account id is required"}
	}
	if config.CredentialTTL <= 0 {
		return nil, &ConfigError{Message: "resource projection credential ttl must be positive"}
	}
	if config.CredentialRefreshMargin <= 0 {
		config.CredentialRefreshMargin = defaultResourceCredentialRefreshMargin
	}
	if config.CommandTimeout <= 0 {
		config.CommandTimeout = defaultResourcePreparationTimeout
	}
	if strings.TrimSpace(config.RcloneVFSCacheMaxSize) == "" {
		config.RcloneVFSCacheMaxSize = defaultRcloneVFSCacheMaxSize
	}
	if strings.TrimSpace(config.RcloneVFSMinFree) == "" {
		config.RcloneVFSMinFree = defaultRcloneVFSMinFree
	}
	clock := config.Clock
	if clock == nil {
		clock = func() time.Time { return storage.Now() }
	}
	return &ResourceProjectionPreparer{
		blob:                    config.Blob,
		copyExecutor:            resourceprojection.CopyExecutor{Blob: config.Blob},
		credentialMinter:        config.CredentialMinter,
		commandRunner:           config.CommandRunner,
		bucket:                  strings.TrimSpace(config.Bucket),
		accountID:               strings.TrimSpace(config.AccountID),
		credentialTTL:           config.CredentialTTL,
		credentialRefreshMargin: config.CredentialRefreshMargin,
		commandTimeout:          config.CommandTimeout,
		rcloneVFSCacheMaxSize:   strings.TrimSpace(config.RcloneVFSCacheMaxSize),
		rcloneVFSMinFree:        strings.TrimSpace(config.RcloneVFSMinFree),
		projectionLevel:         config.ProjectionLevel,
		clock:                   clock,
	}, nil
}

// PrepareSessionResources projects the session's file resources, choosing one
// of three credential/mount modes for the fuse_bind level. Minting is a
// sub-step of mounting, so an incremental add over an already-live mount
// performs no mint and carries the prior expiry forward.
//
//	recorded credential   staging mount    mode
//	(ResourceCredExpiresAt) (MountAlive)
//	nil                    n/a             fresh mint + mount: mint a new
//	                                       credential and mount+bind. Covers the
//	                                       first file resource and cold-return
//	                                       (the caller nils the recorded expiry).
//	non-nil                alive           reuse-existing: no mint, carry
//	                                       ResourceCredExpiresAt forward. The
//	                                       prefix-scoped credential already
//	                                       covers any newly added object.
//	non-nil                dead            force-remount (live rotation): tear
//	                                       down binds and staging
//	                                       (writeLiveRotationTeardown), then
//	                                       re-mint and re-mount.
//
// UPDATE-WITH: mount.go (MountBindVerifyCommand, MountAliveCommand),
// credential.go (Mint), rotation.go (live-rotation teardown).
func (p *ResourceProjectionPreparer) PrepareSessionResources(ctx context.Context, setup sandbox.SandboxSetup, handle sandbox.ProviderHandle) (preparedSetup sandbox.ResourceSetup, returnErr error) {
	prepared := cloneResourceSetupFromSandbox(setup.Resources)
	if p == nil {
		return prepared, &ConfigError{Message: "resource projection preparer is required"}
	}
	skillsMaterialized := false
	defer func() {
		if returnErr != nil && skillsMaterialized && handle.SandboxID != "" {
			returnErr = errors.Join(returnErr, p.cleanupSkills(ctx, handle, setup.Resources.Skills))
		}
	}()
	deletedTargets, err := deletedFileCleanupTargets(setup.WorkspaceID, setup.SessionID, setup.Resources.DeletedFiles)
	if err != nil {
		return sandbox.ResourceSetup{}, projectionFailureFromError(err)
	}
	planRequest := resourceprojection.PlanRequest{
		WorkspaceID:        string(setup.WorkspaceID),
		SessionID:          setup.SessionID,
		Files:              fileResourcesFromSandbox(setup.Resources.Files),
		GitHubRepositories: githubResourcesFromSandbox(setup.Resources.GitHubRepositories),
		MemoryStores:       memoryResourcesFromSandbox(setup.Resources.MemoryStores),
	}
	plan, err := resourceprojection.BuildPlan(planRequest)
	if err != nil {
		return sandbox.ResourceSetup{}, projectionFailureFromError(err)
	}
	if err := validateSkillMounts(setup.Resources.Skills); err != nil {
		return sandbox.ResourceSetup{}, projectionFailureFromError(err)
	}
	if len(setup.Resources.Skills) > 0 {
		if handle.SandboxID == "" {
			return sandbox.ResourceSetup{}, missingProviderSandboxIDError()
		}
		if err := p.materializeSkills(ctx, handle, setup.Resources.Skills); err != nil {
			return sandbox.ResourceSetup{}, projectionFailureFromError(err)
		}
		skillsMaterialized = true
	}
	if len(setup.Resources.Files) == 0 && len(setup.Resources.MemoryStores) == 0 {
		if len(deletedTargets) > 0 || setup.Resources.ResourceCredExpiresAt != nil {
			if handle.SandboxID == "" {
				return sandbox.ResourceSetup{}, missingProviderSandboxIDError()
			}
			if err := p.cleanupDeletedFiles(ctx, handle, deletedTargets, true, setup.ResourceCleanup); err != nil {
				return sandbox.ResourceSetup{}, err
			}
			prepared.Files = nil
			prepared.DeletedFiles = nil
		}
		prepared.ResourceRootsJSON = "[]"
		prepared.ResourceCredExpiresAt = nil
		return p.ensureMaterializationCredential(ctx, setup, prepared)
	}
	if len(setup.Resources.Files) == 0 {
		if len(deletedTargets) > 0 || setup.Resources.ResourceCredExpiresAt != nil {
			if handle.SandboxID == "" {
				return sandbox.ResourceSetup{}, missingProviderSandboxIDError()
			}
			if err := p.cleanupDeletedFiles(ctx, handle, deletedTargets, true, setup.ResourceCleanup); err != nil {
				return sandbox.ResourceSetup{}, err
			}
			prepared.DeletedFiles = nil
		}
		prepared.ResourceRootsJSON = plan.ResourceRootsJSON
		prepared.ResourceCredExpiresAt = nil
		return p.ensureMaterializationCredential(ctx, setup, prepared)
	}
	if handle.SandboxID == "" {
		return sandbox.ResourceSetup{}, missingProviderSandboxIDError()
	}
	if len(deletedTargets) > 0 {
		if err := p.cleanupDeletedFiles(ctx, handle, deletedTargets, false, setup.ResourceCleanup); err != nil {
			return sandbox.ResourceSetup{}, err
		}
	}
	reuseExistingCredential := false
	if setup.Resources.ResourceCredExpiresAt != nil {
		err := p.commandRunner.RunPreparationCommand(ctx, driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}, resourceprojection.MountAliveCommand(), nil, p.commandTimeout)
		reuseExistingCredential = err == nil
	}
	copyActions := resourceprojection.ActionsOfType(plan.Actions, resourceprojection.ActionCopyObject)
	copiedThisAttempt := make([]resourceprojection.Action, 0, len(copyActions))
	for _, action := range copyActions {
		result, err := p.copyExecutor.CopyIfNeeded(ctx, action)
		if err != nil {
			_ = p.deleteCopiedSessionObjects(ctx, copiedThisAttempt)
			return sandbox.ResourceSetup{}, projectionFailureFromError(err)
		}
		if result.Status == resourceprojection.CopyStatusCopied || result.Status == resourceprojection.CopyStatusRecopiedMismatch {
			copiedThisAttempt = append(copiedThisAttempt, action)
		}
	}
	if p.projectionLevel == ResourceProjectionLevelLocalCopy {
		if err := p.stageLocalCopyResources(ctx, handle, plan, copyActions); err != nil {
			_ = p.cleanupLocalCopyStage(ctx, handle, plan)
			_ = p.deleteCopiedSessionObjects(ctx, copiedThisAttempt)
			return sandbox.ResourceSetup{}, projectionFailureFromError(err)
		}
		if err := resourceprojection.RunLocalCopyVerify(ctx, p.commandRunner, driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}, plan, p.commandTimeout); err != nil {
			_ = p.cleanupLocalCopyStage(ctx, handle, plan)
			_ = p.deleteCopiedSessionObjects(ctx, copiedThisAttempt)
			return sandbox.ResourceSetup{}, labelPreparationCommandError(err, "local_copy_verify")
		}
		prepared.Files = nil
		prepared.DeletedFiles = nil
		prepared.ResourceRootsJSON = plan.ResourceRootsJSON
		prepared.ResourceCredExpiresAt = nil
		return p.ensureMaterializationCredential(ctx, setup, prepared)
	}
	env := map[string]string{}
	expiresAt := time.Time{}
	if reuseExistingCredential {
		expiresAt = setup.Resources.ResourceCredExpiresAt.UTC()
	} else {
		mintResult, err := p.credentialMinter.Mint(ctx, resourceprojection.CredentialMintRequest{
			WorkspaceID: string(setup.WorkspaceID),
			SessionID:   setup.SessionID,
			TTL:         p.credentialTTL,
			Now:         p.clock().UTC(),
		})
		if err != nil {
			_ = p.deleteCopiedSessionObjects(ctx, copiedThisAttempt)
			return sandbox.ResourceSetup{}, err
		}
		env = resourceprojection.RcloneEnv(p.accountID, mintResult.Credential)
		expiresAt = mintResult.ExpiresAt.UTC()
	}
	if err := resourceprojection.RunMountBindVerify(ctx, p.commandRunner, driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}, plan, resourceprojection.MountBindVerifyCommandConfig{
		Bucket:                  p.bucket,
		RcloneVFSCacheMaxSize:   p.rcloneVFSCacheMaxSize,
		RcloneVFSMinFree:        p.rcloneVFSMinFree,
		ForceRemount:            !reuseExistingCredential,
		ReuseExistingCredential: reuseExistingCredential,
	}, env, p.commandTimeout); err != nil {
		_ = p.deleteCopiedSessionObjects(ctx, copiedThisAttempt)
		return sandbox.ResourceSetup{}, labelPreparationCommandError(err, "mount_bind_verify")
	}
	prepared.Files = nil
	prepared.DeletedFiles = nil
	prepared.ResourceRootsJSON = plan.ResourceRootsJSON
	prepared.ResourceCredExpiresAt = &expiresAt
	return prepared, nil
}

func (p *ResourceProjectionPreparer) ensureMaterializationCredential(ctx context.Context, setup sandbox.SandboxSetup, prepared sandbox.ResourceSetup) (sandbox.ResourceSetup, error) {
	if prepared.ResourceCredExpiresAt != nil {
		return prepared, nil
	}
	now := p.clock().UTC()
	mintResult, err := p.credentialMinter.Mint(ctx, resourceprojection.CredentialMintRequest{
		WorkspaceID: string(setup.WorkspaceID),
		SessionID:   setup.SessionID,
		TTL:         p.credentialTTL,
		Now:         now,
	})
	if err != nil {
		return sandbox.ResourceSetup{}, err
	}
	if !mintResult.ExpiresAt.After(now) {
		return sandbox.ResourceSetup{}, &ConfigError{Message: "resource projection credential expiry must be in the future"}
	}
	expiresAt := mintResult.ExpiresAt.UTC()
	prepared.ResourceCredExpiresAt = &expiresAt
	return prepared, nil
}

func (p *ResourceProjectionPreparer) CompensateSessionResourcePreparation(ctx context.Context, setup sandbox.SandboxSetup, handle sandbox.ProviderHandle) error {
	if p == nil {
		return &ConfigError{Message: "resource projection preparer is required"}
	}
	if len(setup.Resources.Files) == 0 && len(setup.Resources.Skills) == 0 {
		return nil
	}
	if handle.SandboxID == "" {
		return missingProviderSandboxIDError()
	}
	var joined error
	if len(setup.Resources.Files) > 0 {
		plan, err := resourceprojection.BuildPlan(resourceprojection.PlanRequest{
			WorkspaceID:        string(setup.WorkspaceID),
			SessionID:          setup.SessionID,
			Files:              fileResourcesFromSandbox(setup.Resources.Files),
			GitHubRepositories: githubResourcesFromSandbox(setup.Resources.GitHubRepositories),
			MemoryStores:       memoryResourcesFromSandbox(setup.Resources.MemoryStores),
		})
		if err != nil {
			return projectionFailureFromError(err)
		}
		if err := resourceprojection.RunFileProjectionCompensation(ctx, p.commandRunner, driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}, plan, p.commandTimeout); err != nil {
			joined = errors.Join(joined, labelPreparationCommandError(err, "file_projection_compensation"))
		}
	}
	if len(setup.Resources.Skills) > 0 {
		joined = errors.Join(joined, p.cleanupSkills(ctx, handle, setup.Resources.Skills))
	}
	return joined
}

// DeleteSessionResourceCopiesForGC removes the session-copy prefix only from a
// true session teardown/GC path. Sandbox release must not call this: cold-return
// relies on session copies surviving sandbox recycle.
func (p *ResourceProjectionPreparer) DeleteSessionResourceCopiesForGC(ctx context.Context, setup sandbox.SandboxSetup) error {
	if p == nil {
		return &ConfigError{Message: "resource projection preparer is required"}
	}
	if setup.WorkspaceID == "" {
		return &ConfigError{Message: "resource projection cleanup workspace_id is required"}
	}
	if setup.SessionID == "" {
		return &ConfigError{Message: "resource projection cleanup session_id is required"}
	}
	return p.blob.DeletePrefix(ctx, resourceprojection.SessionPrefix(string(setup.WorkspaceID), setup.SessionID))
}

func validateSkillMounts(skills []sandbox.SkillMount) error {
	seen := map[string]string{}
	for _, mount := range skills {
		if mount.SkillID == "" || mount.SkillVersionID == "" || mount.Version == "" || mount.BlobKey == "" || mount.SizeBytes <= 0 || mount.SHA256 == "" {
			return &resourceprojection.PlanError{Code: "invalid_skill", Message: "skill materialization reference is incomplete"}
		}
		if len(mount.SHA256) != sha256.Size*2 {
			return &resourceprojection.PlanError{Code: "invalid_skill", ResourceID: mount.SkillID, Message: "skill materialization sha256 is invalid"}
		}
		directory, err := validateSkillDirectory(mount.Directory)
		if err != nil {
			return &resourceprojection.PlanError{Code: "invalid_skill_directory", ResourceID: mount.SkillID, Path: "/skills/" + mount.Directory, Message: err.Error()}
		}
		mountPath := "/skills/" + directory
		if other, exists := seen[mountPath]; exists {
			return &resourceprojection.PlanError{
				Code:            "duplicate_skill_directory",
				ResourceID:      mount.SkillID,
				OtherResourceID: other,
				Path:            mountPath,
				OtherPath:       mountPath,
				Message:         "multiple skill bundles claim the same /skills directory",
			}
		}
		seen[mountPath] = mount.SkillID
	}
	return nil
}

func validateSkillDirectory(directory string) (string, error) {
	if directory == "" || strings.Contains(directory, "\x00") {
		return "", errors.New("skill directory is required")
	}
	if strings.Contains(directory, "/") || directory == "." || directory == ".." || path.Clean(directory) != directory {
		return "", errors.New("skill directory must be one clean relative path segment")
	}
	return directory, nil
}

func (p *ResourceProjectionPreparer) materializeSkills(ctx context.Context, handle sandbox.ProviderHandle, skills []sandbox.SkillMount) error {
	stager, ok := p.commandRunner.(driver.PreparationFileStager)
	if !ok {
		return &ConfigError{Message: "skill materialization requires a preparation file stager"}
	}
	target := driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}
	for _, mount := range skills {
		archive, err := p.skillArchiveForSandbox(ctx, mount)
		if err != nil {
			return err
		}
		if err := stager.StagePreparationFile(ctx, target, skillProjectionArchivePath(mount), bytes.NewReader(archive)); err != nil {
			return errors.Join(err, p.cleanupSkills(ctx, handle, skills))
		}
	}
	if err := p.commandRunner.RunPreparationCommand(ctx, target, skillMaterializationCommand(skills), nil, p.commandTimeout); err != nil {
		return errors.Join(labelPreparationCommandError(err, "skill_materialization"), p.cleanupSkills(ctx, handle, skills))
	}
	return nil
}

// labelPreparationCommandError prepends the engine-authored name of the
// failing preparation command to a provider error's safe message. The runner
// deliberately never surfaces command output (it can embed capability
// material), so the label is what makes a dead-lettered preparation name its
// culprit.
func labelPreparationCommandError(err error, label string) error {
	var providerErr *sandbox.ProviderError
	if errors.As(err, &providerErr) {
		labeled := *providerErr
		labeled.SafeMessage = label + ": " + providerErr.SafeMessage
		return &labeled
	}
	return fmt.Errorf("%s: %w", label, err)
}

func (p *ResourceProjectionPreparer) skillArchiveForSandbox(ctx context.Context, mount sandbox.SkillMount) ([]byte, error) {
	reader, err := p.blob.Get(ctx, mount.BlobKey)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	limited := io.LimitReader(reader, skill.MaxNormalizedZipBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > skill.MaxNormalizedZipBytes {
		return nil, &resourceprojection.PlanError{Code: "skill_package_too_large", ResourceID: mount.SkillID, Message: "skill package exceeds the normalized size cap"}
	}
	if mount.SizeBytes > 0 && int64(len(body)) != mount.SizeBytes {
		return nil, &resourceprojection.PlanError{Code: "skill_package_size_mismatch", ResourceID: mount.SkillID, Message: "skill package size does not match the skill version row"}
	}
	if mount.SHA256 != "" {
		sum := sha256.Sum256(body)
		if !strings.EqualFold(hex.EncodeToString(sum[:]), mount.SHA256) {
			return nil, &resourceprojection.PlanError{Code: "skill_package_sha256_mismatch", ResourceID: mount.SkillID, Message: "skill package sha256 does not match the skill version row"}
		}
	}
	return normalizedSkillZipToTarGz(body, mount)
}

func normalizedSkillZipToTarGz(body []byte, mount sandbox.SkillMount) ([]byte, error) {
	directory, err := validateSkillDirectory(mount.Directory)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(body)
	zipReader, err := zip.NewReader(reader, int64(len(body)))
	if err != nil {
		return nil, &resourceprojection.PlanError{Code: "skill_package_invalid", ResourceID: mount.SkillID, Message: "skill package zip is malformed"}
	}
	var output bytes.Buffer
	gzipWriter := gzip.NewWriter(&output)
	tarWriter := tar.NewWriter(gzipWriter)
	writtenDirs := map[string]bool{}
	var expanded int64
	var writeDir func(name string) error
	writeDir = func(name string) error {
		name = strings.TrimSuffix(name, "/")
		if name == "" || writtenDirs[name] {
			return nil
		}
		parent := path.Dir(name)
		if parent != "." && parent != "/" {
			if err := writeDir(parent); err != nil {
				return err
			}
		}
		if err := tarWriter.WriteHeader(&tar.Header{Name: name + "/", Typeflag: tar.TypeDir, Mode: 0o555}); err != nil {
			return err
		}
		writtenDirs[name] = true
		return nil
	}
	for _, zipFile := range zipReader.File {
		rawName := strings.TrimSuffix(zipFile.Name, "/")
		cleanName := path.Clean(rawName)
		if rawName == "" || rawName == "." || cleanName != rawName || path.IsAbs(cleanName) || strings.Contains(rawName, "\x00") || strings.HasPrefix(cleanName, "../") {
			return nil, &resourceprojection.PlanError{Code: "skill_package_invalid", ResourceID: mount.SkillID, Message: "skill package entry path is invalid"}
		}
		if cleanName != directory && !strings.HasPrefix(cleanName, directory+"/") {
			return nil, &resourceprojection.PlanError{Code: "skill_package_invalid", ResourceID: mount.SkillID, Message: "skill package root does not match the skill version directory"}
		}
		if zipFile.FileInfo().IsDir() || strings.HasSuffix(zipFile.Name, "/") {
			if err := writeDir(cleanName); err != nil {
				return nil, err
			}
			continue
		}
		if err := writeDir(path.Dir(cleanName)); err != nil {
			return nil, err
		}
		if zipFile.UncompressedSize64 > uint64(skill.MaxPackageEntryBytes) {
			return nil, &resourceprojection.PlanError{Code: "skill_package_too_large", ResourceID: mount.SkillID, Message: "skill package entry exceeds the size cap"}
		}
		expanded += int64(zipFile.UncompressedSize64)
		if expanded > skill.MaxPackageExpandedBytes {
			return nil, &resourceprojection.PlanError{Code: "skill_package_too_large", ResourceID: mount.SkillID, Message: "skill package exceeds the expanded size cap"}
		}
		if err := tarWriter.WriteHeader(&tar.Header{Name: cleanName, Typeflag: tar.TypeReg, Mode: 0o444, Size: int64(zipFile.UncompressedSize64)}); err != nil {
			return nil, err
		}
		fileReader, err := zipFile.Open()
		if err != nil {
			return nil, &resourceprojection.PlanError{Code: "skill_package_invalid", ResourceID: mount.SkillID, Message: "skill package entry is unreadable"}
		}
		declaredSize := int64(zipFile.UncompressedSize64)
		copied, copyErr := io.Copy(tarWriter, io.LimitReader(fileReader, declaredSize+1))
		closeErr := fileReader.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		if copied != declaredSize {
			return nil, &resourceprojection.PlanError{Code: "skill_package_invalid", ResourceID: mount.SkillID, Message: "skill package entry size mismatch"}
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	if !writtenDirs[directory] {
		return nil, &resourceprojection.PlanError{Code: "skill_package_invalid", ResourceID: mount.SkillID, Message: "skill package is missing the declared directory"}
	}
	if err := tarWriter.Close(); err != nil {
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func skillMaterializationCommand(skills []sandbox.SkillMount) string {
	// Daytona executes this as the runtime user: every operation on the
	// root-owned /skills tree runs under sudo, while stage work stays
	// unprivileged in the runtime-user-owned staging root.
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin\n")
	b.WriteString("sudo mkdir -p /skills\n")
	b.WriteString("sudo chown root:root /skills\n")
	b.WriteString("sudo chmod 0755 /skills\n")
	for _, mount := range skills {
		stage := skillProjectionStagePath(mount)
		destination := "/skills/" + mount.Directory
		extracted := stage + "/extract/" + mount.Directory
		b.WriteString("rm -rf -- " + shellQuote(stage+"/extract") + "\n")
		b.WriteString("mkdir -p -- " + shellQuote(stage+"/extract") + "\n")
		b.WriteString("tar -xzf " + shellQuote(skillProjectionArchivePath(mount)) + " -C " + shellQuote(stage+"/extract") + "\n")
		b.WriteString("test -d " + shellQuote(extracted) + "\n")
		b.WriteString("sudo rm -rf -- " + shellQuote(destination) + "\n")
		b.WriteString("sudo mv -- " + shellQuote(extracted) + " " + shellQuote(destination) + "\n")
		b.WriteString("sudo chown -R root:root " + shellQuote(destination) + "\n")
		b.WriteString("sudo find " + shellQuote(destination) + " -type d -exec chmod 0555 {} +\n")
		b.WriteString("sudo find " + shellQuote(destination) + " -type f -exec chmod 0444 {} +\n")
		b.WriteString("test -r " + shellQuote(destination+"/SKILL.md") + "\n")
		b.WriteString("rm -rf -- " + shellQuote(stage) + "\n")
	}
	return b.String()
}

func (p *ResourceProjectionPreparer) cleanupSkills(ctx context.Context, handle sandbox.ProviderHandle, skills []sandbox.SkillMount) error {
	if len(skills) == 0 {
		return nil
	}
	return p.commandRunner.RunPreparationCommand(ctx, driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}, skillProjectionCleanupCommand(skills), nil, p.commandTimeout)
}

func skillProjectionCleanupCommand(skills []sandbox.SkillMount) string {
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin\n")
	for _, mount := range skills {
		if directory, err := validateSkillDirectory(mount.Directory); err == nil {
			b.WriteString("sudo rm -rf -- " + shellQuote("/skills/"+directory) + "\n")
		}
		b.WriteString("rm -rf -- " + shellQuote(skillProjectionStagePath(mount)) + "\n")
	}
	return b.String()
}

func skillProjectionStagePath(mount sandbox.SkillMount) string {
	sum := sha256.Sum256([]byte(mount.SkillID + "\x00" + mount.Version + "\x00" + mount.Directory))
	return skillProjectionStagingRoot + "/" + hex.EncodeToString(sum[:8])
}

func skillProjectionArchivePath(mount sandbox.SkillMount) string {
	return skillProjectionStagePath(mount) + "/package.tar.gz"
}

func missingProviderSandboxIDError() error {
	return &sandbox.ProviderError{
		Provider:    internalSandboxProviderName,
		Stage:       sandbox.StageMountResources,
		Kind:        sandbox.ProviderErrorInvalidRequest,
		Retryable:   false,
		SafeMessage: "resource projection provider sandbox id is required",
	}
}

func deletedFileCleanupTargets(ws workspace.ID, sessionID string, files []sandbox.FileMount) ([]resourceprojection.DeletedFileCleanupTarget, error) {
	targets := make([]resourceprojection.DeletedFileCleanupTarget, 0, len(files))
	for _, file := range files {
		if file.ResourceID == "" {
			return nil, &resourceprojection.PlanError{Code: "invalid_resource", Message: "deleted resource_id is required"}
		}
		if file.SessionFileID == "" {
			return nil, &resourceprojection.PlanError{Code: "invalid_resource", ResourceID: file.ResourceID, Message: "deleted session file_id is required"}
		}
		mountPath := file.MountPath
		if mountPath == "" {
			mountPath = "/mnt/session/uploads/" + file.SessionFileID
		}
		if err := resourceprojection.ValidateCleanupMountPath(file.ResourceID, mountPath); err != nil {
			return nil, err
		}
		targets = append(targets, resourceprojection.DeletedFileCleanupTarget{
			ResourceID:     file.ResourceID,
			MountPath:      mountPath,
			DestinationKey: resourceprojection.SessionResourceKey(string(ws), sessionID, file.ResourceID),
		})
	}
	return targets, nil
}

func (p *ResourceProjectionPreparer) cleanupDeletedFiles(ctx context.Context, handle sandbox.ProviderHandle, targets []resourceprojection.DeletedFileCleanupTarget, unmountStaging bool, cleanup sandbox.SessionResourceCleanupCoordinator) error {
	if len(targets) == 0 && !unmountStaging {
		return nil
	}
	for _, target := range targets {
		remove := func(ctx context.Context) error {
			if err := resourceprojection.RunDeletedFileCleanup(ctx, p.commandRunner, driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}, []resourceprojection.DeletedFileCleanupTarget{target}, false, p.commandTimeout); err != nil {
				return labelPreparationCommandError(err, "deleted_file_cleanup")
			}
			if target.DestinationKey == "" {
				return nil
			}
			if err := p.blob.Delete(ctx, target.DestinationKey); err != nil {
				var notFound *blob.NotFoundError
				if !errors.As(err, &notFound) {
					return err
				}
			}
			return nil
		}
		if cleanup == nil {
			if err := remove(ctx); err != nil {
				return err
			}
			continue
		}
		if err := cleanup.CleanupSessionResource(ctx, target.ResourceID, remove); err != nil {
			return err
		}
	}
	if unmountStaging {
		if err := resourceprojection.RunDeletedFileCleanup(ctx, p.commandRunner, driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}, nil, true, p.commandTimeout); err != nil {
			return labelPreparationCommandError(err, "deleted_file_cleanup")
		}
		return nil
	}
	return nil
}

func (p *ResourceProjectionPreparer) deleteCopiedSessionObjects(ctx context.Context, copyActions []resourceprojection.Action) error {
	var joined error
	for _, action := range copyActions {
		if action.DestinationKey == "" {
			continue
		}
		if err := p.blob.Delete(ctx, action.DestinationKey); err != nil {
			var notFound *blob.NotFoundError
			if !errors.As(err, &notFound) {
				joined = errors.Join(joined, err)
			}
		}
	}
	return joined
}

func (p *ResourceProjectionPreparer) stageLocalCopyResources(ctx context.Context, handle sandbox.ProviderHandle, plan resourceprojection.Plan, copyActions []resourceprojection.Action) error {
	stager, ok := p.commandRunner.(driver.PreparationFileStager)
	if !ok {
		return &ConfigError{Message: "resource projection local_copy requires a preparation file stager"}
	}
	destinationByResourceID := make(map[string]string, len(copyActions))
	for _, action := range copyActions {
		destinationByResourceID[action.ResourceID] = action.DestinationKey
	}
	target := driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}
	for _, action := range resourceprojection.ActionsOfType(plan.Actions, resourceprojection.ActionBind) {
		destinationKey := destinationByResourceID[action.ResourceID]
		if destinationKey == "" {
			return &resourceprojection.PlanError{Code: "invalid_resource", ResourceID: action.ResourceID, Message: "resource projection local_copy destination is missing"}
		}
		reader, err := p.blob.Get(ctx, destinationKey)
		if err != nil {
			return err
		}
		stageErr := stager.StagePreparationFile(ctx, target, resourceprojection.LocalCopyStagePath(plan, action.ResourceID), reader)
		closeErr := reader.Close()
		if stageErr != nil {
			return stageErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (p *ResourceProjectionPreparer) cleanupLocalCopyStage(ctx context.Context, handle sandbox.ProviderHandle, plan resourceprojection.Plan) error {
	return p.commandRunner.RunPreparationCommand(ctx, driver.PreparationCommandTarget{ProviderSandboxID: handle.SandboxID}, "rm -rf -- "+shellQuote(resourceprojection.LocalCopyStageRoot(plan))+"\n", nil, p.commandTimeout)
}

func fileResourcesFromSandbox(files []sandbox.FileMount) []resourceprojection.FileResource {
	out := make([]resourceprojection.FileResource, 0, len(files))
	for _, file := range files {
		out = append(out, resourceprojection.FileResource{
			ResourceID:    file.ResourceID,
			SourceFileID:  file.SourceFileID,
			SessionFileID: file.SessionFileID,
			ObjectID:      file.ObjectID,
			MountPath:     file.MountPath,
		})
	}
	return out
}

func githubResourcesFromSandbox(repos []sandbox.GitHubRepositoryMount) []resourceprojection.GitHubRepositoryResource {
	out := make([]resourceprojection.GitHubRepositoryResource, 0, len(repos))
	for _, repo := range repos {
		out = append(out, resourceprojection.GitHubRepositoryResource{
			ResourceID: repo.ResourceID,
			URL:        repo.URL,
			MountPath:  repo.MountPath,
		})
	}
	return out
}

func memoryResourcesFromSandbox(stores []sandbox.MemoryStoreMount) []resourceprojection.MemoryStoreResource {
	out := make([]resourceprojection.MemoryStoreResource, 0, len(stores))
	for _, store := range stores {
		out = append(out, resourceprojection.MemoryStoreResource{
			ResourceID: store.ResourceID,
			MountPath:  store.MountPath,
		})
	}
	return out
}

func projectionFailureFromError(err error) error {
	if err == nil {
		return nil
	}
	var planErr *resourceprojection.PlanError
	if errors.As(err, &planErr) && planErr.Code != "" {
		return &resourceProjectionFailure{kind: planErr.Code, cause: err}
	}
	var copyErr *resourceprojection.CopyError
	if errors.As(err, &copyErr) && copyErr.Kind == "canonical_missing" {
		return &resourceProjectionFailure{kind: "canonical_missing", cause: err}
	}
	return err
}

type resourceProjectionFailure struct {
	kind  string
	cause error
}

func (e *resourceProjectionFailure) Error() string {
	if e == nil || e.kind == "" {
		return "resource projection failed"
	}
	return "resource projection failed: " + e.kind
}

func (e *resourceProjectionFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *resourceProjectionFailure) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, e.Error())
}

func (e *resourceProjectionFailure) PreparationFailureKind() string {
	if e == nil {
		return ""
	}
	return e.kind
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func cloneResourceSetupFromSandbox(in sandbox.ResourceSetup) sandbox.ResourceSetup {
	out := sandbox.ResourceSetup{
		Files:                     append([]sandbox.FileMount(nil), in.Files...),
		DeletedFiles:              append([]sandbox.FileMount(nil), in.DeletedFiles...),
		MemoryStores:              append([]sandbox.MemoryStoreMount(nil), in.MemoryStores...),
		DeletedMemoryStores:       append([]sandbox.MemoryStoreMount(nil), in.DeletedMemoryStores...),
		GitHubRepositories:        append([]sandbox.GitHubRepositoryMount(nil), in.GitHubRepositories...),
		DeletedGitHubRepositories: append([]sandbox.GitHubRepositoryMount(nil), in.DeletedGitHubRepositories...),
		Skills:                    append([]sandbox.SkillMount(nil), in.Skills...),
		ResourceRootsJSON:         in.ResourceRootsJSON,
	}
	if in.ResourceCredExpiresAt != nil {
		value := *in.ResourceCredExpiresAt
		out.ResourceCredExpiresAt = &value
	}
	return out
}
