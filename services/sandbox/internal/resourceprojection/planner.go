// Package resourceprojection plans read-only file-resource projection for
// sandbox session preparation.
//
// OWNS:
//   - The R2 key scheme: SessionPrefix, SessionResourcesPrefix,
//     SessionResourceKey (session copies) and CanonicalObjectKey (source
//     files/<workspace>/<object_id>).
//   - The plan-action vocabulary: copy_object, mint_credential, mount, bind,
//     verify (ActionType).
//   - The read-only R2 credential mint (prefix-scoped, TTL-bounded).
//   - Command generation for mount/bind/verify plus the teardown, live-rotation,
//     deleted-file cleanup, and local-copy variants.
//
// STATE MACHINE (per session-copy object, written by CopyExecutor.CopyIfNeeded
// in copy.go):
//
//	from destination state       -> CopyStatus            writer/action
//	absent                       -> copied                CopyObject (create-only)
//	present, metadata matches    -> skipped_matching      HeadObject compare, no write
//	present, size/ETag mismatch  -> recopied_mismatched   Delete then CopyObject
//	create-only conflict (race)  -> skipped_after_race     re-Head resolves to a match
//	canonical source absent      -> (error canonical_missing)
//
// INVARIANTS:
//   - The only hard cross-session/cross-workspace boundary is the minted R2
//     credential (object-read-only, scoped to the one session-resources prefix,
//     TTL-bounded). Every in-sandbox read-only guard (the rclone mount's
//     --read-only flag that the binds inherit, file ownership, chmod) is soft
//     and user-reversible; the runtime user has passwordless sudo.
//   - The plan stage fails atomically with zero R2/filesystem mutation. An
//     execute-stage failure may leave partial per-sandbox copies/binds; the next
//     attempt reconciles them idempotently (copy-if-absent plus the mount/bind
//     guards) and never rolls them back.
//   - This package reads only canonical files/<workspace>/<object_id> keys and
//     writes only session-prefix keys; canonical bytes are never written here.
//   - Session copies survive sandbox recycle, so cold-return recomputes the
//     mount and binds from durable rows without re-copying, and a surviving
//     session copy shields the session when its canonical object is later
//     deleted.
//
// UPDATE-WITH: copy.go, credential.go, mount.go, verify.go, rotation.go,
// teardown.go, services/sandbox/resource_projection_preparer.go
// (orchestrator), services/bridge/bridge_api_tools.go (readiness
// gate), internal/blob/blob.go (the CopyObject/HeadObject boundary).
package resourceprojection

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"unicode"
)

const (
	defaultUploadRoot = "/mnt/session/uploads"
	sessionKeySuffix  = "file"
	stagingMountRoot  = "/mnt/tetral/r2"
	memoryRoot        = "/mnt/memory"
	memoryStagingRoot = "/mnt/memory/.staging"
)

type ActionType string

const (
	ActionCopyObject     ActionType = "copy_object"
	ActionMintCredential ActionType = "mint_credential" //nolint:gosec // Projection plan action enum, not credential material.
	ActionMount          ActionType = "mount"
	ActionBind           ActionType = "bind"
	ActionVerify         ActionType = "verify"
)

type PlanRequest struct {
	WorkspaceID string
	SessionID   string

	Files              []FileResource
	GitHubRepositories []GitHubRepositoryResource
	MemoryStores       []MemoryStoreResource
}

// FileResource is the database row shape the pure planner needs. ObjectID is
// resolved by the caller from source_file_id -> files.object_id before planning.
type FileResource struct {
	ResourceID    string
	SourceFileID  string
	SessionFileID string
	ObjectID      string
	MountPath     string
}

type GitHubRepositoryResource struct {
	ResourceID     string
	URL            string
	RepositoryName string
	MountPath      string
}

type MemoryStoreResource struct {
	ResourceID string
	MountPath  string
}

type Plan struct {
	SessionPrefix     string
	ResourcePrefix    string
	ResourceRootsJSON string
	ResourceRoots     []Root
	Actions           []Action
}

type Root struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
}

type Action struct {
	Type           ActionType
	ResourceID     string
	SourceKey      string
	DestinationKey string
	Prefix         string
	MountPath      string
	StagingPath    string
}

type PlanError struct {
	Code            string
	ResourceID      string
	OtherResourceID string
	Path            string
	OtherPath       string
	Message         string
}

func (e *PlanError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return "resource projection plan failed: " + e.Code
	}
	return "resource projection plan failed"
}

type plannedFile struct {
	FileResource
	MountPath      string
	SourceKey      string
	DestinationKey string
	StagingPath    string
}

type plannedMemoryStore struct {
	MemoryStoreResource
	MountPath string
}

type plannedGitHubRepository struct {
	GitHubRepositoryResource
	MountPath string
}

func BuildPlan(request PlanRequest) (Plan, error) {
	if strings.TrimSpace(request.WorkspaceID) == "" {
		return Plan{}, &PlanError{Code: "invalid_identity", Message: "workspace_id is required"}
	}
	if strings.TrimSpace(request.SessionID) == "" {
		return Plan{}, &PlanError{Code: "invalid_identity", Message: "session_id is required"}
	}
	if len(request.Files) == 0 && len(request.MemoryStores) == 0 {
		if err := rejectCollisions(nil, request.GitHubRepositories, nil); err != nil {
			return Plan{}, err
		}
		return Plan{
			SessionPrefix:  SessionPrefix(request.WorkspaceID, request.SessionID),
			ResourcePrefix: SessionResourcesPrefix(request.WorkspaceID, request.SessionID),
		}, nil
	}
	files := make([]plannedFile, 0, len(request.Files))
	for _, file := range request.Files {
		resolved, err := resolveFile(request.WorkspaceID, request.SessionID, file)
		if err != nil {
			return Plan{}, err
		}
		files = append(files, resolved)
	}
	memories := make([]plannedMemoryStore, 0, len(request.MemoryStores))
	for _, store := range request.MemoryStores {
		resolved, err := resolveMemoryStore(store)
		if err != nil {
			return Plan{}, err
		}
		memories = append(memories, resolved)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].MountPath == files[j].MountPath {
			return files[i].ResourceID < files[j].ResourceID
		}
		return files[i].MountPath < files[j].MountPath
	})
	sort.Slice(memories, func(i, j int) bool {
		if memories[i].MountPath == memories[j].MountPath {
			return memories[i].ResourceID < memories[j].ResourceID
		}
		return memories[i].MountPath < memories[j].MountPath
	})
	if err := rejectCollisions(files, request.GitHubRepositories, memories); err != nil {
		return Plan{}, err
	}
	roots := make([]Root, 0, len(files))
	for _, file := range files {
		roots = append(roots, Root{Path: file.MountPath, Mode: "read"})
	}
	rootsJSON, err := json.Marshal(roots)
	if err != nil {
		return Plan{}, fmt.Errorf("marshal resource roots: %w", err)
	}
	plan := Plan{
		SessionPrefix:     SessionPrefix(request.WorkspaceID, request.SessionID),
		ResourcePrefix:    SessionResourcesPrefix(request.WorkspaceID, request.SessionID),
		ResourceRootsJSON: string(rootsJSON),
		ResourceRoots:     roots,
	}
	if len(files) == 0 {
		return plan, nil
	}
	for _, file := range files {
		plan.Actions = append(plan.Actions, Action{
			Type:           ActionCopyObject,
			ResourceID:     file.ResourceID,
			SourceKey:      file.SourceKey,
			DestinationKey: file.DestinationKey,
		})
	}
	plan.Actions = append(plan.Actions,
		Action{Type: ActionMintCredential, Prefix: plan.ResourcePrefix},
		Action{Type: ActionMount, Prefix: plan.ResourcePrefix, MountPath: stagingMountRoot},
	)
	for _, file := range files {
		plan.Actions = append(plan.Actions, Action{
			Type:        ActionBind,
			ResourceID:  file.ResourceID,
			MountPath:   file.MountPath,
			StagingPath: file.StagingPath,
		})
	}
	for _, file := range files {
		plan.Actions = append(plan.Actions, Action{
			Type:       ActionVerify,
			ResourceID: file.ResourceID,
			MountPath:  file.MountPath,
		})
	}
	return plan, nil
}

func SessionPrefix(workspaceID string, sessionID string) string {
	return "workspaces/" + workspaceID + "/sessions/" + sessionID + "/"
}

func SessionResourcesPrefix(workspaceID string, sessionID string) string {
	return SessionPrefix(workspaceID, sessionID) + "resources/"
}

func SessionResourceKey(workspaceID string, sessionID string, resourceID string) string {
	return SessionResourcesPrefix(workspaceID, sessionID) + resourceID + "/" + sessionKeySuffix
}

func CanonicalObjectKey(workspaceID string, objectID string) string {
	return "files/" + workspaceID + "/" + objectID
}

func resolveFile(workspaceID string, sessionID string, file FileResource) (plannedFile, error) {
	if file.ResourceID == "" {
		return plannedFile{}, &PlanError{Code: "invalid_resource", Message: "resource_id is required"}
	}
	if file.SourceFileID == "" {
		return plannedFile{}, &PlanError{Code: "invalid_resource", ResourceID: file.ResourceID, Message: "source_file_id is required"}
	}
	if file.SessionFileID == "" {
		return plannedFile{}, &PlanError{Code: "invalid_resource", ResourceID: file.ResourceID, Message: "session file_id is required"}
	}
	if file.ObjectID == "" {
		return plannedFile{}, &PlanError{Code: "unresolved_object", ResourceID: file.ResourceID, Message: "source_file_id did not resolve to object_id"}
	}
	mountPath := file.MountPath
	if mountPath == "" {
		mountPath = defaultUploadRoot + "/" + file.SessionFileID
	}
	if err := validateMountPath(file.ResourceID, mountPath); err != nil {
		return plannedFile{}, err
	}
	return plannedFile{
		FileResource:   file,
		MountPath:      mountPath,
		SourceKey:      CanonicalObjectKey(workspaceID, file.ObjectID),
		DestinationKey: SessionResourceKey(workspaceID, sessionID, file.ResourceID),
		StagingPath:    stagingMountRoot + "/" + file.ResourceID + "/" + sessionKeySuffix,
	}, nil
}

func resolveMemoryStore(store MemoryStoreResource) (plannedMemoryStore, error) {
	if store.ResourceID == "" {
		return plannedMemoryStore{}, &PlanError{Code: "invalid_memory_mount_path", Message: "memory_store resource_id is required"}
	}
	if store.MountPath == "" || !strings.HasPrefix(store.MountPath, "/") || strings.Contains(store.MountPath, "\x00") {
		return plannedMemoryStore{}, &PlanError{Code: "invalid_memory_mount_path", ResourceID: store.ResourceID, Path: store.MountPath, Message: "memory_store mount_path must be absolute and valid"}
	}
	if clean := path.Clean(store.MountPath); clean != store.MountPath {
		return plannedMemoryStore{}, &PlanError{Code: "invalid_memory_mount_path", ResourceID: store.ResourceID, Path: store.MountPath, Message: "memory_store mount_path must be lexically clean"}
	}
	if store.MountPath == memoryRoot || !strings.HasPrefix(store.MountPath, memoryRoot+"/") {
		return plannedMemoryStore{}, &PlanError{Code: "invalid_memory_mount_path", ResourceID: store.ResourceID, Path: store.MountPath, OtherPath: memoryRoot, Message: "memory_store mount_path must name a store below /mnt/memory"}
	}
	if store.MountPath == memoryStagingRoot || pathContains(memoryStagingRoot, store.MountPath) {
		return plannedMemoryStore{}, &PlanError{Code: "reserved_memory_mount_path", ResourceID: store.ResourceID, Path: store.MountPath, OtherPath: memoryStagingRoot, Message: "memory_store mount_path overlaps memory projection staging"}
	}
	return plannedMemoryStore{MemoryStoreResource: store, MountPath: store.MountPath}, nil
}

func validateMountPath(resourceID string, mountPath string) error {
	if mountPath == "" || !strings.HasPrefix(mountPath, "/") || strings.Contains(mountPath, "\x00") {
		return &PlanError{Code: "invalid_mount_path", ResourceID: resourceID, Path: mountPath, Message: "mount_path must be absolute and valid"}
	}
	if clean := path.Clean(mountPath); clean != mountPath || clean == "/" {
		return &PlanError{Code: "invalid_mount_path", ResourceID: resourceID, Path: mountPath, Message: "mount_path must be lexically clean"}
	}
	return nil
}

func rejectCollisions(files []plannedFile, repos []GitHubRepositoryResource, memories []plannedMemoryStore) error {
	githubRepos := make([]plannedGitHubRepository, 0, len(repos))
	for _, repo := range repos {
		repoPath, err := resolveGitHubMountPath(repo)
		if err != nil {
			return err
		}
		if err := rejectReservedGitHubPath(repo.ResourceID, repoPath); err != nil {
			return err
		}
		planned := plannedGitHubRepository{GitHubRepositoryResource: repo, MountPath: repoPath}
		for _, existing := range githubRepos {
			if existing.MountPath == planned.MountPath {
				return &PlanError{
					Code:            "duplicate_github_mount_path",
					ResourceID:      existing.ResourceID,
					OtherResourceID: planned.ResourceID,
					Path:            existing.MountPath,
					OtherPath:       planned.MountPath,
					Message:         "github_repository resources resolve to the same mount_path",
				}
			}
			if pathContains(existing.MountPath, planned.MountPath) || pathContains(planned.MountPath, existing.MountPath) {
				return &PlanError{
					Code:            "nested_github_mount_path",
					ResourceID:      existing.ResourceID,
					OtherResourceID: planned.ResourceID,
					Path:            existing.MountPath,
					OtherPath:       planned.MountPath,
					Message:         "github_repository resources have nested mount_paths",
				}
			}
		}
		githubRepos = append(githubRepos, planned)
	}
	for i := range memories {
		for j := i + 1; j < len(memories); j++ {
			if memories[i].MountPath == memories[j].MountPath {
				return &PlanError{
					Code:            "duplicate_memory_mount_path",
					ResourceID:      memories[i].ResourceID,
					OtherResourceID: memories[j].ResourceID,
					Path:            memories[i].MountPath,
					OtherPath:       memories[j].MountPath,
					Message:         "memory_store resources resolve to the same mount_path",
				}
			}
			if pathContains(memories[i].MountPath, memories[j].MountPath) || pathContains(memories[j].MountPath, memories[i].MountPath) {
				return &PlanError{
					Code:            "nested_memory_mount_path",
					ResourceID:      memories[i].ResourceID,
					OtherResourceID: memories[j].ResourceID,
					Path:            memories[i].MountPath,
					OtherPath:       memories[j].MountPath,
					Message:         "memory_store resources have nested mount_paths",
				}
			}
		}
	}
	for i := range files {
		if err := rejectReservedPath(files[i]); err != nil {
			return err
		}
		for j := i + 1; j < len(files); j++ {
			if files[i].MountPath == files[j].MountPath {
				return &PlanError{
					Code:            "duplicate_mount_path",
					ResourceID:      files[i].ResourceID,
					OtherResourceID: files[j].ResourceID,
					Path:            files[i].MountPath,
					OtherPath:       files[j].MountPath,
					Message:         "file resources resolve to the same mount_path",
				}
			}
			if pathContains(files[i].MountPath, files[j].MountPath) || pathContains(files[j].MountPath, files[i].MountPath) {
				return &PlanError{
					Code:            "nested_mount_path",
					ResourceID:      files[i].ResourceID,
					OtherResourceID: files[j].ResourceID,
					Path:            files[i].MountPath,
					OtherPath:       files[j].MountPath,
					Message:         "file resources have nested mount_paths",
				}
			}
		}
		for _, repo := range githubRepos {
			if files[i].MountPath == repo.MountPath || pathContains(files[i].MountPath, repo.MountPath) || pathContains(repo.MountPath, files[i].MountPath) {
				return &PlanError{
					Code:            "github_mount_path_conflict",
					ResourceID:      files[i].ResourceID,
					OtherResourceID: repo.ResourceID,
					Path:            files[i].MountPath,
					OtherPath:       repo.MountPath,
					Message:         "file resource mount_path conflicts with github_repository mount_path",
				}
			}
		}
	}
	return nil
}

func resolveGitHubMountPath(repo GitHubRepositoryResource) (string, error) {
	repoName := repo.RepositoryName
	if repo.URL != "" {
		resolvedName, err := githubRepositoryNameFromURL(repo.ResourceID, repo.URL)
		if err != nil {
			return "", err
		}
		repoName = resolvedName
	}
	if repo.MountPath != "" {
		if err := validateGitHubMountPath(repo.ResourceID, repo.MountPath); err != nil {
			return "", err
		}
		return repo.MountPath, nil
	}
	if !safeRepositoryName(repoName) {
		return "", &PlanError{
			Code:       "invalid_github_mount_path",
			ResourceID: repo.ResourceID,
			Message:    "github_repository mount_path is unresolved",
		}
	}
	return "/workspace/" + repoName, nil
}

func validateGitHubMountPath(resourceID string, mountPath string) error {
	if mountPath == "" || !strings.HasPrefix(mountPath, "/") || hasUnsafePathCharacter(mountPath) {
		return &PlanError{
			Code:       "invalid_github_mount_path",
			ResourceID: resourceID,
			Path:       mountPath,
			Message:    "github_repository mount_path must be absolute and valid",
		}
	}
	if clean := path.Clean(mountPath); clean != mountPath || clean == "/" {
		return &PlanError{
			Code:       "invalid_github_mount_path",
			ResourceID: resourceID,
			Path:       mountPath,
			Message:    "github_repository mount_path must be lexically clean",
		}
	}
	for _, reserved := range reservedSubtrees() {
		if mountPath == reserved || pathContains(reserved, mountPath) || pathContains(mountPath, reserved) {
			return &PlanError{
				Code:       "reserved_github_mount_path",
				ResourceID: resourceID,
				Path:       mountPath,
				OtherPath:  reserved,
				Message:    "github_repository mount_path overlaps a reserved path",
			}
		}
	}
	if mountPath != "/workspace" && !strings.HasPrefix(mountPath, "/workspace/") {
		return &PlanError{
			Code:       "invalid_github_mount_path",
			ResourceID: resourceID,
			Path:       mountPath,
			OtherPath:  "/workspace",
			Message:    "github_repository mount_path must be under /workspace",
		}
	}
	return nil
}

func hasUnsafePathCharacter(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return true
		}
	}
	return false
}

func githubRepositoryNameFromURL(resourceID string, raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" {
		return "", &PlanError{Code: "invalid_github_repository_url", ResourceID: resourceID, Message: "github_repository url is invalid"}
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", &PlanError{Code: "invalid_github_repository_url", ResourceID: resourceID, Message: "github_repository url is invalid"}
	}
	parts := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	if len(parts) != 2 {
		return "", &PlanError{Code: "invalid_github_repository_url", ResourceID: resourceID, Message: "github_repository url is invalid"}
	}
	owner, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", &PlanError{Code: "invalid_github_repository_url", ResourceID: resourceID, Message: "github_repository url is invalid"}
	}
	repoName, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", &PlanError{Code: "invalid_github_repository_url", ResourceID: resourceID, Message: "github_repository url is invalid"}
	}
	repoName = strings.TrimSuffix(repoName, ".git")
	if !safeRepositoryName(owner) || !safeRepositoryName(repoName) {
		return "", &PlanError{Code: "invalid_github_repository_url", ResourceID: resourceID, Message: "github_repository url is invalid"}
	}
	return repoName, nil
}

func safeRepositoryName(repoName string) bool {
	if repoName == "" || repoName == "." || repoName == ".." || strings.Contains(repoName, "/") {
		return false
	}
	for _, r := range repoName {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.IsSpace(r) || strings.ContainsRune("?#&=:@\\", r) {
			return false
		}
	}
	return true
}

func rejectReservedPath(file plannedFile) error {
	for _, reserved := range reservedSubtrees() {
		if file.MountPath == reserved || pathContains(reserved, file.MountPath) || pathContains(file.MountPath, reserved) {
			return &PlanError{
				Code:       "reserved_mount_path",
				ResourceID: file.ResourceID,
				Path:       file.MountPath,
				OtherPath:  reserved,
				Message:    "file resource mount_path overlaps a reserved path",
			}
		}
	}
	if file.MountPath == defaultUploadRoot {
		return &PlanError{
			Code:       "reserved_mount_path",
			ResourceID: file.ResourceID,
			Path:       file.MountPath,
			OtherPath:  defaultUploadRoot,
			Message:    "file resource mount_path overlaps a reserved path",
		}
	}
	if file.MountPath == "/workspace" {
		return &PlanError{
			Code:       "reserved_mount_path",
			ResourceID: file.ResourceID,
			Path:       file.MountPath,
			OtherPath:  "/workspace",
			Message:    "file resource mount_path overlaps a reserved path",
		}
	}
	return nil
}

func rejectReservedGitHubPath(resourceID string, mountPath string) error {
	for _, reserved := range reservedSubtrees() {
		if mountPath == reserved || pathContains(reserved, mountPath) || pathContains(mountPath, reserved) {
			return &PlanError{
				Code:       "reserved_github_mount_path",
				ResourceID: resourceID,
				Path:       mountPath,
				OtherPath:  reserved,
				Message:    "github_repository mount_path overlaps a reserved path",
			}
		}
	}
	if mountPath == "/workspace" {
		return &PlanError{
			Code:       "reserved_github_mount_path",
			ResourceID: resourceID,
			Path:       mountPath,
			OtherPath:  "/workspace",
			Message:    "github_repository mount_path overlaps a reserved path",
		}
	}
	return nil
}

// reservedSubtrees are the in-sandbox paths a file or GitHub-repository mount
// may not overlap, because each already backs a distinct part of the sandbox:
//   - /mnt/tetral/r2: the rclone staging mount for projected session copies.
//   - /tmp/tetral-runtime, /dev/shm/tetral-runtime: the helper runtime roots.
//   - /tmp/tetral/session-prepare: preparation scratch.
//   - /mnt/memory: the memory-store projection target.
//   - /skills: the skill-bundle projection target.
//   - /mnt/session/outputs: output capture; a read-only file here would shadow it.
//
// /workspace is reserved too, but it is the GitHub-repositories root and is
// rejected for file resources separately (rejectReservedPath) so repositories
// can still claim /workspace/<repo>.
//
// UPDATE-WITH: validateGitHubMountPath, rejectReservedPath,
// rejectReservedGitHubPath (all in this file).
func reservedSubtrees() []string {
	return []string{
		stagingMountRoot,
		"/tmp/tetral-runtime",
		"/dev/shm/tetral-runtime",
		"/tmp/tetral/session-prepare",
		"/mnt/memory",
		"/skills",
		"/mnt/session/outputs",
	}
}

func pathContains(parent string, child string) bool {
	if parent == child {
		return true
	}
	if parent == "/" {
		return strings.HasPrefix(child, "/")
	}
	return strings.HasPrefix(child, parent+"/")
}
