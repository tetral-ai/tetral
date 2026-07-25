package resourceprojection

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestBuildPlanEmitsOrderedActionsAndResolvedRoots(t *testing.T) {
	plan, err := BuildPlan(PlanRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		Files: []FileResource{
			{ResourceID: "sesrsc_b", SourceFileID: "file_src_b", SessionFileID: "file_session_b", ObjectID: "obj_b", MountPath: "/workspace/data.csv"},
			{ResourceID: "sesrsc_a", SourceFileID: "file_src_a", SessionFileID: "file_session_a", ObjectID: "obj_a"},
		},
	})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan.SessionPrefix != "workspaces/ws_test/sessions/sesn_test/" {
		t.Fatalf("SessionPrefix = %q", plan.SessionPrefix)
	}
	if plan.ResourcePrefix != "workspaces/ws_test/sessions/sesn_test/resources/" {
		t.Fatalf("ResourcePrefix = %q", plan.ResourcePrefix)
	}
	if got, want := plan.ResourceRootsJSON, `[{"path":"/mnt/session/uploads/file_session_a","mode":"read"},{"path":"/workspace/data.csv","mode":"read"}]`; got != want {
		t.Fatalf("ResourceRootsJSON = %s; want %s", got, want)
	}
	gotTypes := actionTypes(plan.Actions)
	wantTypes := []ActionType{
		ActionCopyObject,
		ActionCopyObject,
		ActionMintCredential,
		ActionMount,
		ActionBind,
		ActionBind,
		ActionVerify,
		ActionVerify,
	}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("action types = %v; want %v", gotTypes, wantTypes)
	}
	firstCopy := plan.Actions[0]
	if firstCopy.ResourceID != "sesrsc_a" ||
		firstCopy.SourceKey != "files/ws_test/obj_a" ||
		firstCopy.DestinationKey != "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_a/file" {
		t.Fatalf("first copy = %+v; want canonical object_id to deterministic session key", firstCopy)
	}
	if plan.Actions[2].Prefix != plan.ResourcePrefix {
		t.Fatalf("mint prefix = %q; want session resources prefix", plan.Actions[2].Prefix)
	}
	if plan.Actions[4].StagingPath != "/mnt/tetral/r2/sesrsc_a/file" || plan.Actions[4].MountPath != "/mnt/session/uploads/file_session_a" {
		t.Fatalf("first bind = %+v; want staging resource file to default mount path", plan.Actions[4])
	}
}

func TestBuildPlanAlwaysHeadsDeterministicCopiesAndRechecksAllBindGuards(t *testing.T) {
	plan, err := BuildPlan(PlanRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		Files: []FileResource{
			{ResourceID: "sesrsc_existing", SourceFileID: "file_src_existing", SessionFileID: "file_existing", ObjectID: "obj_existing"},
			{ResourceID: "sesrsc_new", SourceFileID: "file_src_new", SessionFileID: "file_new", ObjectID: "obj_new", MountPath: "/workspace/new.txt"},
		},
	})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if got, want := plan.ResourceRootsJSON, `[{"path":"/mnt/session/uploads/file_existing","mode":"read"},{"path":"/workspace/new.txt","mode":"read"}]`; got != want {
		t.Fatalf("ResourceRootsJSON = %s; want all resource roots %s", got, want)
	}
	if plan.Actions[0].Type != ActionCopyObject || plan.Actions[0].ResourceID != "sesrsc_existing" {
		t.Fatalf("first action = %+v; want existing deterministic copy HEAD", plan.Actions[0])
	}
	if got, want := actionTypes(plan.Actions), []ActionType{ActionCopyObject, ActionCopyObject, ActionMintCredential, ActionMount, ActionBind, ActionBind, ActionVerify, ActionVerify}; !reflect.DeepEqual(got, want) {
		t.Fatalf("action types = %v; want %v", got, want)
	}
	if plan.Actions[4].ResourceID != "sesrsc_existing" || plan.Actions[5].ResourceID != "sesrsc_new" {
		t.Fatalf("bind actions = %+v/%+v; want existing then new", plan.Actions[4], plan.Actions[5])
	}
	if plan.Actions[6].ResourceID != "sesrsc_existing" || plan.Actions[7].ResourceID != "sesrsc_new" {
		t.Fatalf("verify actions = %+v/%+v; want existing then new", plan.Actions[6], plan.Actions[7])
	}
}

func TestEnsureParentCommandNeverMutatesExistingParentMetadata(t *testing.T) {
	command := EnsureParentCommand("/etc")
	for _, required := range []string{
		"if [ -e '/etc' ]; then",
		"[ -d '/etc' ]",
		"sudo -u 'daytona' test -w '/etc'",
		"sudo -u 'daytona' test -x '/etc'",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("parent command missing %q:\n%s", required, command)
		}
	}
	for _, forbidden := range []string{"chown", "chmod"} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("parent command mutates existing metadata with %q:\n%s", forbidden, command)
		}
	}
}

func TestBuildPlanEmptyFileSetEmitsNoActions(t *testing.T) {
	plan, err := BuildPlan(PlanRequest{WorkspaceID: "ws_test", SessionID: "sesn_test"})
	if err != nil {
		t.Fatalf("BuildPlan empty: %v", err)
	}
	if len(plan.Actions) != 0 || plan.ResourceRootsJSON != "" || len(plan.ResourceRoots) != 0 {
		t.Fatalf("empty plan = %+v; want no actions or roots", plan)
	}
	if plan.SessionPrefix != "workspaces/ws_test/sessions/sesn_test/" {
		t.Fatalf("SessionPrefix = %q", plan.SessionPrefix)
	}
}

func TestBuildPlanValidatesMemoryMountsWithoutFileActions(t *testing.T) {
	plan, err := BuildPlan(PlanRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		MemoryStores: []MemoryStoreResource{
			{ResourceID: "sesrsc_memory", MountPath: "/mnt/memory/project"},
		},
	})
	if err != nil {
		t.Fatalf("BuildPlan memory-only: %v", err)
	}
	if len(plan.Actions) != 0 || len(plan.ResourceRoots) != 0 || plan.ResourceRootsJSON != "[]" {
		t.Fatalf("memory-only plan = %+v; want validation-only plan with empty roots", plan)
	}

	plan, err = BuildPlan(PlanRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		MemoryStores: []MemoryStoreResource{
			{ResourceID: "sesrsc_memory_root", MountPath: "/mnt/memory"},
		},
	})
	if len(plan.Actions) != 0 {
		t.Fatalf("plan actions = %+v; want zero actions on invalid memory mount", plan.Actions)
	}
	assertPlanError(t, err, "invalid_memory_mount_path", "sesrsc_memory_root", "/mnt/memory")
}

func TestBuildPlanRejectsExplicitGitHubMountPathOutsideWorkspace(t *testing.T) {
	plan, err := BuildPlan(PlanRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		GitHubRepositories: []GitHubRepositoryResource{{
			ResourceID: "sesrsc_repo",
			URL:        "https://github.com/tetral-ai/tetral.git",
			MountPath:  "/tmp/repos/tetral",
		}},
	})
	if len(plan.Actions) != 0 || len(plan.ResourceRoots) != 0 {
		t.Fatalf("plan = %+v; want no actions after invalid GitHub mount path", plan)
	}
	assertPlanError(t, err, "invalid_github_mount_path", "sesrsc_repo", "/workspace")
}

func TestBuildPlanRejectsUnresolvedObjectBeforeActions(t *testing.T) {
	_, err := BuildPlan(PlanRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		Files: []FileResource{{
			ResourceID:    "sesrsc_missing_object",
			SourceFileID:  "file_src",
			SessionFileID: "file_session",
			MountPath:     "/workspace/input.txt",
		}},
	})
	assertPlanError(t, err, "unresolved_object", "sesrsc_missing_object", "")
}

func TestBuildPlanCollisionMatrix(t *testing.T) {
	base := func(files ...FileResource) PlanRequest {
		return PlanRequest{
			WorkspaceID: "ws_test",
			SessionID:   "sesn_test",
			Files:       files,
		}
	}
	file := func(resourceID string, mountPath string) FileResource {
		return FileResource{
			ResourceID:    resourceID,
			SourceFileID:  "file_src_" + resourceID,
			SessionFileID: "file_session_" + resourceID,
			ObjectID:      "obj_" + resourceID,
			MountPath:     mountPath,
		}
	}
	tests := []struct {
		name          string
		request       PlanRequest
		wantCode      string
		wantResource  string
		wantOtherPath string
	}{
		{
			name:          "DUP same mount path",
			request:       base(file("sesrsc_a", "/workspace/a.txt"), file("sesrsc_b", "/workspace/a.txt")),
			wantCode:      "duplicate_mount_path",
			wantResource:  "sesrsc_a",
			wantOtherPath: "/workspace/a.txt",
		},
		{
			name:          "NEST component prefix",
			request:       base(file("sesrsc_a", "/workspace/a"), file("sesrsc_b", "/workspace/a/b.txt")),
			wantCode:      "nested_mount_path",
			wantResource:  "sesrsc_a",
			wantOtherPath: "/workspace/a/b.txt",
		},
		{
			name: "GHREPO conflict",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				Files:       []FileResource{file("sesrsc_file", "/workspace/repo/data.txt")},
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo",
					MountPath:  "/workspace/repo",
				}},
			},
			wantCode:      "github_mount_path_conflict",
			wantResource:  "sesrsc_file",
			wantOtherPath: "/workspace/repo",
		},
		{
			name: "GHREPO default mount path conflict",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				Files:       []FileResource{file("sesrsc_file", "/workspace/tetral/data.txt")},
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo",
					URL:        "https://github.com/tetral-ai/tetral.git",
				}},
			},
			wantCode:      "github_mount_path_conflict",
			wantResource:  "sesrsc_file",
			wantOtherPath: "/workspace/tetral",
		},
		{
			name: "GHREPO duplicate default mount path",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				Files:       []FileResource{file("sesrsc_file", "/workspace/other.txt")},
				GitHubRepositories: []GitHubRepositoryResource{
					{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/tetral"},
					{ResourceID: "sesrsc_repo_b", URL: "https://github.com/other/tetral.git"},
				},
			},
			wantCode:      "duplicate_github_mount_path",
			wantResource:  "sesrsc_repo_a",
			wantOtherPath: "/workspace/tetral",
		},
		{
			name: "GHREPO invalid URL with explicit mount path",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				Files:       []FileResource{file("sesrsc_file", "/workspace/other.txt")},
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo",
					URL:        "https://example.com/tetral-ai/tetral",
					MountPath:  "/workspace/tetral",
				}},
			},
			wantCode:      "invalid_github_repository_url",
			wantResource:  "sesrsc_repo",
			wantOtherPath: "",
		},
		{
			name: "GHREPO unresolved mount path",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				Files:       []FileResource{file("sesrsc_file", "/workspace/tetral/data.txt")},
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo",
				}},
			},
			wantCode:      "invalid_github_mount_path",
			wantResource:  "sesrsc_repo",
			wantOtherPath: "",
		},
		{
			name: "GHREPO reserved outputs path",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo",
					URL:        "https://github.com/tetral-ai/tetral",
					MountPath:  "/mnt/session/outputs/repo",
				}},
			},
			wantCode:      "reserved_github_mount_path",
			wantResource:  "sesrsc_repo",
			wantOtherPath: "/mnt/session/outputs",
		},
		{
			name: "GHREPO reserved workspace root",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo",
					URL:        "https://github.com/tetral-ai/tetral",
					MountPath:  "/workspace",
				}},
			},
			wantCode:      "reserved_github_mount_path",
			wantResource:  "sesrsc_repo",
			wantOtherPath: "/workspace",
		},
		{
			name: "GHREPO relative explicit path",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo",
					URL:        "https://github.com/tetral-ai/tetral",
					MountPath:  "workspace/tetral",
				}},
			},
			wantCode:      "invalid_github_mount_path",
			wantResource:  "sesrsc_repo",
			wantOtherPath: "",
		},
		{
			name: "GHREPO nul explicit path",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo",
					URL:        "https://github.com/tetral-ai/tetral",
					MountPath:  "/tmp/repos/te\x00tral",
				}},
			},
			wantCode:      "invalid_github_mount_path",
			wantResource:  "sesrsc_repo",
			wantOtherPath: "",
		},
		{
			name:          "RESERVE runtime root",
			request:       base(file("sesrsc_runtime", "/tmp/tetral-runtime/rclone.conf")),
			wantCode:      "reserved_mount_path",
			wantResource:  "sesrsc_runtime",
			wantOtherPath: "/tmp/tetral-runtime",
		},
		{
			name:          "RESERVE outputs",
			request:       base(file("sesrsc_outputs", "/mnt/session/outputs/report.txt")),
			wantCode:      "reserved_mount_path",
			wantResource:  "sesrsc_outputs",
			wantOtherPath: "/mnt/session/outputs",
		},
		{
			name:          "RESERVE workspace root",
			request:       base(file("sesrsc_workspace", "/workspace")),
			wantCode:      "reserved_mount_path",
			wantResource:  "sesrsc_workspace",
			wantOtherPath: "/workspace",
		},
		{
			name: "MEM file overlap rejected by reserved memory subtree",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				Files:       []FileResource{file("sesrsc_file_mem", "/mnt/memory/project/file.txt")},
				MemoryStores: []MemoryStoreResource{
					{ResourceID: "sesrsc_mem", MountPath: "/mnt/memory/project"},
				},
			},
			wantCode:      "reserved_mount_path",
			wantResource:  "sesrsc_file_mem",
			wantOtherPath: "/mnt/memory",
		},
		{
			name: "MEM github overlap rejected by github workspace containment",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				GitHubRepositories: []GitHubRepositoryResource{{
					ResourceID: "sesrsc_repo_mem",
					URL:        "https://github.com/tetral-ai/tetral",
					MountPath:  "/mnt/memory/project/repo",
				}},
				MemoryStores: []MemoryStoreResource{
					{ResourceID: "sesrsc_mem", MountPath: "/mnt/memory/project"},
				},
			},
			wantCode:      "reserved_github_mount_path",
			wantResource:  "sesrsc_repo_mem",
			wantOtherPath: "/mnt/memory",
		},
		{
			name: "MEM duplicate mount path",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				MemoryStores: []MemoryStoreResource{
					{ResourceID: "sesrsc_mem_a", MountPath: "/mnt/memory/project"},
					{ResourceID: "sesrsc_mem_b", MountPath: "/mnt/memory/project"},
				},
			},
			wantCode:      "duplicate_memory_mount_path",
			wantResource:  "sesrsc_mem_a",
			wantOtherPath: "/mnt/memory/project",
		},
		{
			name: "MEM nested mount path",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				MemoryStores: []MemoryStoreResource{
					{ResourceID: "sesrsc_mem_a", MountPath: "/mnt/memory/project"},
					{ResourceID: "sesrsc_mem_b", MountPath: "/mnt/memory/project/nested"},
				},
			},
			wantCode:      "nested_memory_mount_path",
			wantResource:  "sesrsc_mem_a",
			wantOtherPath: "/mnt/memory/project/nested",
		},
		{
			name: "MEM staging reserved",
			request: PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				MemoryStores: []MemoryStoreResource{
					{ResourceID: "sesrsc_mem_stage", MountPath: "/mnt/memory/.staging/project"},
				},
			},
			wantCode:      "reserved_memory_mount_path",
			wantResource:  "sesrsc_mem_stage",
			wantOtherPath: "/mnt/memory/.staging",
		},
		{
			name:          "ABS relative path",
			request:       base(file("sesrsc_relative", "workspace/a.txt")),
			wantCode:      "invalid_mount_path",
			wantResource:  "sesrsc_relative",
			wantOtherPath: "",
		},
		{
			name:          "ABS unclean path",
			request:       base(file("sesrsc_unclean", "/workspace/../data.txt")),
			wantCode:      "invalid_mount_path",
			wantResource:  "sesrsc_unclean",
			wantOtherPath: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := BuildPlan(tc.request)
			if len(plan.Actions) != 0 {
				t.Fatalf("plan actions = %+v; want zero actions on collision", plan.Actions)
			}
			assertPlanError(t, err, tc.wantCode, tc.wantResource, tc.wantOtherPath)
		})
	}
}

func TestBuildPlanDoesNotTreatStringPrefixAsNestedPath(t *testing.T) {
	plan, err := BuildPlan(PlanRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		Files: []FileResource{
			{ResourceID: "sesrsc_a", SourceFileID: "file_src_a", SessionFileID: "file_a", ObjectID: "obj_a", MountPath: "/workspace/a/b"},
			{ResourceID: "sesrsc_b", SourceFileID: "file_src_b", SessionFileID: "file_b", ObjectID: "obj_b", MountPath: "/workspace/a/bc"},
		},
	})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(plan.Actions) == 0 {
		t.Fatal("BuildPlan emitted no actions; want non-nested paths accepted")
	}
}

func TestReservedPathMatrix(t *testing.T) {
	tests := []struct {
		mountPath string
		offender  string
	}{
		{mountPath: "/mnt/tetral/r2/file.txt", offender: "/mnt/tetral/r2"},
		{mountPath: "/tmp/tetral-runtime/secret", offender: "/tmp/tetral-runtime"},
		{mountPath: "/dev/shm/tetral-runtime/secret", offender: "/dev/shm/tetral-runtime"},
		{mountPath: "/tmp/tetral/session-prepare/file", offender: "/tmp/tetral/session-prepare"},
		{mountPath: "/mnt/memory/project/file", offender: "/mnt/memory"},
		{mountPath: "/skills/generated", offender: "/skills"},
		{mountPath: "/mnt/session/outputs/out.txt", offender: "/mnt/session/outputs"},
		{mountPath: "/mnt/session/uploads", offender: "/mnt/session/uploads"},
	}
	for _, tc := range tests {
		t.Run(tc.mountPath, func(t *testing.T) {
			plan, err := BuildPlan(PlanRequest{
				WorkspaceID: "ws_test",
				SessionID:   "sesn_test",
				Files: []FileResource{{
					ResourceID:    "sesrsc_reserved",
					SourceFileID:  "file_src",
					SessionFileID: "file_session",
					ObjectID:      "obj",
					MountPath:     tc.mountPath,
				}},
			})
			if len(plan.Actions) != 0 {
				t.Fatalf("plan actions = %+v; want zero actions on reserved path", plan.Actions)
			}
			assertPlanError(t, err, "reserved_mount_path", "sesrsc_reserved", tc.offender)
		})
	}
}

func actionTypes(actions []Action) []ActionType {
	out := make([]ActionType, len(actions))
	for i, action := range actions {
		out[i] = action.Type
	}
	return out
}

func assertPlanError(t *testing.T, err error, code string, resourceID string, otherPath string) {
	t.Helper()
	var planErr *PlanError
	if !errors.As(err, &planErr) {
		t.Fatalf("error = %T %v; want *PlanError", err, err)
	}
	if planErr.Code != code {
		t.Fatalf("PlanError.Code = %q; want %q", planErr.Code, code)
	}
	if resourceID != "" && planErr.ResourceID != resourceID {
		t.Fatalf("PlanError.ResourceID = %q; want %q", planErr.ResourceID, resourceID)
	}
	if otherPath != "" && planErr.OtherPath != otherPath {
		t.Fatalf("PlanError.OtherPath = %q; want %q", planErr.OtherPath, otherPath)
	}
}
