package environment_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type recordingEnvironmentBackend struct {
	calls []string

	lastContext       context.Context
	lastWorkspace     workspace.ID
	lastRequest       environment.CreateEnvironmentRequest
	lastOptions       environment.ListOptions
	lastPatch         environment.EnvironmentPatch
	lastEnvironmentID string

	createResult  *environment.Environment
	getResult     *environment.Environment
	listResult    environment.ListResult
	updateResult  *environment.Environment
	archiveResult *environment.Environment
	deleteResult  *environment.DeleteResult
	err           error
}

func (b *recordingEnvironmentBackend) record(ctx context.Context, method string, ws workspace.ID) {
	b.calls = append(b.calls, method)
	b.lastContext = ctx
	b.lastWorkspace = ws
}

func (b *recordingEnvironmentBackend) Create(ctx context.Context, ws workspace.ID, request environment.CreateEnvironmentRequest) (*environment.Environment, error) {
	b.record(ctx, "Create", ws)
	b.lastRequest = request
	return b.createResult, b.err
}

func (b *recordingEnvironmentBackend) Get(ctx context.Context, ws workspace.ID, environmentID string) (*environment.Environment, error) {
	b.record(ctx, "Get", ws)
	b.lastEnvironmentID = environmentID
	return b.getResult, b.err
}

func (b *recordingEnvironmentBackend) List(ctx context.Context, ws workspace.ID, options environment.ListOptions) (environment.ListResult, error) {
	b.record(ctx, "List", ws)
	b.lastOptions = options
	return b.listResult, b.err
}

func (b *recordingEnvironmentBackend) Update(ctx context.Context, ws workspace.ID, environmentID string, patch environment.EnvironmentPatch) (*environment.Environment, error) {
	b.record(ctx, "Update", ws)
	b.lastEnvironmentID = environmentID
	b.lastPatch = patch
	return b.updateResult, b.err
}

func (b *recordingEnvironmentBackend) Archive(ctx context.Context, ws workspace.ID, environmentID string) (*environment.Environment, error) {
	b.record(ctx, "Archive", ws)
	b.lastEnvironmentID = environmentID
	return b.archiveResult, b.err
}

func (b *recordingEnvironmentBackend) Delete(ctx context.Context, ws workspace.ID, environmentID string) (*environment.DeleteResult, error) {
	b.record(ctx, "Delete", ws)
	b.lastEnvironmentID = environmentID
	return b.deleteResult, b.err
}

func TestEnvironmentServiceDelegatesCreate(t *testing.T) {
	ctx := context.WithValue(context.Background(), environmentServiceContextKey{}, "create")
	ws := workspace.ID("ws_environment_service")
	request := environment.CreateEnvironmentRequest{
		Name:        "unit-create",
		Description: "created by service test",
		Config:      environment.EnvironmentConfig{Networking: &environment.NetworkingConfig{Type: "unrestricted"}},
		Metadata:    environment.StringMap{"team": "runtime"},
	}
	want := &environment.Environment{ID: "env_created", Type: "environment", Name: request.Name}
	backend := &recordingEnvironmentBackend{createResult: want}
	service := environment.NewService(backend)

	got, err := service.Create(ctx, ws, request)
	if err != nil {
		t.Fatalf("Create error = %v", err)
	}
	if got != want {
		t.Fatalf("Create result pointer = %p; want %p", got, want)
	}
	assertEnvironmentBackendCall(ctx, t, backend, "Create", ws)
	if backend.lastRequest.Name != request.Name || backend.lastRequest.Description != request.Description {
		t.Fatalf("Create request = %+v; want %+v", backend.lastRequest, request)
	}
	if backend.lastRequest.Config.Networking != request.Config.Networking || backend.lastRequest.Metadata["team"] != "runtime" {
		t.Fatalf("Create request did not pass through config/metadata: %+v", backend.lastRequest)
	}
}

func TestEnvironmentServiceDelegatesList(t *testing.T) {
	ctx := context.WithValue(context.Background(), environmentServiceContextKey{}, "list")
	ws := workspace.ID("ws_environment_service")
	nextPage := "next-token"
	item := &environment.Environment{ID: "env_list", Type: "environment", Name: "listed"}
	want := environment.ListResult{Data: []*environment.Environment{item}, NextPage: &nextPage}
	options := environment.ListOptions{Limit: 17, Page: "page-token", IncludeArchived: true}
	backend := &recordingEnvironmentBackend{listResult: want}
	service := environment.NewService(backend)

	got, err := service.List(ctx, ws, options)
	if err != nil {
		t.Fatalf("List error = %v", err)
	}
	if len(got.Data) != 1 || got.Data[0] != item || got.NextPage != &nextPage {
		t.Fatalf("List result = %+v; want pass-through %+v", got, want)
	}
	assertEnvironmentBackendCall(ctx, t, backend, "List", ws)
	if backend.lastOptions != options {
		t.Fatalf("List options = %+v; want %+v", backend.lastOptions, options)
	}
}

func TestEnvironmentServiceDelegatesIDMethods(t *testing.T) {
	ctx := context.WithValue(context.Background(), environmentServiceContextKey{}, "id-methods")
	ws := workspace.ID("ws_environment_service")
	environmentID := "env_service"

	t.Run("get", func(t *testing.T) {
		want := &environment.Environment{ID: environmentID, Type: "environment", Name: "got"}
		backend := &recordingEnvironmentBackend{getResult: want}
		service := environment.NewService(backend)

		got, err := service.Get(ctx, ws, environmentID)
		if err != nil {
			t.Fatalf("Get error = %v", err)
		}
		if got != want {
			t.Fatalf("Get result pointer = %p; want %p", got, want)
		}
		assertEnvironmentBackendIDCall(ctx, t, backend, "Get", ws, environmentID)
	})

	t.Run("update", func(t *testing.T) {
		patch, err := environment.DecodeUpdateEnvironmentRequest([]byte(`{"description":"updated"}`))
		if err != nil {
			t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
		}
		want := &environment.Environment{ID: environmentID, Type: "environment", Description: "updated"}
		backend := &recordingEnvironmentBackend{updateResult: want}
		service := environment.NewService(backend)

		got, err := service.Update(ctx, ws, environmentID, patch)
		if err != nil {
			t.Fatalf("Update error = %v", err)
		}
		if got != want {
			t.Fatalf("Update result pointer = %p; want %p", got, want)
		}
		assertEnvironmentBackendIDCall(ctx, t, backend, "Update", ws, environmentID)
		materialized, err := backend.lastPatch.Materialize(environment.Environment{Name: "base"})
		if err != nil {
			t.Fatalf("Materialize backend patch: %v", err)
		}
		if materialized.Description != "updated" {
			t.Fatalf("patch description = %q; want updated", materialized.Description)
		}
	})

	t.Run("archive", func(t *testing.T) {
		want := &environment.Environment{ID: environmentID, Type: "environment", Name: "archived"}
		backend := &recordingEnvironmentBackend{archiveResult: want}
		service := environment.NewService(backend)

		got, err := service.Archive(ctx, ws, environmentID)
		if err != nil {
			t.Fatalf("Archive error = %v", err)
		}
		if got != want {
			t.Fatalf("Archive result pointer = %p; want %p", got, want)
		}
		assertEnvironmentBackendIDCall(ctx, t, backend, "Archive", ws, environmentID)
	})

	t.Run("delete", func(t *testing.T) {
		want := &environment.DeleteResult{ID: environmentID, Type: "environment_deleted"}
		backend := &recordingEnvironmentBackend{deleteResult: want}
		service := environment.NewService(backend)

		got, err := service.Delete(ctx, ws, environmentID)
		if err != nil {
			t.Fatalf("Delete error = %v", err)
		}
		if got != want {
			t.Fatalf("Delete result pointer = %p; want %p", got, want)
		}
		assertEnvironmentBackendIDCall(ctx, t, backend, "Delete", ws, environmentID)
	})
}

func TestEnvironmentServicePreservesTypedErrors(t *testing.T) {
	typedErrors := []error{
		&environment.ValidationError{Message: "bad environment request"},
		&environment.NotFoundError{Message: "environment not found"},
		&environment.ConflictError{Message: "environment conflict"},
	}
	for _, wantErr := range typedErrors {
		t.Run(wantErr.Error(), func(t *testing.T) {
			backend := &recordingEnvironmentBackend{err: wantErr}
			service := environment.NewService(backend)

			_, gotErr := service.Get(context.Background(), workspace.DefaultID, "env_error")

			if !errors.Is(gotErr, wantErr) {
				t.Fatalf("Get error = %T %v; want original %T %v", gotErr, gotErr, wantErr, wantErr)
			}
		})
	}
}

func TestEnvironmentServiceAvoidsRuntimeSessionMaterializationDependencies(t *testing.T) {
	path := filepath.Join(environmentPackageDir(t), "service.go")
	source, err := os.ReadFile(path) //nolint:gosec // path is derived from runtime.Caller for this package test file.
	if err != nil {
		t.Fatalf("ReadFile service.go: %v", err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		t.Fatalf("ParseFile service.go: %v", err)
	}
	for _, imp := range file.Imports {
		importPath := strings.Trim(imp.Path.Value, `"`)
		for _, forbidden := range []string{
			"github.com/tetral-ai/tetral/internal/httpapi",
			"github.com/tetral-ai/tetral/internal/session",
			"github.com/tetral-ai/backend",
			"github.com/tetral-ai/gateway",
			"github.com/tetral-ai/runtime",
		} {
			if strings.HasPrefix(importPath, forbidden) {
				t.Fatalf("service.go imports forbidden package %q", importPath)
			}
		}
	}

	for _, fragment := range []string{
		"Runtime",
		"Session",
		"Materialize",
		"PackageInstall",
		"NetworkPolicy",
		"Docker",
		"Firecracker",
	} {
		if strings.Contains(string(source), fragment) {
			t.Fatalf("service.go contains runtime/materialization fragment %q", fragment)
		}
	}

	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok {
			if ident, ok := selector.X.(*ast.Ident); ok && (ident.Name == "runtime" || ident.Name == "session") {
				t.Fatalf("service.go selects forbidden expression %s.%s", ident.Name, selector.Sel.Name)
			}
		}
		return true
	})
}

func assertEnvironmentBackendCall(ctx context.Context, t *testing.T, backend *recordingEnvironmentBackend, method string, ws workspace.ID) {
	t.Helper()
	if len(backend.calls) != 1 || backend.calls[0] != method {
		t.Fatalf("backend calls = %v; want [%s]", backend.calls, method)
	}
	if backend.lastContext != ctx {
		t.Fatalf("context pointer = %p; want %p", backend.lastContext, ctx)
	}
	if backend.lastWorkspace != ws {
		t.Fatalf("workspace = %q; want %q", backend.lastWorkspace, ws)
	}
}

func assertEnvironmentBackendIDCall(ctx context.Context, t *testing.T, backend *recordingEnvironmentBackend, method string, ws workspace.ID, environmentID string) {
	t.Helper()
	assertEnvironmentBackendCall(ctx, t, backend, method, ws)
	if backend.lastEnvironmentID != environmentID {
		t.Fatalf("environmentID = %q; want %q", backend.lastEnvironmentID, environmentID)
	}
}

func environmentPackageDir(t *testing.T) string {
	t.Helper()
	_, path, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(path)
}

type environmentServiceContextKey struct{}
