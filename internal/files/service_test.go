package files_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type recordingFilesBackend struct {
	calls []string

	lastContext                context.Context
	lastWorkspace              workspace.ID
	lastStaged                 *files.StagedUpload
	lastOptions                files.ListOptions
	lastFileID                 string
	lastSessionID              string
	lastSessionTx              files.SessionTransaction
	lastSessionIdentityRequest files.SessionFileIdentityRequest

	createResult          *files.FileMetadata
	sessionIdentityResult *files.FileMetadata
	listResult            files.ListResult
	getResult             *files.FileMetadata
	deleteResult          *files.DeleteResponse
	openResult            *files.ContentStream
	sourceResult          *files.Source
	mountSourceResult     *files.MountSource
	err                   error
}

func (b *recordingFilesBackend) record(ctx context.Context, method string, ws workspace.ID) {
	b.calls = append(b.calls, method)
	b.lastContext = ctx
	b.lastWorkspace = ws
}

func (b *recordingFilesBackend) CreateUploadedFile(ctx context.Context, ws workspace.ID, staged *files.StagedUpload) (*files.FileMetadata, error) {
	b.record(ctx, "CreateUploadedFile", ws)
	b.lastStaged = staged
	return b.createResult, b.err
}

func (b *recordingFilesBackend) ListFiles(ctx context.Context, ws workspace.ID, options files.ListOptions) (files.ListResult, error) {
	b.record(ctx, "ListFiles", ws)
	b.lastOptions = options
	return b.listResult, b.err
}

func (b *recordingFilesBackend) GetFile(ctx context.Context, ws workspace.ID, fileID string) (*files.FileMetadata, error) {
	b.record(ctx, "GetFile", ws)
	b.lastFileID = fileID
	return b.getResult, b.err
}

func (b *recordingFilesBackend) DeleteFile(ctx context.Context, ws workspace.ID, fileID string) (*files.DeleteResponse, error) {
	b.record(ctx, "DeleteFile", ws)
	b.lastFileID = fileID
	return b.deleteResult, b.err
}

func (b *recordingFilesBackend) OpenContent(ctx context.Context, ws workspace.ID, fileID string) (*files.ContentStream, error) {
	b.record(ctx, "OpenContent", ws)
	b.lastFileID = fileID
	return b.openResult, b.err
}

func (b *recordingFilesBackend) ResolveSource(ctx context.Context, ws workspace.ID, fileID string) (*files.Source, error) {
	b.record(ctx, "ResolveSource", ws)
	b.lastFileID = fileID
	return b.sourceResult, b.err
}

func (b *recordingFilesBackend) ResolveMountSource(ctx context.Context, ws workspace.ID, sessionID string, fileID string) (*files.MountSource, error) {
	b.record(ctx, "ResolveMountSource", ws)
	b.lastSessionID = sessionID
	b.lastFileID = fileID
	return b.mountSourceResult, b.err
}

func (b *recordingFilesBackend) ValidateSessionFileSource(ctx context.Context, ws workspace.ID, fileID string) error {
	b.record(ctx, "ValidateSessionFileSource", ws)
	b.lastFileID = fileID
	return b.err
}

func (b *recordingFilesBackend) CreateSessionFileIdentity(ctx context.Context, tx files.SessionTransaction, ws workspace.ID, request files.SessionFileIdentityRequest) (*files.FileMetadata, error) {
	b.record(ctx, "CreateSessionFileIdentity", ws)
	b.lastSessionTx = tx
	b.lastSessionIdentityRequest = request
	return b.sessionIdentityResult, b.err
}

func (b *recordingFilesBackend) TombstoneSessionFileIdentity(ctx context.Context, tx files.SessionTransaction, ws workspace.ID, sessionID string, fileID string) error {
	b.record(ctx, "TombstoneSessionFileIdentity", ws)
	b.lastSessionTx = tx
	b.lastSessionID = sessionID
	b.lastFileID = fileID
	return b.err
}

func TestFilesServiceDelegatesCreateUploadedFile(t *testing.T) {
	ctx := context.WithValue(context.Background(), testContextKey{}, "create")
	ws := workspace.ID("ws_files_service")
	staged := &files.StagedUpload{Filename: "upload.txt", MIMEType: "text/plain", SizeBytes: 12}
	want := &files.FileMetadata{ID: "file_created", Type: "file", Filename: "upload.txt"}
	backend := &recordingFilesBackend{createResult: want}
	service := files.NewService(backend)

	got, err := service.CreateUploadedFile(ctx, ws, staged)
	if err != nil {
		t.Fatalf("CreateUploadedFile error = %v", err)
	}
	if got != want {
		t.Fatalf("CreateUploadedFile result pointer = %p; want %p", got, want)
	}
	assertFilesBackendCall(ctx, t, backend, "CreateUploadedFile", ws)
	if backend.lastStaged != staged {
		t.Fatalf("staged pointer = %p; want %p", backend.lastStaged, staged)
	}
}

func TestFilesServiceDelegatesListFiles(t *testing.T) {
	ctx := context.WithValue(context.Background(), testContextKey{}, "list")
	ws := workspace.ID("ws_files_service")
	firstID := "file_first"
	lastID := "file_last"
	wantItem := &files.FileMetadata{ID: "file_list", Type: "file"}
	want := files.ListResult{Data: []*files.FileMetadata{wantItem}, FirstID: &firstID, LastID: &lastID, HasMore: true}
	options := files.ListOptions{Limit: 7, AfterID: "file_after", BeforeID: "file_before", ScopeID: "sesn_scope"}
	backend := &recordingFilesBackend{listResult: want}
	service := files.NewService(backend)

	got, err := service.ListFiles(ctx, ws, options)
	if err != nil {
		t.Fatalf("ListFiles error = %v", err)
	}
	if len(got.Data) != 1 || got.Data[0] != wantItem || got.FirstID != &firstID || got.LastID != &lastID || !got.HasMore {
		t.Fatalf("ListFiles result = %+v; want pass-through %+v", got, want)
	}
	assertFilesBackendCall(ctx, t, backend, "ListFiles", ws)
	if backend.lastOptions != options {
		t.Fatalf("options = %+v; want %+v", backend.lastOptions, options)
	}
}

func TestFilesServiceDelegatesGetDeleteOpenAndResolve(t *testing.T) {
	ctx := context.WithValue(context.Background(), testContextKey{}, "id-methods")
	ws := workspace.ID("ws_files_service")
	fileID := "file_service"

	t.Run("get", func(t *testing.T) {
		want := &files.FileMetadata{ID: fileID, Type: "file"}
		backend := &recordingFilesBackend{getResult: want}
		service := files.NewService(backend)

		got, err := service.GetFile(ctx, ws, fileID)
		if err != nil {
			t.Fatalf("GetFile error = %v", err)
		}
		if got != want {
			t.Fatalf("GetFile result pointer = %p; want %p", got, want)
		}
		assertFilesBackendIDCall(ctx, t, backend, "GetFile", ws, fileID)
	})

	t.Run("delete", func(t *testing.T) {
		want := &files.DeleteResponse{ID: fileID, Type: "file_deleted"}
		backend := &recordingFilesBackend{deleteResult: want}
		service := files.NewService(backend)

		got, err := service.DeleteFile(ctx, ws, fileID)
		if err != nil {
			t.Fatalf("DeleteFile error = %v", err)
		}
		if got != want {
			t.Fatalf("DeleteFile result pointer = %p; want %p", got, want)
		}
		assertFilesBackendIDCall(ctx, t, backend, "DeleteFile", ws, fileID)
	})

	t.Run("open content", func(t *testing.T) {
		want := &files.ContentStream{
			Metadata: &files.FileMetadata{ID: fileID, Type: "file", MIMEType: "text/plain"},
			Reader:   io.NopCloser(strings.NewReader("content")),
		}
		backend := &recordingFilesBackend{openResult: want}
		service := files.NewService(backend)

		got, err := service.OpenContent(ctx, ws, fileID)
		if err != nil {
			t.Fatalf("OpenContent error = %v", err)
		}
		if got != want {
			t.Fatalf("OpenContent result pointer = %p; want %p", got, want)
		}
		assertFilesBackendIDCall(ctx, t, backend, "OpenContent", ws, fileID)
	})

	t.Run("resolve source", func(t *testing.T) {
		want := &files.Source{
			Metadata: &files.FileMetadata{ID: fileID, Type: "file"},
			Open: func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("source")), nil
			},
		}
		backend := &recordingFilesBackend{sourceResult: want}
		service := files.NewService(backend)

		got, err := service.ResolveSource(ctx, ws, fileID)
		if err != nil {
			t.Fatalf("ResolveSource error = %v", err)
		}
		if got != want {
			t.Fatalf("ResolveSource result pointer = %p; want %p", got, want)
		}
		assertFilesBackendIDCall(ctx, t, backend, "ResolveSource", ws, fileID)
	})

	t.Run("validate session file source", func(t *testing.T) {
		backend := &recordingFilesBackend{}
		service := files.NewService(backend)

		if err := service.ValidateSessionFileSource(ctx, ws, fileID); err != nil {
			t.Fatalf("ValidateSessionFileSource error = %v", err)
		}
		assertFilesBackendIDCall(ctx, t, backend, "ValidateSessionFileSource", ws, fileID)
	})
}

func TestFilesServicePreservesTypedErrors(t *testing.T) {
	typedErrors := []error{
		&files.ValidationError{Message: "bad request"},
		&files.RequestTooLargeError{Message: "too large"},
		&files.NotFoundError{Message: "file not found"},
		&files.PermissionError{Message: "file content is not downloadable"},
		&files.ConflictError{Message: "conflict"},
		&files.QuotaError{Kind: files.QuotaKindCount, Message: "count quota"},
		&files.QuotaError{Kind: files.QuotaKindRetainedBytes, Message: "byte quota"},
	}
	for _, wantErr := range typedErrors {
		t.Run(wantErr.Error(), func(t *testing.T) {
			backend := &recordingFilesBackend{err: wantErr}
			service := files.NewService(backend)

			_, gotErr := service.GetFile(context.Background(), workspace.DefaultID, "file_error")

			if !errors.Is(gotErr, wantErr) {
				t.Fatalf("GetFile error = %T %v; want original %T %v", gotErr, gotErr, wantErr, wantErr)
			}
		})
	}
}

func TestFilesServiceFileAvoidsStorageMechanics(t *testing.T) {
	path := filepath.Join(filesServicePackageDir(t), "service.go")
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
		if importPath == "github.com/tetral-ai/tetral/internal/blob" {
			t.Fatalf("service.go imports BlobStore package %q; Files service must stay above storage mechanics", importPath)
		}
		awsSDKRoot := "github.com/" + "aws/" + "aws-sdk-go-v2"
		if strings.HasPrefix(importPath, awsSDKRoot) {
			t.Fatalf("service.go imports AWS SDK package %q", importPath)
		}
		for _, forbidden := range []string{
			"github.com/tetral-ai/tetral/internal/httpapi",
			"github.com/tetral-ai/backend",
			"github.com/tetral-ai/gateway",
			"github.com/tetral-ai/runtime",
			"gopkg.in/" + "yaml.v3",
		} {
			if strings.HasPrefix(importPath, forbidden) {
				t.Fatalf("service.go imports forbidden package %q", importPath)
			}
		}
	}

	for _, fragment := range []string{
		"BlobStore",
		"blobKey",
		"bucket",
		"objectKey",
		"S3",
		"aws",
		"bestEffortDelete",
		"file_objects",
		"blobKeyPrefix",
	} {
		if strings.Contains(string(source), fragment) {
			t.Fatalf("service.go contains storage-specific fragment %q", fragment)
		}
	}

	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok {
			if ident, ok := selector.X.(*ast.Ident); ok && (ident.Name == "blob" || ident.Name == "s3") {
				t.Fatalf("service.go selects storage client expression %s.%s", ident.Name, selector.Sel.Name)
			}
		}
		return true
	})
}

func assertFilesBackendCall(ctx context.Context, t *testing.T, backend *recordingFilesBackend, method string, ws workspace.ID) {
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

func assertFilesBackendIDCall(ctx context.Context, t *testing.T, backend *recordingFilesBackend, method string, ws workspace.ID, fileID string) {
	t.Helper()
	assertFilesBackendCall(ctx, t, backend, method, ws)
	if backend.lastFileID != fileID {
		t.Fatalf("fileID = %q; want %q", backend.lastFileID, fileID)
	}
}

func filesServicePackageDir(t *testing.T) string {
	t.Helper()
	_, path, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(path)
}

type testContextKey struct{}
