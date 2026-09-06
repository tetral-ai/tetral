package tetralsandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	apiclient "github.com/daytonaio/daytona/libs/api-client-go"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
)

func TestDaytonaToolPreparationPreservesFilesystemFailureThroughSDK(t *testing.T) {
	// The response shapes come from the daemon revision linked in tool_error.go:
	// CreateFolder passes os.PathError to ErrorMiddleware; UploadFiles wraps
	// write failures in {errors: [...], files: [...]}. Exercise the real SDK's
	// distinct decoders and the real helper payload preparation/adapter boundary.
	for _, tc := range []struct {
		name, endpoint, kind, message string
		errno                         syscall.Errno
	}{
		{"directory full", "folder", "storage_full", "Execution environment storage is full.", syscall.ENOSPC},
		{"upload full", "bulk-upload", "storage_full", "Execution environment storage is full.", syscall.ENOSPC},
		{"directory denied", "folder", "filesystem_access_denied", "Execution environment filesystem access was denied.", syscall.EACCES},
		{"upload read-only", "bulk-upload", "filesystem_read_only", "Execution environment filesystem is read-only.", syscall.EROFS},
		{"unclassified sensitive response", "folder", "invalid_request", "Execution environment preparation failed; the provider rejected the request.", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			failedRequests, processRequests := 0, 0
			const sensitive = "token=diagnostic-private-value"
			var failedPath string
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet && r.URL.Path == "/sandbox/provider-test" {
					state := apiclient.SANDBOXSTATE_STARTED
					_ = json.NewEncoder(w).Encode(apiclient.Sandbox{
						Id: "provider-test", State: &state, Labels: map[string]string{},
						ToolboxProxyUrl: server.URL + "/toolbox",
					})
					return
				}
				if r.Method == http.MethodDelete {
					w.WriteHeader(http.StatusOK) // cleanup after failed upload
					return
				}
				if strings.HasSuffix(r.URL.Path, "/process/execute") {
					processRequests++
					t.Error("preparation failure reached remote process execution")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				if tc.endpoint == "bulk-upload" && strings.HasSuffix(r.URL.Path, "/files/folder") {
					w.WriteHeader(http.StatusCreated)
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/toolbox/provider-test/files/"+tc.endpoint {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				failedRequests++
				if tc.endpoint == "folder" {
					failedPath = r.URL.Query().Get("path")
					if tc.errno == syscall.ENOSPC {
						// MkdirAll can fail creating the shared staging parent
						// before reaching the per-invocation directory.
						failedPath = path.Dir(failedPath)
					}
					message := (&os.PathError{Op: "mkdir", Path: failedPath, Err: tc.errno}).Error()
					if tc.errno == 0 {
						// A body mentioning disk space is not itself an ENOSPC
						// response for the Engine-owned filesystem operation.
						message = "rejected " + sensitive + "; no space left on device"
					}
					w.WriteHeader(http.StatusBadRequest)
					_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": 400, "message": message, "code": "Bad Request"})
					return
				}
				r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Errorf("parse SDK streaming upload: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				defer func() {
					if err := r.MultipartForm.RemoveAll(); err != nil {
						t.Errorf("remove multipart temporary files: %v", err)
					}
				}()
				failedPath = r.FormValue("files[0].path")
				message := fmt.Sprintf("%s: write: %v", failedPath, &os.PathError{Op: "write", Path: failedPath, Err: tc.errno})
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"errors": []string{message}, "files": []any{}})
			}))
			defer server.Close()
			client, err := daytona.NewClientWithConfig(&types.DaytonaConfig{
				APIKey: "test-only", APIUrl: server.URL, OrganizationID: "test-only", Target: "test-only",
			})
			if err != nil {
				t.Fatal(err)
			}
			executor, err := sandboxdriver.NewDaytonaHelperExecutorForSDKClient(client, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			adapter := DaytonaAdapter{Tools: executor, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
			outcome := adapter.PrepareTool(context.Background(), ToolExecutionRequest{
				Handle: sandbox.ProviderHandle{SandboxID: "provider-test"},
				Invocation: sandboxdriver.ToolInvocation{
					ToolUseEventID: "evt_failure", ToolName: "Bash", InputJSON: `{"command":"true"}`,
					Target: sandboxdriver.ToolTarget{WorkspaceID: "ws_test", SessionID: "sesn_test"},
				},
			})
			wantMessage := tc.message + " The tool operation was not started."
			if outcome.ErrorKind != tc.kind || outcome.SafeMessage != wantMessage ||
				outcome.EffectBoundary != ProviderProvedNotStarted || outcome.Disposition != ProviderTerminal {
				t.Fatalf("outcome = %+v; want terminal %s with %q before execution", outcome, tc.kind, wantMessage)
			}
			if failedPath == "" || failedRequests != 1 || processRequests != 0 {
				t.Fatalf("requests: failed=%d process=%d path=%q", failedRequests, processRequests, failedPath)
			}
			var logged map[string]any
			if err := json.Unmarshal(logs.Bytes(), &logged); err != nil {
				t.Fatal(err)
			}
			operation := "create_payload_directory"
			if tc.endpoint == "bulk-upload" {
				operation = "upload_payload"
			}
			if logged["provider.operation"] != operation || logged["provider.status_code"] != float64(400) || logged["error.code"] != tc.kind {
				t.Fatalf("missing operational diagnosis: %s", logs.String())
			}
			detail, _ := logged["provider.error_detail"].(string)
			if tc.errno == 0 {
				if detail != "Provider diagnostic detail redacted." {
					t.Fatalf("sensitive diagnostic was not redacted: %s", logs.String())
				}
			} else if !strings.Contains(detail, tc.errno.Error()) {
				t.Fatalf("filesystem cause lost: %s", logs.String())
			}
			if strings.Contains(logs.String()+outcome.SafeMessage, failedPath) || strings.Contains(logs.String()+outcome.SafeMessage, sensitive) {
				t.Fatal("internal payload path or sensitive response escaped into log/tool message")
			}
		})
	}
}
