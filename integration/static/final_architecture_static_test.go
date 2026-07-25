package static

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFinalArchitectureEventStreamIsImplementedReadOnlyService(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	eventstreamSource := readFinalArchitectureSource(t, engineRoot, "services/event-stream/eventstream.go")
	readerSource := readFinalArchitectureSource(t, engineRoot, "internal/eventstream/postgresql_reader.go")
	commandSource := readFinalArchitectureSource(t, engineRoot, "services/event-stream/cmd/event-stream/main.go")
	tetralAPISource := readFinalArchitectureSource(t, engineRoot, "services/api/tetralapi.go")
	for _, want := range []string{
		"text/event-stream",
		"event: %s",
		"data: %s",
	} {
		if !strings.Contains(eventstreamSource, want) {
			t.Fatalf("services/event-stream/eventstream.go missing Event Stream implementation token %q", want)
		}
	}
	if !strings.Contains(eventstreamSource, "github.com/tetral-ai/tetral/internal/eventstream") {
		t.Fatal("event-stream must compose the shared internal/eventstream reader")
	}
	for _, forbidden := range []string{"NewListRouter", "NewListHandler", "ListSessionEvents", "ListThreadEvents"} {
		if strings.Contains(eventstreamSource, forbidden) {
			t.Fatalf("event-stream SSE service retains list ownership token %q", forbidden)
		}
	}
	if !strings.Contains(tetralAPISource, "github.com/tetral-ai/tetral/internal/eventstream") {
		t.Fatal("api must compose the shared internal/eventstream list handler")
	}
	if strings.Contains(tetralAPISource, "github.com/tetral-ai/tetral/services/event-stream") {
		t.Fatal("api must not import the event-stream service package")
	}
	for _, want := range []string{
		"session_event_stream_changes",
		"session_events",
		"visibility = 'public'",
		"session_visible = TRUE",
	} {
		if !strings.Contains(readerSource, want) {
			t.Fatalf("internal/eventstream/postgresql_reader.go missing read-model token %q", want)
		}
	}
	for _, want := range []string{
		"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64",
		"NewPostgreSQLReader",
	} {
		if !strings.Contains(commandSource, want) {
			t.Fatalf("services/event-stream/cmd/event-stream/main.go missing service wiring token %q", want)
		}
	}
	for _, packageDir := range []string{"internal/eventstream", "services/event-stream", "services/event-stream/cmd/event-stream"} {
		scanProductionSourceTokens(t, engineRoot, packageDir, []string{
			"AppendClientEvents",
			"queue.Enqueue",
			"INSERT INTO session_events",
			"UPDATE session_events",
			"DELETE FROM session_events",
			"session_runtime_inbox",
			"RunTool",
			"SendCommandInput",
			"ReadCommandResult",
			"CancelCommand",
			"NewAPIKeyStore",
			"StoreAuthenticator",
			"x-api-key",
			"InitializeSchema",
		})
	}
}

func TestFinalArchitecturePublicWorkloadsAreServiceLocal(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	for _, requiredPath := range []string{
		"services/auth/cmd/tetral-auth/main.go",
		"services/auth/k8s/deployment.yaml",
		"services/api/cmd/tetral-api/main.go",
		"services/api/bootstrap.go",
		"services/api/config.go",
		"services/api/routes.go",
		"services/api/wiring.go",
		"services/event-stream/cmd/event-stream/main.go",
		"services/api/k8s/configmap.yaml",
		"services/api/k8s/deployment.yaml",
		"services/api/k8s/secret.example.yaml",
		"services/event-stream/k8s/deployment.yaml",
	} {
		if _, err := os.Stat(filepath.Join(engineRoot, requiredPath)); err != nil {
			t.Fatalf("service-local public workload artifact missing at %s: %v", requiredPath, err)
		}
	}
}

func readFinalArchitectureSource(t *testing.T, engineRoot string, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(engineRoot, rel)) //nolint:gosec // G304: fixed repo-local source path from test table.
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(body)
}

func scanProductionSourceTokens(t *testing.T, engineRoot string, packageDir string, forbidden []string) {
	t.Helper()
	root := filepath.Join(engineRoot, packageDir)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "gen" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path) //nolint:gosec // G304: path comes from repository-local WalkDir in a static test.
		if err != nil {
			return err
		}
		source := string(body)
		for _, token := range forbidden {
			if strings.Contains(source, token) {
				t.Fatalf("%s contains forbidden skeleton data-flow token %q", finalArchitectureRel(t, engineRoot, path), token)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan skeleton source under %s: %v", packageDir, err)
	}
}
