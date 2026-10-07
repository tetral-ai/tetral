package eventstream_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// This source guard rejects any database-client call other than the workspace
// read-only transaction helper. The behavioral guards are
// TestPostgreSQLRequestFinalMessagesAndPreviewAdmission and
// TestPostgreSQLSessionChangeLifecyclePreservesDeletion, which open the reader
// through the SELECT-only event_stream role.
func TestPostgreSQLReaderUsesReadOnlyTransactions(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	clientCall := regexp.MustCompile(`\.client\.(\w+)\(`)
	count := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(source)
		if strings.Contains(text, ".WithWorkspaceTx") {
			t.Fatalf("%s uses read-write workspace transactions", name)
		}
		for _, call := range clientCall.FindAllStringSubmatch(text, -1) {
			if call[1] != "WithWorkspaceReadOnlyTx" {
				t.Fatalf("%s calls database client method %s outside a read-only workspace transaction", name, call[1])
			}
			count++
		}
	}
	if count == 0 {
		t.Fatal("reader must use workspace read-only transactions")
	}
}

func TestPostgreSQLReaderSessionListUsesImmutableGlobalOrder(t *testing.T) {
	source, err := os.ReadFile("postgresql_reader.go")
	if err != nil {
		t.Fatalf("read postgresql_reader.go: %v", err)
	}
	text := string(source)
	start := strings.Index(text, "func (r *PostgreSQLReader) ListSessionEvents")
	end := strings.Index(text, "func (r *PostgreSQLReader) ListThreadEvents")
	if start < 0 || end <= start {
		t.Fatal("could not locate ListSessionEvents source")
	}
	method := text[start:end]
	if !strings.Contains(method, "ORDER BY e.insert_stream_position %s, e.event_id %s") {
		t.Fatal("ListSessionEvents must order by immutable insert_stream_position with event_id tie-break")
	}
	if strings.Contains(method, "ORDER BY e.sequence") {
		t.Fatal("ListSessionEvents must not order cross-thread events by thread-local sequence")
	}
}
