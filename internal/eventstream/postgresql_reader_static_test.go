package eventstream_test

import (
	"os"
	"strings"
	"testing"
)

func TestPostgreSQLReaderUsesReadOnlyTransactions(t *testing.T) {
	source, err := os.ReadFile("postgresql_reader.go")
	if err != nil {
		t.Fatalf("read postgresql_reader.go: %v", err)
	}
	text := string(source)
	if strings.Contains(text, ".WithWorkspaceTx(") {
		t.Fatalf("PostgreSQLReader must not use read-write workspace transactions")
	}
	if got := strings.Count(text, ".WithWorkspaceReadOnlyTx("); got != 6 {
		t.Fatalf("read-only workspace transaction count = %d; want 6", got)
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
