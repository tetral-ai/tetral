package agent

import (
	"database/sql"
	"testing"
	"time"
)

// Durable timestamps are instants, but the API renders them as strings, and Go
// renders a time.Time in whatever location it carries. The database driver hands
// a scanned timestamptz back in the process's local zone, so a projection that
// forgets to normalize publishes "2026-07-25T08:13:41+08:00" on a host set to
// Asia/Shanghai and "2026-07-25T00:13:41Z" on a host set to UTC — the same
// instant, two different response bodies, decided by an environment variable no
// test controls. This pins the normalization at the projection boundary.
func TestAgentProjectionNormalizesScannedTimestampsToUTC(t *testing.T) {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	instant := time.Date(2026, 7, 25, 0, 13, 41, 123456000, time.UTC)
	scanned := instant.In(shanghai)

	projected, err := agentFromStoredConfig(
		"agnt_timezone",
		"agev_timezone",
		1,
		`{"name":"timezone","instructions":"i","model":"anthropic/claude-fable-5"}`,
		sql.NullTime{Time: scanned, Valid: true},
		scanned,
		scanned,
	)
	if err != nil {
		t.Fatalf("agentFromStoredConfig: %v", err)
	}

	for name, value := range map[string]time.Time{
		"created_at":  projected.CreatedAt,
		"updated_at":  projected.UpdatedAt,
		"archived_at": *projected.ArchivedAt,
	} {
		if !value.Equal(instant) {
			t.Errorf("%s instant = %s; want %s", name, value, instant)
		}
		if offset := value.Format("Z07:00"); offset != "Z" {
			t.Errorf("%s renders with offset %q; want UTC so the response body does not depend on the host timezone", name, offset)
		}
	}
}
