package integration

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

// Reserve the entire listener set together, retaining it through child setup.
// Releasing immediately before Start prevents duplicate fixture allocations;
// another process can still race the child to bind after release.
func publicReserveAddresses(t *testing.T, count int) ([]string, func()) {
	t.Helper()
	slots := make([]int, count)
	for i := range slots {
		slots[i] = i
	}
	ports, release, err := testinfra.ReserveLoopbackPorts(slots...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	addresses := make([]string, count)
	for i := range addresses {
		addresses[i] = net.JoinHostPort("127.0.0.1", strconv.Itoa(ports[i]))
	}
	return addresses, release
}

// Child output can contain credentials and dependency text. Retain only fixed
// startup categories, never arbitrary messages, paths, addresses or field values.
func publicStartupDiagnostic(output string) string {
	const limit = 64 * 1024
	if len(output) > limit {
		output = output[:limit]
	}
	for _, line := range strings.Split(output, "\n") {
		var record map[string]json.RawMessage
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		field := func(name string) string { var value string; _ = json.Unmarshal(record[name], &value); return value }
		if field("event.kind") != "startup_failed" {
			continue
		}
		class, cause := field("error.class"), field("startup.cause")
		if class != "config_error" && class != "startup_error" {
			continue
		}
		switch cause {
		case "configuration", "schema", "listener", "dependency_readiness", "unknown":
			diagnostic := fmt.Sprintf("startup.cause=%s error.class=%s", cause, class)
			if listener := field("startup.cause_category"); listener == "grpc" || listener == "metrics" || listener == "http" {
				diagnostic += " startup.cause_category=" + listener
			}
			return diagnostic
		}
	}
	return "startup classification unavailable"
}

func TestPublicProcessFixtureBoundaries(t *testing.T) {
	t.Run("reserved-listeners", func(t *testing.T) {
		addresses, release := publicReserveAddresses(t, 3)
		seen := map[string]bool{}
		for _, address := range addresses {
			if seen[address] {
				t.Fatal("listener addresses duplicated")
			}
			seen[address] = true
			listener, err := net.Listen("tcp", address)
			if err == nil {
				_ = listener.Close()
				t.Fatal("fixture reservation was not held")
			}
		}
		release()
		release()
		for _, address := range addresses {
			listener, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatal("released listener could not bind")
			}
			_ = listener.Close()
		}
	})
	t.Run("safe-startup-classification", func(t *testing.T) {
		secret := "credential-sentinel-do-not-print"
		for _, test := range []struct{ input, want string }{
			{`{"event.kind":"startup_failed","error.class":"config_error","startup.cause":"configuration","error.message_safe":"` + secret + `","database":"` + secret + `"}`, "startup.cause=configuration error.class=config_error"},
			{`{"event.kind":"startup_failed","error.class":"startup_error","startup.cause":"listener","message":"` + secret + `"}`, "startup.cause=listener error.class=startup_error"},
			{`{"event.kind":"startup_failed","error.class":"` + secret + `","startup.cause":"listener"}`, "startup classification unavailable"},
			{`{"event.kind":"startup_failed","error.class":"startup_error","startup.cause":"` + secret + `"}`, "startup classification unavailable"},
			{strings.Repeat("x", 64*1024) + `{"event.kind":"startup_failed","error.class":"config_error","startup.cause":"configuration"}`, "startup classification unavailable"},
			{`{"event.kind":"startup_failed","error.class":"startup_error","startup.cause":"listener","startup.cause_category":"metrics"}`, "startup.cause=listener error.class=startup_error startup.cause_category=metrics"},
			{`{"event.kind":"startup_failed","error.class":"startup_error","startup.cause":"listener","startup.cause_category":"` + secret + `"}`, "startup.cause=listener error.class=startup_error"},
			{secret, "startup classification unavailable"},
		} {
			got := publicStartupDiagnostic(test.input)
			if got != test.want || strings.Contains(got, secret) {
				t.Fatalf("unsafe or incorrect startup projection: %q", got)
			}
		}
	})
}
