package eventstream

import (
	"bytes"
	"strings"
	"testing"
)

func TestDefaultLoggerCarriesResourceFields(t *testing.T) {
	var buffer bytes.Buffer
	defaultLogger(&buffer).Info("probe")
	line := buffer.String()
	for _, required := range []string{
		`"service.name":"event-stream"`,
		`"service.version":"unknown"`,
		`"deployment.environment":"local"`,
	} {
		if !strings.Contains(line, required) {
			t.Fatalf("default event-stream logger missing %s in %s", required, line)
		}
	}
}
