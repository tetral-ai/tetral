package httpapi

import (
	"bytes"
	"strings"
	"testing"
)

func TestDefaultRouterLoggerCarriesResourceFields(t *testing.T) {
	var buffer bytes.Buffer
	defaultRouterLogger(&buffer).Info("probe")
	line := buffer.String()
	for _, required := range []string{
		`"service.name":"api"`,
		`"service.version":"unknown"`,
		`"deployment.environment":"local"`,
	} {
		if !strings.Contains(line, required) {
			t.Fatalf("default api logger missing %s in %s", required, line)
		}
	}
}
