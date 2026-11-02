package helm_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	bridge "github.com/tetral-ai/tetral/services/bridge"
)

func TestBlobDeleteDeadlineProjection(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy/helm/tetral")
	for _, ms := range []int64{30000, 1234} {
		t.Run(fmt.Sprint(ms), func(t *testing.T) {
			values := []string{}
			if ms != 30000 {
				values = append(values, fmt.Sprintf("blob.deleteTimeoutMs=%d", ms))
			}
			envs := configurationEnvironments(t, uniqueObjects(t, renderChart(t, helm, chart, values...)))
			consumers := 0
			for name, env := range envs {
				if env[blob.EnvTLSCAPath] == "" {
					continue
				}
				consumers++
				if env[blob.EnvDeleteTimeoutMS] != fmt.Sprint(ms) {
					t.Fatalf("%s delete timeout = %q", name, env[blob.EnvDeleteTimeoutMS])
				}
				parsed, err := blob.ParseDeleteTimeoutMS(env[blob.EnvDeleteTimeoutMS])
				if err != nil || parsed != time.Duration(ms)*time.Millisecond {
					t.Fatalf("%s parser: %s %v", name, parsed, err)
				}
			}
			if consumers == 0 {
				t.Fatal("no object-store consumers checked")
			}
		})
	}
	for _, value := range []string{"0", "-1", "1.5", `"bad"`} {
		if out, err := renderChartFailure(t, helm, chart, "blob.deleteTimeoutMs="+value); err == nil {
			t.Fatalf("invalid timeout %s rendered: %s", value, out)
		}
	}
}

func TestBridgeAttachmentLifetimeNumericValuesFile(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy/helm/tetral")
	for _, seconds := range []int64{3600, 31536000, 63072000} {
		t.Run(fmt.Sprint(seconds), func(t *testing.T) {
			values := filepath.Join(t.TempDir(), "values.yaml")
			if err := os.WriteFile(values, []byte(fmt.Sprintf("imageStore:\n  lifetimeSeconds: %d\n", seconds)), 0600); err != nil {
				t.Fatal(err)
			}
			output, err := exec.Command(helm, "template", "tetral", chart, "-f", values).CombinedOutput()
			if err != nil {
				t.Fatalf("render: %v %s", err, output)
			}
			envs := configurationEnvironments(t, uniqueObjects(t, decodeManifestObjects(t, bytes.NewReader(output), "numeric values file")))
			cfg, err := bridge.BridgeAPIConfigFromEnv(envs["bridge"])
			if err != nil || cfg.AttachmentLifetime != time.Duration(seconds)*time.Second {
				t.Fatalf("numeric lifetime: %s %v", cfg.AttachmentLifetime, err)
			}
		})
	}
}
