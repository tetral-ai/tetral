package helm_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

func TestPublicStreamingBrokerConfiguration(t *testing.T) {
	hel := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	for _, profile := range []string{"standard-routed", "hardened"} {
		t.Run(profile, func(t *testing.T) {
			objects := uniqueObjects(t, renderChart(t, hel, chart, "transport.profile="+profile, "replicas.eventStream=3"))
			stream := objects["apps/v1|Deployment|tetral-system|event-stream"]
			if stream["spec"].(map[string]any)["replicas"] != 3 {
				t.Fatal("configured SSE replica count not projected")
			}
			for role, secret := range map[string]string{"provider-gateway": "tetral-nats-publisher", "event-stream": "tetral-nats-subscriber"} {
				deployment := objects["apps/v1|Deployment|tetral-system|"+role]
				pod := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
				container := pod["containers"].([]any)[0].(map[string]any)
				env := map[string]string{}
				for _, item := range container["env"].([]any) {
					v := item.(map[string]any)
					if value, ok := v["value"].(string); ok {
						env[v["name"].(string)] = value
					}
				}
				scheme := "nats"
				if profile == "hardened" {
					scheme = "tls"
				}
				if env["TETRAL_NATS_SERVERS"] != scheme+"://tetral-nats.tetral-system.svc.cluster.local:4222" || env["TETRAL_NATS_USER_PATH"] != "/var/run/tetral/nats-role/user" || env["TETRAL_NATS_PASSWORD_PATH"] != "/var/run/tetral/nats-role/password" {
					t.Fatal("role/file/verified broker wiring missing")
				}
				found := false
				for _, item := range pod["volumes"].([]any) {
					v := item.(map[string]any)
					if v["name"] == "nats-role" {
						found = true
						if v["secret"].(map[string]any)["secretName"] != secret {
							t.Fatal("wrong preview role credential mount")
						}
					}
				}
				if !found {
					t.Fatal("preview credentials not mounted")
				}
				if role == "provider-gateway" {
					for key, value := range map[string]string{"TETRAL_GATEWAY_PREVIEW_QUEUE_BYTES": "4194304", "TETRAL_GATEWAY_PREVIEW_QUEUE_FRAMES": "1024", "TETRAL_GATEWAY_PREVIEW_BATCH_BYTES": "262144", "TETRAL_GATEWAY_PREVIEW_BATCH_FRAMES": "64", "TETRAL_GATEWAY_PREVIEW_FLUSH_TIMEOUT_MS": "1000", "TETRAL_NATS_CONNECT_TIMEOUT_MS": "1000", "TETRAL_NATS_RETRY_MAX_MS": "5000", "TETRAL_NATS_CREDENTIAL_POLL_MS": "250", "TETRAL_NATS_PING_INTERVAL_MS": "1000", "TETRAL_NATS_MAX_PING_OUT": "1"} {
						if env[key] != value {
							t.Fatalf("publisher bound projection %s=%q differs", key, env[key])
						}
					}
				} else {
					for key, value := range map[string]string{"TETRAL_NATS_PING_INTERVAL_MS": "120000", "TETRAL_NATS_MAX_PING_OUT": "2"} {
						if env[key] != value {
							t.Fatalf("subscriber heartbeat projection %s=%q differs", key, env[key])
						}
					}
					native, err := eventstream.NATSConfigFromEnv(func(key string) string { return env[key] })
					defaults, defaultErr := eventstream.NATSConfigFromEnv(func(string) string { return "" })
					if err != nil || defaultErr != nil || native.PingInterval != defaults.PingInterval || native.MaxPingsOutstanding != defaults.MaxPingsOutstanding {
						t.Fatalf("typed Go NATS heartbeat default projection differs: %v %v", err, defaultErr)
					}
					actual, err := eventstream.StreamConfigFromEnv(func(key string) string { return env[key] })
					if err != nil || actual != eventstream.DefaultStreamConfig() {
						t.Fatalf("typed subscriber default projection differs: %v", err)
					}
					for _, key := range []string{"TETRAL_EVENT_STREAM_POLL_INTERVAL_MS", "TETRAL_EVENT_STREAM_HEARTBEAT_INTERVAL_MS", "TETRAL_EVENT_STREAM_WRITE_TIMEOUT_MS", "TETRAL_EVENT_STREAM_PREVIEW_SETUP_TIMEOUT_MS", "TETRAL_EVENT_STREAM_HUB_MAX_BYTES", "TETRAL_EVENT_STREAM_HUB_MAX_FRAMES", "TETRAL_EVENT_STREAM_VIEWER_MAX_BYTES", "TETRAL_EVENT_STREAM_VIEWER_MAX_FRAMES", "TETRAL_EVENT_STREAM_SUBSCRIPTION_MAX_BYTES", "TETRAL_EVENT_STREAM_SUBSCRIPTION_MAX_FRAMES", "TETRAL_EVENT_STREAM_ACTIVE_REQUESTS"} {
						if env[key] == "" {
							t.Fatalf("missing checked subscriber projection %s", key)
						}
					}
				}
				if profile == "hardened" {
					for _, key := range []string{"CA", "CERT", "KEY"} {
						if env["TETRAL_NATS_TLS_"+key+"_PATH"] == "" {
							t.Fatal("partial protected preview material")
						}
					}
				}
				if objects["networking.k8s.io/v1|NetworkPolicy|tetral-system|"+role+"-nats"] == nil {
					t.Fatal("preview broker egress not granted")
				}
			}
			for _, role := range []string{"api", "auth", "mcp-connector", "job-runner"} {
				if strings.Contains(fmt.Sprint(objects["apps/v1|Deployment|tetral-system|"+role]), "TETRAL_NATS_") {
					t.Fatalf("broker credentials reached %s", role)
				}
			}
			if strings.Contains(fmt.Sprint(objects["apps/v1|Deployment|tetral-agent-runtime|agent-runtime"]), "TETRAL_NATS_") {
				t.Fatal("Runtime received broker credentials")
			}
		})
	}
	without := uniqueObjects(t, renderChart(t, hel, chart, "preview.enabled=false"))
	for _, role := range []string{"provider-gateway", "event-stream"} {
		if strings.Contains(fmt.Sprint(without["apps/v1|Deployment|tetral-system|"+role]), "TETRAL_NATS_") || without["networking.k8s.io/v1|NetworkPolicy|tetral-system|"+role+"-nats"] != nil {
			t.Fatal("formal-only installation retained preview transport wiring")
		}
	}
}

func TestPublicStreamingOperationalOverrides(t *testing.T) {
	helm := requireHelm(t)
	chart := filepath.Join(engineRoot(t), "deploy", "helm", "tetral")
	objects := uniqueObjects(t, renderChart(t, helm, chart, "preview.gateway.queueBytes=524288", "preview.gateway.batchBytes=131072", "preview.gateway.retryMaxMs=4000", "preview.subscriber.reconnectWaitMs=700", "preview.gateway.pingIntervalMs=2500", "preview.gateway.maxPingOut=3", "preview.subscriber.pingIntervalMs=90000", "preview.subscriber.maxPingOut=4", "eventStream.pollIntervalMs=500", "eventStream.viewerMaxFrames=64"))
	for role, expected := range map[string]map[string]string{
		"provider-gateway": {"TETRAL_GATEWAY_PREVIEW_QUEUE_BYTES": "524288", "TETRAL_GATEWAY_PREVIEW_BATCH_BYTES": "131072", "TETRAL_NATS_RETRY_MAX_MS": "4000", "TETRAL_NATS_PING_INTERVAL_MS": "2500", "TETRAL_NATS_MAX_PING_OUT": "3"},
		"event-stream":     {"TETRAL_NATS_RECONNECT_WAIT_MS": "700", "TETRAL_NATS_PING_INTERVAL_MS": "90000", "TETRAL_NATS_MAX_PING_OUT": "4", "TETRAL_EVENT_STREAM_POLL_INTERVAL_MS": "500", "TETRAL_EVENT_STREAM_VIEWER_MAX_FRAMES": "64"},
	} {
		pod := objects["apps/v1|Deployment|tetral-system|"+role]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		env := pod["containers"].([]any)[0].(map[string]any)["env"].([]any)
		projected := map[string]string{}
		for _, item := range env {
			value := item.(map[string]any)
			key := value["name"].(string)
			if text, ok := value["value"].(string); ok {
				projected[key] = text
			}
			if want, ok := expected[key]; ok {
				if value["value"] != want {
					t.Fatalf("override %s differs", key)
				}
				delete(expected, key)
			}
		}
		if len(expected) != 0 {
			t.Fatalf("missing operational overrides: %v", expected)
		}
		if role == "event-stream" {
			actual, err := eventstream.NATSConfigFromEnv(func(key string) string { return projected[key] })
			if err != nil || actual.PingInterval != 90*time.Second || actual.MaxPingsOutstanding != 4 {
				t.Fatalf("typed Go NATS heartbeat override projection differs: %v", err)
			}
		}
	}
	for _, invalid := range []string{"eventStream.pollIntervalMs=0", "eventStream.writeTimeoutMs=60001", "eventStream.viewerMaxBytes=8388609", "eventStream.hubMaxFrames=1", "preview.gateway.batchBytes=262145", "preview.gateway.queueBytes=1", "preview.gateway.batchFrames=1025", "preview.gateway.retryMaxMs=60001", "preview.subscriber.reconnectWaitMs=5001", "preview.gateway.queueFrames=1.5", "preview.enabled=yes", "preview.gateway.pingIntervalMs=0", "preview.gateway.pingIntervalMs=60001", "preview.gateway.maxPingOut=0", "preview.gateway.maxPingOut=17", "preview.gateway.maxPingOut=1.5", "preview.subscriber.pingIntervalMs=0", "preview.subscriber.pingIntervalMs=3600001", "preview.subscriber.maxPingOut=0", "preview.subscriber.maxPingOut=17", "preview.subscriber.pingIntervalMs=1.5"} {
		requireRenderError(t, helm, chart, []string{invalid})
	}
}
