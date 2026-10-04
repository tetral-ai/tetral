package eventstream

import (
	"reflect"
	"testing"
	"time"
)

func TestStreamConfigDefaultsOverridesAndInvalidBounds(t *testing.T) {
	cfg, err := StreamConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PollInterval != time.Second || cfg.HeartbeatInterval != time.Second || cfg.WriteTimeout != 10*time.Second || cfg.PreviewSetupTimeout != time.Second || cfg.HubMaxBytes != 8388608 || cfg.HubMaxFrames != 2048 || cfg.ViewerMaxBytes != 262144 || cfg.ViewerMaxFrames != 128 {
		t.Fatalf("production defaults=%+v", cfg)
	}
	env := map[string]string{"TETRAL_EVENT_STREAM_POLL_INTERVAL_MS": "37", "TETRAL_EVENT_STREAM_WRITE_TIMEOUT_MS": "83", "TETRAL_EVENT_STREAM_VIEWER_MAX_BYTES": "1024", "TETRAL_EVENT_STREAM_VIEWER_MAX_FRAMES": "3"}
	cfg, err = StreamConfigFromEnv(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	options := newOptions(WithStreamConfig(cfg))
	if options.streamConfig.PollInterval != 37*time.Millisecond || options.streamConfig.WriteTimeout != 83*time.Millisecond || options.streamConfig.ViewerMaxBytes != 1024 || options.streamConfig.ViewerMaxFrames != 3 {
		t.Fatal("validated settings did not reach consuming options")
	}
	for _, invalid := range []map[string]string{{"TETRAL_EVENT_STREAM_WRITE_TIMEOUT_MS": "0"}, {"TETRAL_EVENT_STREAM_POLL_INTERVAL_MS": "-1"}, {"TETRAL_EVENT_STREAM_HUB_MAX_BYTES": "9999999999999999999999999999999999999"}, {"TETRAL_EVENT_STREAM_VIEWER_MAX_BYTES": "8MiB"}, {"TETRAL_EVENT_STREAM_HUB_MAX_FRAMES": "1", "TETRAL_EVENT_STREAM_VIEWER_MAX_FRAMES": "2"}} {
		if _, err := StreamConfigFromEnv(func(key string) string { return invalid[key] }); err == nil {
			t.Fatalf("invalid settings accepted=%v", invalid)
		}
	}
}

func TestNATSConfigOptionalDisabledAndStrictCredentialReferences(t *testing.T) {
	config, err := NATSConfigFromEnv(func(string) string { return "" })
	if err != nil || config.Enabled() {
		t.Fatalf("optional config=%+v %v", config, err)
	}
	if config.PingInterval != 120000*time.Millisecond || config.MaxPingsOutstanding != 2 {
		t.Fatalf("pinned Go heartbeat defaults changed: %+v", config)
	}
	valid := map[string]string{"TETRAL_NATS_SERVERS": "tls://broker-a.test:4222,tls://broker-b.test:4222", "TETRAL_NATS_USER_PATH": "/credentials/user", "TETRAL_NATS_PASSWORD_PATH": "/credentials/password", "TETRAL_NATS_TLS_CA_PATH": "/tls/ca", "TETRAL_NATS_TLS_CERT_PATH": "/tls/cert", "TETRAL_NATS_TLS_KEY_PATH": "/tls/key"}
	config, err = NATSConfigFromEnv(func(key string) string { return valid[key] })
	if err != nil || !reflect.DeepEqual(config.Servers, []string{"tls://broker-a.test:4222", "tls://broker-b.test:4222"}) {
		t.Fatalf("protected config=%+v err=%v", config, err)
	}
	for _, change := range []map[string]string{{"TETRAL_NATS_SERVERS": ""}, {"TETRAL_NATS_PASSWORD_PATH": ""}, {"TETRAL_NATS_TLS_KEY_PATH": ""}, {"TETRAL_NATS_SERVERS": "tls://user:secret@broker.test:4222"}, {"TETRAL_NATS_SERVERS": "tls://127.0.0.1:4222"}, {"TETRAL_NATS_SERVERS": "nats://broker.test:4222/path"}, {"TETRAL_NATS_SERVERS": "nats://broker.test:4222?secret=value"}} {
		candidate := map[string]string{}
		for key, value := range valid {
			candidate[key] = value
		}
		for key, value := range change {
			candidate[key] = value
		}
		if _, err := NATSConfigFromEnv(func(key string) string { return candidate[key] }); err == nil {
			t.Fatalf("invalid NATS configuration accepted=%v", change)
		}
	}
}

func TestNATSHeartbeatConfigRangesAndFailFast(t *testing.T) {
	for _, item := range []struct {
		key     string
		valid   []string
		invalid []string
	}{
		{"TETRAL_NATS_PING_INTERVAL_MS", []string{"1", "120000", "3600000"}, []string{"0", "-1", "3600001", "2m", "9999999999999999999999999999"}},
		{"TETRAL_NATS_MAX_PING_OUT", []string{"1", "2", "16"}, []string{"0", "-1", "17", "1.5", "9999999999999999999999999999"}},
	} {
		for _, raw := range item.valid {
			if _, err := NATSConfigFromEnv(func(key string) string {
				if key == item.key {
					return raw
				}
				return ""
			}); err != nil {
				t.Fatalf("valid %s=%s: %v", item.key, raw, err)
			}
		}
		for _, raw := range item.invalid {
			if _, err := NATSConfigFromEnv(func(key string) string {
				if key == item.key {
					return raw
				}
				return ""
			}); err == nil {
				t.Fatalf("invalid %s=%s accepted", item.key, raw)
			}
		}
	}
	for _, config := range []NATSConfig{{PingInterval: -time.Second}, {PingInterval: time.Hour + time.Millisecond}, {MaxPingsOutstanding: -1}, {MaxPingsOutstanding: 17}} {
		// Local typed validation must precede credential reads and any dial.
		if _, err := NewNATSPreviewTransport(t.Context(), config, DefaultStreamConfig(), nil, nil); err == nil || err.Error() != "invalid NATS heartbeat settings" {
			t.Fatalf("typed fail-fast config=%+v error=%v", config, err)
		}
	}
}
