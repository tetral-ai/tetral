package eventstream

import (
	"fmt"
	"strconv"
	"time"
)

// StreamConfig is parsed once at process startup. Byte and frame counts bound
// both queued and currently dispatched preview storage.
type StreamConfig struct {
	PollInterval          time.Duration
	HeartbeatInterval     time.Duration
	WriteTimeout          time.Duration
	PreviewSetupTimeout   time.Duration
	HubMaxBytes           int
	HubMaxFrames          int
	ViewerMaxBytes        int
	ViewerMaxFrames       int
	SubscriptionMaxBytes  int
	SubscriptionMaxFrames int
	ActiveRequests        int
}

func DefaultStreamConfig() StreamConfig {
	return StreamConfig{
		PollInterval: time.Second, HeartbeatInterval: time.Second, WriteTimeout: 10 * time.Second, PreviewSetupTimeout: time.Second,
		HubMaxBytes: 8 * 1024 * 1024, HubMaxFrames: 2048, ViewerMaxBytes: 256 * 1024, ViewerMaxFrames: 128,
		SubscriptionMaxBytes: 8 * 1024 * 1024, SubscriptionMaxFrames: 2048, ActiveRequests: 32,
	}
}

func StreamConfigFromEnv(getenv func(string) string) (StreamConfig, error) {
	cfg := DefaultStreamConfig()
	for _, item := range []struct {
		key  string
		dest *time.Duration
		max  int
	}{
		{"TETRAL_EVENT_STREAM_POLL_INTERVAL_MS", &cfg.PollInterval, 60000},
		{"TETRAL_EVENT_STREAM_HEARTBEAT_INTERVAL_MS", &cfg.HeartbeatInterval, 60000},
		{"TETRAL_EVENT_STREAM_WRITE_TIMEOUT_MS", &cfg.WriteTimeout, 60000},
		{"TETRAL_EVENT_STREAM_PREVIEW_SETUP_TIMEOUT_MS", &cfg.PreviewSetupTimeout, 10000},
	} {
		if raw := getenv(item.key); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 || value > item.max {
				return cfg, fmt.Errorf("invalid %s", item.key)
			}
			*item.dest = time.Duration(value) * time.Millisecond
		}
	}
	for _, item := range []struct {
		key  string
		dest *int
		max  int
	}{
		{"TETRAL_EVENT_STREAM_HUB_MAX_BYTES", &cfg.HubMaxBytes, 64 * 1024 * 1024},
		{"TETRAL_EVENT_STREAM_HUB_MAX_FRAMES", &cfg.HubMaxFrames, 16384},
		{"TETRAL_EVENT_STREAM_VIEWER_MAX_BYTES", &cfg.ViewerMaxBytes, 8 * 1024 * 1024},
		{"TETRAL_EVENT_STREAM_VIEWER_MAX_FRAMES", &cfg.ViewerMaxFrames, 4096},
		{"TETRAL_EVENT_STREAM_SUBSCRIPTION_MAX_BYTES", &cfg.SubscriptionMaxBytes, 64 * 1024 * 1024},
		{"TETRAL_EVENT_STREAM_SUBSCRIPTION_MAX_FRAMES", &cfg.SubscriptionMaxFrames, 16384},
		{"TETRAL_EVENT_STREAM_ACTIVE_REQUESTS", &cfg.ActiveRequests, 4096},
	} {
		if raw := getenv(item.key); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 || value > item.max {
				return cfg, fmt.Errorf("invalid %s", item.key)
			}
			*item.dest = value
		}
	}
	return cfg, cfg.Validate()
}

func (c StreamConfig) Validate() error {
	if c.PollInterval <= 0 || c.HeartbeatInterval <= 0 || c.WriteTimeout <= 0 || c.PreviewSetupTimeout <= 0 || c.HubMaxBytes <= 0 || c.HubMaxFrames <= 0 || c.ViewerMaxBytes <= 0 || c.ViewerMaxFrames <= 0 || c.SubscriptionMaxBytes <= 0 || c.SubscriptionMaxFrames <= 0 || c.ActiveRequests <= 0 {
		return fmt.Errorf("event stream bounds must be positive")
	}
	if c.ViewerMaxBytes > c.HubMaxBytes || c.ViewerMaxFrames > c.HubMaxFrames {
		return fmt.Errorf("viewer preview bounds exceed hub bounds")
	}
	return nil
}
