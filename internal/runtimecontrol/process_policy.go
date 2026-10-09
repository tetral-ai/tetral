package runtimecontrol

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	EnvProcessRegistrationTimeout = "TETRAL_RUNTIME_REGISTER_TIMEOUT_MS"
	EnvProcessReportTimeout       = "TETRAL_RUNTIME_REPORT_TIMEOUT_MS"
	EnvProcessReportInterval      = "TETRAL_RUNTIME_REPORT_INTERVAL_MS"
	EnvProcessFreshness           = "TETRAL_RUNTIME_PROCESS_FRESHNESS_MS"
)

// ProcessPolicy is the shared process-custody policy consumed by Bridge and
// Runner. Runtime consumes the same boot-only settings for its actual timers.
type ProcessPolicy struct {
	RegistrationTimeout time.Duration
	ReportTimeout       time.Duration
	ReportInterval      time.Duration
	Freshness           time.Duration
}

func DefaultProcessPolicy() ProcessPolicy {
	return ProcessPolicy{RegistrationTimeout: 5 * time.Second, ReportTimeout: time.Second, ReportInterval: 2 * time.Second, Freshness: 10 * time.Second}
}

func (p ProcessPolicy) Validate() error {
	if p.RegistrationTimeout <= 0 || p.ReportTimeout <= 0 || p.ReportInterval <= p.ReportTimeout || p.Freshness <= p.ReportInterval {
		return workload.NewConfigError("runtime process policy requires positive registration timeout and report timeout < report interval < freshness")
	}
	return nil
}

func ProcessPolicyFromEnv(getenv func(string) string) (ProcessPolicy, error) {
	policy := DefaultProcessPolicy()
	if getenv == nil {
		return policy, nil
	}
	for _, setting := range []struct {
		key    string
		target *time.Duration
	}{
		{EnvProcessRegistrationTimeout, &policy.RegistrationTimeout}, {EnvProcessReportTimeout, &policy.ReportTimeout},
		{EnvProcessReportInterval, &policy.ReportInterval}, {EnvProcessFreshness, &policy.Freshness},
	} {
		raw := getenv(setting.key)
		if raw == "" {
			continue
		}
		value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil || value <= 0 || value > math.MaxInt64/int64(time.Millisecond) {
			return ProcessPolicy{}, workload.NewConfigError(setting.key + " must be positive milliseconds")
		}
		*setting.target = time.Duration(value) * time.Millisecond
	}
	return policy, policy.Validate()
}
