package jobrunner

import (
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

// Every direct command is one attempt. Queue owns later delivery attempts;
// command channels never add generic retries. The earlier caller/lease deadline
// always wins over this boot-time attempt policy.
type RuntimeCommandPolicy struct {
	AcceptInput, RecoverThread, AcceptAgentMail, AcceptTaskNotification    time.Duration
	Interrupt, ResolveToolConfirmation, ApplyRuntimeConfig, CleanupSession time.Duration
}

func DefaultRuntimeCommandPolicy() RuntimeCommandPolicy {
	return RuntimeCommandPolicy{30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second}
}

func (p *RuntimeCommandPolicy) methods() map[string]*time.Duration {
	return map[string]*time.Duration{"AcceptInput": &p.AcceptInput, "RecoverThread": &p.RecoverThread, "AcceptAgentMail": &p.AcceptAgentMail, "AcceptTaskNotification": &p.AcceptTaskNotification, "Interrupt": &p.Interrupt, "ResolveToolConfirmation": &p.ResolveToolConfirmation, "ApplyRuntimeConfig": &p.ApplyRuntimeConfig, "CleanupSession": &p.CleanupSession}
}

func RuntimeCommandPolicyFromEnv(getenv func(string) string) (RuntimeCommandPolicy, error) {
	p := DefaultRuntimeCommandPolicy()
	for name, value := range p.methods() {
		var words []string
		var start int
		for i, ch := range name {
			if i > 0 && ch >= 'A' && ch <= 'Z' {
				words = append(words, name[start:i])
				start = i
			}
		}
		words = append(words, name[start:])
		key := "TETRAL_RUNTIME_" + strings.ToUpper(strings.Join(words, "_")) + "_TIMEOUT_MS"
		if raw := getenv(key); raw != "" {
			duration, err := parsePositiveMilliseconds(raw, key)
			if err != nil {
				return RuntimeCommandPolicy{}, err
			}
			*value = duration
		}
	}
	return p, nil
}

func (p RuntimeCommandPolicy) timeout(method string) (time.Duration, error) {
	if p == (RuntimeCommandPolicy{}) {
		p = DefaultRuntimeCommandPolicy()
	}
	value, exists := p.methods()[method]
	if !exists || *value <= 0 {
		return 0, workload.NewConfigError("Runtime command method has no valid explicit attempt policy: " + method)
	}
	return *value, nil
}
