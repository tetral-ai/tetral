package transportsecurity

import (
	"context"
	"errors"
	"net/http"
	"time"
)

const EnvRoutingProxyRequired = "TETRAL_ROUTING_PROXY_REQUIRED"

// WaitForRoutingProxy gates application startup on the mandatory local proxy.
// It accepts no destination override: an unrelated healthy process cannot
// substitute for the workload's own routing sidecar.
func WaitForRoutingProxy(ctx context.Context, required bool) error {
	if !required {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:15021/healthz/ready", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errors.New("mandatory routing proxy did not become ready")
		case <-ticker.C:
		}
	}
}
