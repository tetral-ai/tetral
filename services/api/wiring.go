package tetralapi

import (
	"context"
	"log/slog"

	"github.com/tetral-ai/tetral/internal/workload"
)

// BuildApplicationFunc lets the command tests replace service construction
// while keeping the service-local package as the production composition owner.
type BuildApplicationFunc func(context.Context, ProductionConfig) (*Application, error)

// ApplicationConfig converts workload config into the public API composition
// shape. Domain logic remains reusable internal packages; this service package
// owns the process-to-domain wiring.
func ApplicationConfig(cfg Config, env Env, logger *slog.Logger, httpMetrics *workload.HTTPMetrics) ProductionConfig {
	return ProductionConfig{
		VaultKey:    cfg.VaultKey,
		DataDir:     cfg.DataDir,
		Logger:      logger,
		Env:         env,
		HTTPMetrics: httpMetrics,
	}
}
