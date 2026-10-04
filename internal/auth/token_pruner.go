package auth

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	tokenPruneInterval = time.Minute
	tokenPruneDeadline = 2 * time.Second
	tokenPruneBatch    = 1000
)

type PruneResult struct {
	DeletedCount   int
	ExpiredBacklog int64
	OldestExpiry   sql.NullTime
}

// PruneTokens uses the schema owner's retention, batch and concurrent-row
// controls. The resolver borrows Auth's pool and never owns its lifetime.
func (s *AuthorityResolver) PruneTokens(ctx context.Context, batch int) (PruneResult, error) {
	ctx, cancel := context.WithTimeout(ctx, tokenPruneDeadline)
	defer cancel()
	var result PruneResult
	err := s.db.QueryRowContext(ctx, `SELECT deleted_count,expired_backlog,oldest_expiry FROM public.tetral_auth_prune_tokens($1)`, batch).Scan(&result.DeletedCount, &result.ExpiredBacklog, &result.OldestExpiry)
	return result, err
}

// TokenPruner owns one bounded expiry pass per minute. Close cancels the current
// query and joins the loop before Auth closes its database pool.
type TokenPruner struct {
	cancel                                context.CancelFunc
	done                                  chan struct{}
	mu                                    sync.Mutex
	last                                  PruneResult
	lastSuccess                           time.Time
	succeeded, failed, cancelled, deleted uint64
	healthy                               bool
}

func StartTokenPruner(ctx context.Context, resolver *AuthorityResolver, logger *slog.Logger) *TokenPruner {
	ctx, cancel := context.WithCancel(ctx)
	p := &TokenPruner{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(tokenPruneInterval)
		defer ticker.Stop()
		p.run(ctx, resolver.PruneTokens, logger, ticker.C)
	}()
	return p
}

func (p *TokenPruner) Close() {
	if p != nil {
		p.cancel()
		<-p.done
	}
}

func (p *TokenPruner) run(ctx context.Context, prune func(context.Context, int) (PruneResult, error), logger *slog.Logger, ticks <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		result, err := prune(ctx, tokenPruneBatch)
		p.mu.Lock()
		recovered := p.failed > 0 && !p.healthy
		if err == nil {
			p.last, p.lastSuccess, p.healthy = result, time.Now(), true
			p.succeeded++
			p.deleted += uint64(result.DeletedCount)
		} else if ctx.Err() != nil {
			p.cancelled++
		} else {
			p.failed++
			p.healthy = false
		}
		p.mu.Unlock()
		if logger != nil {
			if err != nil && ctx.Err() == nil {
				logger.Warn("auth.token_prune.failed", "phase", "token_prune", "error.class", "dependency_unavailable", "error.code", "auth_token_prune_unavailable", "error.message_safe", "expired token cleanup unavailable")
			} else if err == nil && recovered {
				logger.Info("auth.token_prune.recovered", "phase", "token_prune", "recovery.event", "auth.token_prune.failed")
			}
		}
	}
}

// Collector reports one aggregate snapshot without database work or credential
// labels. A failed pass retains the last successful capacity observations.
func (p *TokenPruner) Collector() workload.MetricsCollector {
	return func(context.Context) ([]workload.Metric, error) {
		if p == nil {
			return nil, nil
		}
		p.mu.Lock()
		last, lastSuccess, healthy := p.last, p.lastSuccess, p.healthy
		succeeded, failed, cancelled, deleted := p.succeeded, p.failed, p.cancelled, p.deleted
		p.mu.Unlock()
		var age, successAt, status float64
		if last.OldestExpiry.Valid {
			age = max(0, time.Since(last.OldestExpiry.Time).Seconds())
		}
		if !lastSuccess.IsZero() {
			successAt = float64(lastSuccess.Unix())
		}
		if healthy {
			status = 1
		}
		return []workload.Metric{
			{Name: "tetral_auth_token_prune_passes_total", Help: "Completed token expiry maintenance passes.", Type: "counter", Labels: []workload.MetricLabel{{Name: "status", Value: "success"}}, Value: float64(succeeded)},
			{Name: "tetral_auth_token_prune_passes_total", Help: "Completed token expiry maintenance passes.", Type: "counter", Labels: []workload.MetricLabel{{Name: "status", Value: "failed"}}, Value: float64(failed)},
			{Name: "tetral_auth_token_prune_passes_total", Help: "Completed token expiry maintenance passes.", Type: "counter", Labels: []workload.MetricLabel{{Name: "status", Value: "cancelled"}}, Value: float64(cancelled)},
			{Name: "tetral_auth_token_prune_deleted_total", Help: "Access token rows deleted by this process.", Type: "counter", Value: float64(deleted)},
			{Name: "tetral_auth_token_prune_expired_backlog", Help: "Eligible rows observed after the last successful pass.", Type: "gauge", Value: float64(last.ExpiredBacklog)},
			{Name: "tetral_auth_token_prune_oldest_expiry_age_seconds", Help: "Current age of the oldest eligible expiry observed by the last successful pass.", Type: "gauge", Value: age},
			{Name: "tetral_auth_token_prune_last_success_timestamp_seconds", Help: "Last successful pass time, or zero before success.", Type: "gauge", Value: successAt},
			{Name: "tetral_auth_token_prune_healthy", Help: "Whether the latest completed pass succeeded, or zero before a pass.", Type: "gauge", Value: status},
		}, nil
	}
}
