package workload

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// rejectionEpisodeGap is the quiet time after which a point's rejections for
// one cause count as a new episode.
const rejectionEpisodeGap = 60 * time.Second

// Rejection is the error a pressure point returns when it refuses work. An
// observation that ends with it counts outcome rejected, never error.
type Rejection struct {
	Point string
	Cause string
}

func (r *Rejection) Error() string { return r.Point + " refused work: " + r.Cause }

// PressurePoint is a place that refuses work. It counts every refusal in
// <point>_rejected_total{cause} and records the refusals of one cause as an
// episode: pressure.rejecting_started at the first refusal after 60 quiet
// seconds, pressure.rejecting_ended with the episode's count at the first
// admission or refusal 60 seconds or more after its last refusal.
type PressurePoint struct {
	name     string
	causes   map[string]bool
	rejected *Counter
	clock    func() time.Time
	mu       sync.Mutex
	episodes map[string]*rejectionEpisode
}

type rejectionEpisode struct {
	last  time.Time
	count int64
}

// NewPressurePoint registers point's rejection counter on registry; causes
// is the point's fixed set.
func NewPressurePoint(registry *Registry, point string, causes ...string) *PressurePoint {
	p := &PressurePoint{name: point, causes: map[string]bool{}, clock: time.Now, episodes: map[string]*rejectionEpisode{}}
	for _, cause := range causes {
		p.causes[cause] = true
	}
	p.rejected = registry.Counter(point+"_rejected_total", "Work refused at this point, by cause.", "cause")
	return p
}

// WithClock replaces the point's clock, for tests.
func (p *PressurePoint) WithClock(clock func() time.Time) *PressurePoint {
	p.clock = clock
	return p
}

// Reject counts one refusal and returns the Rejection to hand the caller.
func (p *PressurePoint) Reject(ctx context.Context, cause string) error {
	if !p.causes[cause] {
		panic("workload: pressure point " + p.name + " has no cause " + cause)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock()
	p.endQuietEpisodes(ctx, now)
	episode := p.episodes[cause]
	if episode == nil {
		episode = &rejectionEpisode{}
		p.episodes[cause] = episode
		p.record(ctx, "pressure.rejecting_started", cause)
	}
	episode.last = now
	episode.count++
	p.rejected.Add(1, cause)
	return &Rejection{Point: p.name, Cause: cause}
}

// Admit marks one admission, which ends the episodes that have been quiet
// for 60 seconds or more.
func (p *PressurePoint) Admit(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.episodes) > 0 {
		p.endQuietEpisodes(ctx, p.clock())
	}
}

func (p *PressurePoint) endQuietEpisodes(ctx context.Context, now time.Time) {
	for cause, episode := range p.episodes {
		if now.Sub(episode.last) >= rejectionEpisodeGap {
			p.record(ctx, "pressure.rejecting_ended", cause, slog.Int64("pressure.rejected", episode.count))
			delete(p.episodes, cause)
		}
	}
}

func (p *PressurePoint) record(ctx context.Context, event, cause string, extra ...slog.Attr) {
	attrs := append([]slog.Attr{slog.String("pressure.point", p.name), slog.String("pressure.cause", cause)}, extra...)
	CurrentTelemetry().logger.LogAttrs(ctx, slog.LevelWarn, event, attrs...)
}
