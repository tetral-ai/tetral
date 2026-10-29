package computerhub

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workload"
)

// hubMetrics are the hub's own families of this pod: the streams it holds,
// which its autoscaler averages, the connected events it wrote, the end of
// each stream it held by cause and the queue wait of each instruction's
// first write. The instruction pressure gauges are read from the database at
// scrape (instructionPressure).
type hubMetrics struct {
	registry    *workload.Registry
	openStreams *workload.Gauge
	started     *workload.Counter
	ended       *workload.Counter
	queueWait   *workload.Histogram
}

// The causes of computerhub_streams_ended_total; a stream counts under the
// first one that ended it.
const (
	endDrain           = "drain"
	endAgeLimit        = "age_limit"
	endStalled         = "stalled"
	endConnectionEnded = "connection_ended"
	endMoved           = "moved"
	endClientClosed    = "client_closed"
)

func newHubMetrics() *hubMetrics {
	registry := workload.NewRegistry(ServiceName)
	m := &hubMetrics{
		registry:    registry,
		openStreams: registry.Gauge("computerhub_open_streams", "Client streams this pod holds."),
		started:     registry.Counter("computerhub_connections_started_total", "Connected events written, by new or resumed connection.", "kind"),
		ended:       registry.Counter("computerhub_streams_ended_total", "Streams this pod held, each counted once when it ends, by the first cause that ended it.", "cause"),
		queueWait:   registry.Histogram("computerhub_instructions_queue_wait_seconds", "Time from storing a Session instruction to its first write to a stream."),
	}
	m.openStreams.Set(0)
	return m
}

func (m *hubMetrics) streamStarted(resumed bool) {
	if m == nil {
		return
	}
	kind := "new"
	if resumed {
		kind = "resumed"
	}
	m.started.Add(1, kind)
	m.openStreams.Add(1)
}

func (m *hubMetrics) streamEnded() {
	if m != nil {
		m.openStreams.Add(-1)
	}
}

func (m *hubMetrics) streamEndedBy(cause string) {
	if m != nil {
		m.ended.Add(1, cause)
	}
}

func (m *hubMetrics) instructionDelivered(queued time.Duration) {
	if m != nil {
		m.queueWait.Observe(queued.Seconds())
	}
}

// instructionPressure reads the pressure point computerhub_instructions: the
// unreported instructions of every live connection written to a stream
// (in_progress), not yet written (queued) and the age of the oldest queued
// one by the database clock. Every pod reports the same values.
func instructionPressure(database *dbconnect.Client) workload.MetricsCollector {
	return func(ctx context.Context) ([]workload.Metric, error) {
		var pressure instance.InstructionPressure
		err := database.WithTx(ctx, "computer_hub.instruction_pressure", nil, func(tx *dbconnect.Tx) error {
			var err error
			pressure, err = instance.ReadInstructionPressure(ctx, tx)
			return err
		})
		if err != nil {
			return nil, err
		}
		return []workload.Metric{
			{Name: "computerhub_instructions_in_progress", Help: "Session instructions written to a stream whose final report is not stored.", Type: "gauge", Value: float64(pressure.InProgress)},
			{Name: "computerhub_instructions_queued", Help: "Session instructions stored and not yet written to any stream.", Type: "gauge", Value: float64(pressure.Queued)},
			{Name: "computerhub_instructions_oldest_queued_seconds", Help: "Age of the oldest Session instruction not yet written to any stream.", Type: "gauge", Value: pressure.OldestQueuedSeconds},
		}, nil
	}
}
