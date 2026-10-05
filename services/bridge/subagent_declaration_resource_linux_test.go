package agentruntimebridge

import (
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Snapshots bracket the composition, outside declaration deadlines. These are
// process-wide counters, not per-handler attribution or a CPU contention proof.
func observeSubagentDeclarationResources(t *testing.T) {
	t.Helper()
	before := captureSubagentResources()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		after := captureSubagentResources()
		observation := map[string]any{
			"scope":     "Go test process; after composition cleanup, sampler/handler join not established by this observation",
			"elapsedMS": after.at.Sub(before.at).Milliseconds(),
			"before":    before.config, "final": after.config,
			"cpuAvailable":                before.cpuOK && after.cpuOK,
			"gcCountDelta":                after.memory.NumGC - before.memory.NumGC,
			"gcPauseNSDelta":              after.memory.PauseTotalNs - before.memory.PauseTotalNs,
			"allocatedBytesDelta":         after.memory.TotalAlloc - before.memory.TotalAlloc,
			"cgroupQuotaThrottlePressure": "uncaptured; effective hierarchy not resolved",
		}
		if before.cpuOK && after.cpuOK {
			observation["userCPUNSDelta"] = after.userNS - before.userNS
			observation["systemCPUNSDelta"] = after.systemNS - before.systemNS
		}
		data, err := json.Marshal(observation)
		if err != nil {
			t.Error("encode subagent declaration resource observations")
			return
		}
		t.Logf("subagent declaration resource observations=%s", data)
	})
}

type subagentResourceConfig struct {
	NumCPU                   int  `json:"numCPU"`
	GOMAXPROCS               int  `json:"gomaxprocs"`
	ProcessLeaderAllowedCPUs *int `json:"processLeaderAllowedCPUs,omitempty"`
	AffinityUnavailable      bool `json:"affinityUnavailable"`
}
type subagentResourceSnapshot struct {
	at               time.Time
	config           subagentResourceConfig
	memory           runtime.MemStats
	cpuOK            bool
	userNS, systemNS int64
}

func captureSubagentResources() subagentResourceSnapshot {
	s := subagentResourceSnapshot{at: time.Now(), config: subagentResourceConfig{NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), AffinityUnavailable: true}}
	var usage unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &usage) == nil {
		s.cpuOK = true
		s.userNS = int64(usage.Utime.Sec)*int64(time.Second) + int64(usage.Utime.Usec)*int64(time.Microsecond)
		s.systemNS = int64(usage.Stime.Sec)*int64(time.Second) + int64(usage.Stime.Usec)*int64(time.Microsecond)
	}
	runtime.ReadMemStats(&s.memory)
	var affinity unix.CPUSet
	if unix.SchedGetaffinity(unix.Getpid(), &affinity) == nil {
		if count := affinity.Count(); count > 0 {
			s.config.ProcessLeaderAllowedCPUs = &count
			s.config.AffinityUnavailable = false
		}
	}
	return s
}
