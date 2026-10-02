package jobrunner

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workload"
)

type RuntimePlacementPolicy struct {
	ProbeTimeout     time.Duration
	ProbeBudget      time.Duration
	Rounds           int
	MaxResponseBytes int64
	MemoryCutoff     float64
}

func DefaultRuntimePlacementPolicy() RuntimePlacementPolicy {
	return RuntimePlacementPolicy{ProbeTimeout: time.Second, ProbeBudget: 2 * time.Second, Rounds: 2, MaxResponseBytes: 256 * 1024, MemoryCutoff: .8}
}
func (p RuntimePlacementPolicy) Validate() error {
	if p.ProbeTimeout <= 0 || p.ProbeBudget < p.ProbeTimeout || p.Rounds < 1 || p.Rounds > 2 || p.MaxResponseBytes <= 0 || p.MaxResponseBytes > 256*1024 || math.IsNaN(p.MemoryCutoff) || math.IsInf(p.MemoryCutoff, 0) || p.MemoryCutoff <= 0 || p.MemoryCutoff > 1 {
		return workload.NewConfigError("runtime placement policy requires positive bounded probes, at most two rounds, at most 256 KiB and a finite memory cutoff in (0,1]")
	}
	return nil
}
func RuntimePlacementPolicyFromEnv(getenv func(string) string) (RuntimePlacementPolicy, error) {
	p := DefaultRuntimePlacementPolicy()
	for _, field := range []struct {
		name   string
		target *time.Duration
	}{{"TETRAL_RUNTIME_LOAD_PROBE_TIMEOUT_MS", &p.ProbeTimeout}, {"TETRAL_RUNTIME_PLACEMENT_TIMEOUT_MS", &p.ProbeBudget}} {
		if raw := getenv(field.name); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || value <= 0 || value > math.MaxInt64/int64(time.Millisecond) {
				return p, workload.NewConfigError(field.name + " must be positive milliseconds")
			}
			*field.target = time.Duration(value) * time.Millisecond
		}
	}
	if raw := getenv("TETRAL_RUNTIME_PLACEMENT_ROUNDS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			return p, workload.NewConfigError("placement rounds must be an integer")
		}
		p.Rounds = value
	}
	if raw := getenv("TETRAL_RUNTIME_LOAD_MAX_BYTES"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return p, workload.NewConfigError("load response size must be an integer")
		}
		p.MaxResponseBytes = value
	}
	if raw := getenv("TETRAL_RUNTIME_PLACEMENT_MEMORY_CUTOFF"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return p, workload.NewConfigError("placement memory cutoff must be finite")
		}
		p.MemoryCutoff = value
	}
	return p, p.Validate()
}

type RuntimeLoadReport struct{ ActiveSessions, Capacity, MemoryUsage, MemoryLimit float64 }

func ParseRuntimeLoadReport(raw string, cutoff float64) (RuntimeLoadReport, error) {
	names := map[string]bool{"runtimepod_active_sessions": true, "runtimepod_session_capacity": true, "runtimepod_container_memory_usage_bytes": true, "runtimepod_container_memory_limit_bytes": true, "runtimepod_ready": true, "runtimepod_accepting_commands": true}
	values := map[string]float64{}
	scanner := bufio.NewScanner(strings.NewReader(raw))
	scanner.Buffer(make([]byte, 4096), 256*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		name := fields[0]
		base := strings.SplitN(name, "{", 2)[0]
		if !names[base] {
			continue
		}
		if name != base || len(fields) != 2 {
			return RuntimeLoadReport{}, fmt.Errorf("required metric %s has invalid labels or sample", base)
		}
		if _, exists := values[base]; exists {
			return RuntimeLoadReport{}, fmt.Errorf("required metric %s is duplicated", base)
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return RuntimeLoadReport{}, fmt.Errorf("required metric %s is invalid", base)
		}
		values[base] = value
	}
	if err := scanner.Err(); err != nil {
		return RuntimeLoadReport{}, fmt.Errorf("metrics read: %w", err)
	}
	for name := range names {
		if _, ok := values[name]; !ok {
			return RuntimeLoadReport{}, fmt.Errorf("required metric %s is missing", name)
		}
	}
	report := RuntimeLoadReport{ActiveSessions: values["runtimepod_active_sessions"], Capacity: values["runtimepod_session_capacity"], MemoryUsage: values["runtimepod_container_memory_usage_bytes"], MemoryLimit: values["runtimepod_container_memory_limit_bytes"]}
	if report.ActiveSessions != math.Trunc(report.ActiveSessions) || report.Capacity != math.Trunc(report.Capacity) || report.Capacity <= 0 || report.MemoryLimit <= 0 {
		return RuntimeLoadReport{}, fmt.Errorf("metrics counts or finite container limit are invalid")
	}
	if values["runtimepod_ready"] != 1 || values["runtimepod_accepting_commands"] != 1 {
		return RuntimeLoadReport{}, fmt.Errorf("runtime does not admit new commands")
	}
	if report.ActiveSessions >= report.Capacity || report.MemoryUsage/report.MemoryLimit >= cutoff {
		return RuntimeLoadReport{}, fmt.Errorf("runtime capacity excludes new bindings")
	}
	return report, nil
}

var runtimeLoadHTTPClient = &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxIdleConns: 32, MaxIdleConnsPerHost: 2, IdleConnTimeout: time.Minute}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func probeRuntimeLoad(ctx context.Context, client *http.Client, candidate enginekubernetes.BindingCandidate, policy RuntimePlacementPolicy) (RuntimeLoadReport, error) {
	if _, err := netip.ParseAddr(candidate.PodIP); err != nil {
		return RuntimeLoadReport{}, fmt.Errorf("candidate IP is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, policy.ProbeTimeout)
	defer cancel()
	// A non-replayable empty body disables net/http's automatic retry of a GET
	// on a stale pooled connection. The wire body has zero bytes.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(candidate.PodIP, "8080")+"/metrics", io.NopCloser(strings.NewReader("")))
	if err != nil {
		return RuntimeLoadReport{}, err
	}
	request.GetBody = nil
	if client == nil {
		client = runtimeLoadHTTPClient
	}
	// Client redirects are forbidden even when tests inject a transport.
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := copyClient.Do(request)
	if err != nil {
		return RuntimeLoadReport{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return RuntimeLoadReport{}, fmt.Errorf("load HTTP status %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, policy.MaxResponseBytes+1))
	if err != nil {
		return RuntimeLoadReport{}, err
	}
	if int64(len(raw)) > policy.MaxResponseBytes {
		return RuntimeLoadReport{}, fmt.Errorf("load response exceeds size bound")
	}
	return ParseRuntimeLoadReport(string(raw), policy.MemoryCutoff)
}

type runtimePlacementRequiredError struct{}

func (runtimePlacementRequiredError) Error() string { return "runtime placement sample is required" }

type runtimePlacementContextKey struct{}
type runtimePlacementChoice struct {
	WorkspaceID, SessionID, ProcessID string
	Candidate                         enginekubernetes.BindingCandidate
	Report                            RuntimeLoadReport
	Rounds, Probes                    int
}

// sampleRuntimePlacement runs only after the owning transaction has rolled
// back its need-for-placement result. Every probe precedes Session arbitration.
func (r KubernetesRuntimeTargetResolver) sampleRuntimePlacement(ctx context.Context, client *dbconnect.Client, job RuntimeJob) (runtimePlacementChoice, error) {
	started := time.Now()
	outcome := "unavailable"
	defer func() { r.PlacementMetrics.observeAttempt(outcome, time.Since(started)) }()
	policy := r.PlacementPolicy
	if policy == (RuntimePlacementPolicy{}) {
		policy = DefaultRuntimePlacementPolicy()
	}
	if err := policy.Validate(); err != nil {
		return runtimePlacementChoice{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, policy.ProbeBudget)
	defer cancel()
	snapshot := r.BindingVisibilitySnapshot()
	if !snapshot.Ready {
		return runtimePlacementChoice{}, runtimecontrol.PreparationError{Kind: "runtime_visibility_not_ready", Message: "Runtime visibility is not ready", Retryable: true}
	}
	var candidates []enginekubernetes.BindingCandidate
	seen := make(map[string]bool)
	for _, candidate := range snapshot.Candidates {
		key := candidate.Namespace + "/" + candidate.PodUID
		if candidate.PodUID == "" || seen[key] {
			continue
		}
		seen[key] = true
		candidates = append(candidates, candidate)
	}
	// Fisher-Yates yields a uniform distinct sample without probing a Pod twice.
	for i := len(candidates) - 1; i > 0; i-- {
		j, err := r.placementRandomIndex(i + 1)
		if err != nil {
			return runtimePlacementChoice{}, err
		}
		if r.RandomIndex != nil {
			if j < 0 || j > i {
				return runtimePlacementChoice{}, fmt.Errorf("placement random index is invalid")
			}
		}
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}
	probes := 0
	completedRounds := 0
	for round := 1; round <= policy.Rounds && len(candidates) > 0; round++ {
		completedRounds = round
		size := 2
		if len(candidates) < size {
			size = len(candidates)
		}
		sample := candidates[:size]
		candidates = candidates[size:]
		type probeResult struct {
			choice runtimePlacementChoice
			err    error
		}
		results := make(chan probeResult, size)
		var joined sync.WaitGroup
		joined.Add(size)
		for _, candidate := range sample {
			probes++
			go func(candidate enginekubernetes.BindingCandidate) {
				defer joined.Done()
				started := time.Now()
				outcome := "registry_unavailable"
				defer func() { r.PlacementMetrics.observeProbe(outcome, time.Since(started)) }()
				probeCtx, cancelProbe := context.WithTimeout(ctx, policy.ProbeTimeout)
				defer cancelProbe()
				var processID string
				err := client.QueryRow(probeCtx, "runtime_placement.read_process", `SELECT runtime_process_id FROM runtime_processes WHERE namespace=$1 AND pod_uid=$2 AND is_current AND phase='accepting' AND retired_at IS NULL`, candidate.Namespace, candidate.PodUID).Scan(&processID)
				if err != nil {
					results <- probeResult{err: err}
					return
				}
				report, err := probeRuntimeLoad(probeCtx, r.LoadClient, candidate, policy)
				outcome = "load_unavailable"
				if err == nil {
					outcome = "eligible"
				}
				results <- probeResult{choice: runtimePlacementChoice{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, ProcessID: processID, Candidate: candidate, Report: report}, err: err}
			}(candidate)
		}
		var valid []runtimePlacementChoice
		for range size {
			result := <-results
			if result.err == nil {
				valid = append(valid, result.choice)
			}
		}
		joined.Wait()
		if len(valid) > 0 {
			chosen := valid[0]
			tie := 0
			if len(valid) == 2 && valid[1].Report.ActiveSessions == chosen.Report.ActiveSessions {
				var err error
				tie, err = r.placementRandomIndex(2)
				if err != nil {
					return runtimePlacementChoice{}, err
				}
				if r.RandomIndex != nil {
					if tie < 0 || tie > 1 {
						return runtimePlacementChoice{}, fmt.Errorf("placement random index is invalid")
					}
				}
			}
			if len(valid) == 2 && (valid[1].Report.ActiveSessions < chosen.Report.ActiveSessions || (valid[1].Report.ActiveSessions == chosen.Report.ActiveSessions && tie == 1)) {
				chosen = valid[1]
			}
			outcome = "selected"
			chosen.Rounds = round
			chosen.Probes = probes
			return chosen, nil
		}
		if ctx.Err() != nil {
			break
		}
	}
	return runtimePlacementChoice{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, Rounds: completedRounds, Probes: probes}, runtimecontrol.PreparationError{Kind: "runtime_placement_unavailable", Message: "bounded Runtime load sampling found no eligible candidate", Retryable: true}
}

func (r KubernetesRuntimeTargetResolver) placementRandomIndex(bound int) (int, error) {
	if r.RandomIndex != nil {
		return r.RandomIndex(bound), nil
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(bound)))
	if err != nil {
		return 0, fmt.Errorf("placement randomness: %w", err)
	}
	return int(value.Int64()), nil
}
