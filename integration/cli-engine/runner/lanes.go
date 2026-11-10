package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"
)

const (
	// Each job takes its loopback ports from a range of its own, below the
	// range the kernel hands out for port 0 on both hosts (Linux from
	// 32768, macOS from 49152): firstPort + portsPerJob × (its place in its
	// lane + macPortOffset on the Mac lane).
	firstPort, portsPerJob, macPortOffset = 21000, 48, 100
	// maxJobsPerLane keeps the lanes' port ranges apart.
	maxJobsPerLane = macPortOffset
	// jobGrace is how much longer than its budget a job may run before go
	// test's own timeout ends it.
	jobGrace = 300 * time.Second
	// smallVolumeTest is the test that needs TETRAL_SMALL_VOLUME on Linux.
	smallVolumeTest = "TestCacheEvictionFreesSpaceAndKeepsPendingChanges"
)

// jobResult is how one job's test process ran.
type jobResult struct {
	job        job
	start, end time.Time
	// exit is the process's exit code, -1 when it could not start.
	exit int
	// pid and pidStart identify the test process, which labels the Docker
	// resources it creates with them.
	pid      int
	pidStart string
	log      string
}

// lane runs the lane's jobs, longest first, on its slots, and records its
// wall time from the run's start.
func (r *runner) lane(ctx context.Context, machineOS string) {
	jobs := r.jobs[machineOS]
	if len(jobs) == 0 {
		r.log("%s lane: no jobs", machineOS)
		return
	}
	if machineOS == "macos" {
		if err := r.startMacLane(ctx); err != nil {
			r.log("macos lane: %v", err)
			return
		}
	}
	if err := os.MkdirAll(filepath.Join(r.cfg.out, machineOS), 0o700); err != nil {
		r.log("%v", err)
		return
	}
	env := r.laneEnv(machineOS)
	queue := make(chan int, len(jobs))
	for i := range jobs {
		queue <- i
	}
	close(queue)
	slots := r.cfg.slots[machineOS]
	r.log("%s lane: %d jobs on %d slots", machineOS, len(jobs), slots)
	var wg sync.WaitGroup
	for range slots {
		wg.Go(func() {
			for i := range queue {
				if ctx.Err() != nil {
					return
				}
				result := r.runJob(ctx, machineOS, env, i, jobs[i])
				r.mu.Lock()
				r.results[machineOS] = append(r.results[machineOS], result)
				r.mu.Unlock()
			}
		})
	}
	wg.Wait()
	r.mu.Lock()
	r.walls[machineOS] = time.Since(r.started)
	r.mu.Unlock()
	r.log("%s lane ended: wall %.1f s", machineOS, time.Since(r.started).Seconds())
}

// runJob runs job, the index-th of its lane, as one test process.
func (r *runner) runJob(ctx context.Context, machineOS string, env []string, index int, j job) jobResult {
	first := firstPort + portsPerJob*index
	if machineOS == "macos" {
		first += portsPerJob * macPortOffset
	}
	args := []string{"-test.v", "-test.count=1", fmt.Sprintf("-test.timeout=%s", time.Duration(j.budget)*time.Second+jobGrace), "-test.run", j.pattern()}
	command := exec.CommandContext(ctx, r.binary, args...) //nolint:gosec // the run's own harness
	if machineOS == "linux" && j.rows[0].test() == smallVolumeTest && os.Getenv("TETRAL_SMALL_VOLUME") == "" {
		// The stand-in for a small volume: a 7 GiB tmpfs in a user and mount
		// namespace of the job's own, where the CLI runs as the namespace's
		// root.
		volume := filepath.Join(r.cfg.out, "small-volume")
		if err := os.Mkdir(volume, 0o700); err != nil {
			return jobResult{job: j, exit: -1, log: err.Error()}
		}
		command = exec.CommandContext(ctx, "unshare", append([]string{"--user", "--map-root-user", "--mount", "sh", "-c",
			`mount -t tmpfs -o size=7g tmpfs "$1" && TETRAL_SMALL_VOLUME="$1" && export TETRAL_SMALL_VOLUME && shift && exec "$@"`,
			"sh", volume, r.binary}, args...)...)
	}
	// The lane's slots share env; a clipped env makes append copy it.
	command.Env = append(slices.Clip(env), fmt.Sprintf("TETRAL_JOINT_PORTS=%d-%d", first, first+portsPerJob-1))
	command.Cancel = func() error { return command.Process.Signal(syscall.SIGTERM) }
	logPath := filepath.Join(r.cfg.out, machineOS, j.name+".log")
	result := jobResult{job: j, log: logPath, exit: -1}
	logFile, err := os.Create(logPath)
	if err != nil {
		return result
	}
	defer logFile.Close()
	command.Stdout, command.Stderr = logFile, logFile
	result.start = time.Now()
	if err := command.Start(); err != nil {
		fmt.Fprintf(logFile, "runner: start: %v\n", err)
		result.end = time.Now()
		return result
	}
	result.pid = command.Process.Pid
	result.pidStart = processStart(result.pid)
	_ = command.Wait()
	result.end = time.Now()
	result.exit = command.ProcessState.ExitCode()
	r.log("%s %s: exit %d after %.1f s of %d budget-seconds", machineOS, j.name, result.exit, result.end.Sub(result.start).Seconds(), j.budget)
	return result
}
