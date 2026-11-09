package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	// minBwrap is the oldest bubblewrap the Linux lane runs with: older
	// versions race Docker's containerd mounts (containers/bubblewrap#650).
	minBwrap = "0.13.0"
	// maxMacLoad is the highest 1-minute load average at which the Mac lane
	// starts; a busier Mac pushes disk-bound rows over their budgets.
	maxMacLoad = 4.0
	// macLoadRecheck and macLoadWait are how often and how long the runner
	// checks again before it gives up.
	macLoadRecheck, macLoadWait = 30 * time.Second, 5 * time.Minute
	// networksPerEngine are the Docker networks one Engine's containers use.
	networksPerEngine = 2
	// macRoundTripSamples is how many connects to the Mac's SSH port the
	// runner times; the shortest is the round trip it checks.
	macRoundTripSamples = 5
)

// preflight is what the host checks found, for the report.
type preflight struct {
	bwrapPath, bwrapVersion string
	macRoundTrip            time.Duration
	macLoad                 float64
	// networksCreated of networksWanted probe networks were created.
	networksCreated, networksWanted int
}

// checkHosts checks the hosts the lanes run on before anything is built.
func (r *runner) checkHosts(ctx context.Context) error {
	if slices.Contains(r.cfg.lanes, "linux") {
		path, version, err := bwrapVersion(ctx)
		r.bwrapPath, r.bwrapVersion = path, version
		if err != nil {
			return err
		}
		if !atLeast(version, minBwrap) {
			return fmt.Errorf("the first bwrap in PATH, %s, is version %s; want %s or later", path, version, minBwrap)
		}
		r.log("preflight: bwrap %s is version %s", path, version)
	}
	if slices.Contains(r.cfg.lanes, "macos") {
		address, err := r.cfg.mac.sshAddress(ctx)
		if err != nil {
			return err
		}
		if err := r.checkMacRoundTrip(ctx, func(ctx context.Context) (time.Duration, error) { return connectTime(ctx, address) }); err != nil {
			return err
		}
		load, err := r.waitMacLoad(ctx)
		r.macLoad = load
		if err != nil {
			return err
		}
		r.log("preflight: the Mac's 1-minute load average is %.2f", load)
	}
	return nil
}

func bwrapVersion(ctx context.Context) (path, version string, err error) {
	path, err = exec.LookPath("bwrap")
	if err != nil {
		return "", "", fmt.Errorf("no bwrap in PATH: %w", err)
	}
	out, err := exec.CommandContext(ctx, path, "--version").Output()
	if err != nil {
		return path, "", fmt.Errorf("%s --version: %w", path, err)
	}
	// "bubblewrap 0.13.0"
	fields := strings.Fields(string(out))
	if len(fields) != 2 || fields[0] != "bubblewrap" {
		return path, "", fmt.Errorf("%s --version printed %q", path, out)
	}
	return path, fields[1], nil
}

// atLeast is whether the dotted version is want or later.
func atLeast(version, want string) bool {
	parse := func(v string) []int {
		var parts []int
		for field := range strings.SplitSeq(v, ".") {
			n, err := strconv.Atoi(field)
			if err != nil {
				return nil
			}
			parts = append(parts, n)
		}
		return parts
	}
	got := parse(version)
	return got != nil && slices.Compare(got, parse(want)) >= 0
}

// checkMacRoundTrip fails unless the shortest of macRoundTripSamples round
// trips to the Mac, each timed by connect, is at most -max-mac-rtt. A Mac
// off its local network, on a phone's hotspot for example, fails and slows
// its rows for reasons that are not in the code. Queueing and Wi-Fi power
// saving only add delay, in bursts that can last several connects, so the
// shortest connect is the path's own round trip.
func (r *runner) checkMacRoundTrip(ctx context.Context, connect func(context.Context) (time.Duration, error)) error {
	for i := range macRoundTripSamples {
		d, err := connect(ctx)
		if err != nil {
			return fmt.Errorf("time a connect to the Mac's SSH port: %w", err)
		}
		if i == 0 || d < r.macRoundTrip {
			r.macRoundTrip = d
		}
	}
	shortest := r.macRoundTrip.Round(100 * time.Microsecond)
	if r.macRoundTrip > r.cfg.maxMacRoundTrip {
		return fmt.Errorf("the shortest of %d round trips to the Mac is %s, above %s (-max-mac-rtt): the Mac is not on a local path, and Mac-lane numbers would be invalid", macRoundTripSamples, shortest, r.cfg.maxMacRoundTrip)
	}
	r.log("preflight: the shortest of %d round trips to the Mac is %s", macRoundTripSamples, shortest)
	return nil
}

// waitMacLoad returns the Mac's 1-minute load average once it is at most
// maxMacLoad, checking every macLoadRecheck for up to macLoadWait.
func (r *runner) waitMacLoad(ctx context.Context) (float64, error) {
	deadline := time.Now().Add(macLoadWait)
	for {
		load, err := r.macLoadAverage(ctx)
		if err != nil {
			return 0, err
		}
		if load <= maxMacLoad {
			return load, nil
		}
		if time.Now().After(deadline) {
			busiest, _ := r.cfg.mac.run(ctx, "ps -A -r -o pid,pcpu,comm | head -n 6")
			return load, fmt.Errorf("the Mac's 1-minute load average is %.2f, above %.1f, after %s; its busiest processes:\n%s", load, maxMacLoad, macLoadWait, busiest)
		}
		r.log("preflight: the Mac's 1-minute load average is %.2f, above %.1f; checking again in %s", load, maxMacLoad, macLoadRecheck)
		select {
		case <-ctx.Done():
			return load, ctx.Err()
		case <-time.After(macLoadRecheck):
		}
	}
}

// macLoadAverage reads the Mac's 1-minute load average.
func (r *runner) macLoadAverage(ctx context.Context) (float64, error) {
	// "{ 2.34 2.10 1.98 }"
	out, err := r.cfg.mac.run(ctx, "sysctl -n vm.loadavg")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(strings.Trim(out, "{} \n"))
	if len(fields) != 3 {
		return 0, fmt.Errorf("sysctl -n vm.loadavg printed %q", out)
	}
	return strconv.ParseFloat(fields[0], 64)
}

// checkNetworks creates Docker networks one at a time, as many as the
// lanes' Engines need at once, removes them, and lowers the Mac lane's
// slots by one when the host has room for one Engine fewer.
func (r *runner) checkNetworks(ctx context.Context) error {
	engines := 0
	for _, machineOS := range r.cfg.lanes {
		engines += min(r.cfg.slots[machineOS], len(r.jobs[machineOS]))
	}
	r.networksWanted = engines * networksPerEngine
	created, err := probeNetworks(ctx, r.networksWanted)
	r.networksCreated = created
	switch {
	case created >= r.networksWanted:
		r.log("preflight: created and removed %d Docker networks: %d Engines at once", created, engines)
	case min(r.cfg.slots["macos"], len(r.jobs["macos"])) > 1 && created >= r.networksWanted-networksPerEngine:
		r.cfg.slots["macos"] = min(r.cfg.slots["macos"], len(r.jobs["macos"])) - 1
		r.log("preflight: created and removed %d of %d Docker networks (%v): %d Engines at once, %d Mac slots", created, r.networksWanted, err, engines-1, r.cfg.slots["macos"])
	default:
		return fmt.Errorf("created %d of the %d Docker networks that %d Engines need: %v", created, r.networksWanted, engines, err)
	}
	return nil
}

// probeNetworks creates up to n Docker networks one at a time, then removes
// those it created by ID. They carry testinfra's owner labels with the
// runner's process, so that testinfra removes them if the runner dies
// before it does.
func probeNetworks(ctx context.Context, n int) (int, error) {
	pid := os.Getpid()
	labels := []string{"--label", "tetral.test.owner=testinfra", "--label", "tetral.test.kind=joint-preflight", "--label", "tetral.test.owner-pid=" + strconv.Itoa(pid), "--label", "tetral.test.owner-start=" + processStart(pid)}
	var ids []string
	var createErr error
	for i := range n {
		out, err := exec.CommandContext(ctx, "docker", append(append([]string{"network", "create"}, labels...), fmt.Sprintf("tetral-joint-probe-%d-%d", pid, i))...).CombinedOutput()
		if err != nil {
			createErr = fmt.Errorf("docker network create: %w: %s", err, strings.TrimSpace(string(out)))
			break
		}
		ids = append(ids, strings.TrimSpace(string(out)))
	}
	if len(ids) > 0 {
		if out, err := exec.Command("docker", append([]string{"network", "rm"}, ids...)...).CombinedOutput(); err != nil {
			return 0, fmt.Errorf("remove the probe networks %s: %w: %s", strings.Join(ids, " "), err, out)
		}
	}
	return len(ids), createErr
}

// processStart is the start time of process pid in clock ticks since boot,
// field 22 of /proc/<pid>/stat, as testinfra labels its resources.
func processStart(pid int) string {
	fields := procStatFields(pid)
	if len(fields) <= 19 {
		return ""
	}
	return fields[19]
}

// procStatFields are the fields of /proc/<pid>/stat after the command name:
// the state first, then the parent's PID.
func procStatFields(pid int) []string {
	body, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return nil
	}
	closeParen := strings.LastIndexByte(string(body), ')')
	if closeParen < 0 {
		return nil
	}
	return strings.Fields(string(body[closeParen+1:]))
}
