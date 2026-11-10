package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	joint "github.com/tetral-ai/tetral/integration/cli-engine"
)

// macHost is the Mac that TETRAL_MAC_SSH_DESTINATION names, reached as the
// harness reaches it.
type macHost struct{ destination, key string }

func (h macHost) sshArgs(extra ...string) []string {
	return append([]string{"-i", h.key, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes"}, extra...)
}

// run runs script in a login shell on the Mac and returns its output.
func (h macHost) run(ctx context.Context, script string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ssh", h.sshArgs(h.destination, "zsh -lc "+shellQuote(script))...).CombinedOutput() //nolint:gosec // the runner's own command on the Mac
	if err != nil {
		return string(out), fmt.Errorf("ssh %s %q: %w: %s", h.destination, script, err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

// sshAddress is the IP address and port ssh connects to for the Mac, its
// destination resolved as ssh resolves it (ssh -G).
func (h macHost) sshAddress(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "ssh", "-G", h.destination).Output() //nolint:gosec // the runner's own destination
	if err != nil {
		return "", fmt.Errorf("ssh -G %s: %w", h.destination, err)
	}
	var host, port string
	for line := range strings.Lines(string(out)) {
		key, value, _ := strings.Cut(strings.TrimSpace(line), " ")
		switch key {
		case "hostname":
			host = value
		case "port":
			port = value
		}
	}
	addresses, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return "", fmt.Errorf("resolve the Mac's host %q: %w", host, err)
	}
	return net.JoinHostPort(addresses[0], port), nil
}

// connectTime returns how long one TCP connect to address takes: one round
// trip.
func connectTime(ctx context.Context, address string) (time.Duration, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(start)
	_ = conn.Close()
	return elapsed, nil
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

// macLane is what the Mac lane put in place on the Mac.
type macLane struct {
	// root is the run's Mac directory (joint.MacRoot), holding the lane
	// directory and every machine directory and disk image of the run.
	root string
	// caffeinate is the PID of the caffeinate -i the lane holds.
	caffeinate string
	// dir is the lane directory holding the CLI binaries, which each
	// machine copies after checking their SHA-256.
	dir string
}

// startMacLane creates the run's Mac directory, holds caffeinate -i and
// copies the Mac binaries once into the lane directory.
func (r *runner) startMacLane(ctx context.Context) error {
	h := r.cfg.mac
	root, err := h.run(ctx, "mkdir -p ~/"+joint.MacRoot(r.runID)+" && cd ~/"+joint.MacRoot(r.runID)+" && pwd")
	if err != nil {
		return err
	}
	// The cleanup takes what names root, so root must name this run.
	if !strings.HasSuffix(root, "/"+joint.MacRoot(r.runID)) {
		return fmt.Errorf("the run's Mac directory is %q; want a path ending in %s", root, joint.MacRoot(r.runID))
	}
	lane := &macLane{root: root}
	r.mac = lane
	if lane.caffeinate, err = h.run(ctx, "nohup caffeinate -i > /dev/null 2>&1 < /dev/null & echo $!"); err != nil {
		return err
	}
	if lane.dir, err = h.run(ctx, "mktemp -d "+shellQuote(root)+"/lane.XXXXXXXX"); err != nil {
		return err
	}
	var copies []string
	for _, name := range []string{"TETRAL_CLI_BIN_MACOS", "TETRAL_MCP_TEST_SERVER_BIN_MACOS"} {
		if local := os.Getenv(name); local != "" {
			copies = append(copies, local)
		}
	}
	copied := time.Now()
	scp := exec.CommandContext(ctx, "scp", h.sshArgs(append(copies, h.destination+":"+lane.dir+"/")...)...) //nolint:gosec // the runner's own copy
	if out, err := scp.CombinedOutput(); err != nil {
		return fmt.Errorf("scp to the lane directory %s: %w: %s", lane.dir, err, out)
	}
	var names []string
	for _, local := range copies {
		names = append(names, shellQuote(path.Join(lane.dir, filepath.Base(local))))
	}
	if _, err := h.run(ctx, "chmod 0555 "+strings.Join(names, " ")); err != nil {
		return err
	}
	r.log("macos lane: caffeinate -i PID %s; %d binaries copied to %s in %.1f s", lane.caffeinate, len(copies), lane.dir, time.Since(copied).Seconds())
	return nil
}

// macDevices lists the attached disk images as "<device> <image path>".
func macDevices(ctx context.Context, h macHost) ([]string, error) {
	out, err := h.run(ctx, "hdiutil info")
	if err != nil {
		return nil, err
	}
	var devices []string
	image := ""
	scanner := bufio.NewScanner(strings.NewReader(out))
	disk := regexp.MustCompile(`^(/dev/disk[0-9]+)(\s|$)`)
	for scanner.Scan() {
		line := scanner.Text()
		if value, ok := strings.CutPrefix(line, "image-path"); ok {
			_, image, _ = strings.Cut(value, ":")
			image = strings.TrimSpace(image)
		}
		if m := disk.FindStringSubmatch(line); m != nil && image != "" {
			devices = append(devices, m[1]+" "+image)
			image = ""
		}
	}
	return devices, nil
}

// mountPoints are the mount points of mounts, the output of mount.
func mountPoints(mounts string) []string {
	mountPoint := regexp.MustCompile(`^.* on (.*) \([^()]*\)$`)
	var points []string
	for line := range strings.Lines(mounts) {
		if m := mountPoint.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			points = append(points, m[1])
		}
	}
	return points
}

// macRun is what is one run's on the Mac: what lies under root, and each
// RAM disk whose volume, mounted at volume followed by the disk's device
// name, the run erased.
type macRun struct{ root, volume string }

func newMacRun(root, runID string) macRun {
	return macRun{root: root, volume: "/Volumes/" + joint.RAMVolume(runID)}
}

// names reports whether s names a path under the run's root or volumes.
func (m macRun) names(s string) bool {
	return strings.Contains(s, m.root+"/") || strings.Contains(s, m.volume)
}

// processes are the lines of listing (macProcesses) of the processes whose
// command line names a path of the run: a test detaches its RAM disk at its
// cleanup, so the disk can be gone while a process of it still runs.
func (m macRun) processes(listing string) []string {
	var lines []string
	for line := range strings.Lines(listing) {
		line = strings.TrimSpace(line)
		pid, command, _ := strings.Cut(line, " ")
		if _, err := strconv.Atoi(pid); err != nil || strings.Contains(command, macProcesses) {
			continue
		}
		if m.names(command) {
			lines = append(lines, line)
		}
	}
	return lines
}

// mounts are the points that lie under the run's root or inside one of its
// volumes; a volume itself goes with its disk.
func (m macRun) mounts(points []string) []string {
	var under []string
	for _, point := range points {
		rest, onVolume := strings.CutPrefix(point, m.volume)
		if strings.HasPrefix(point, m.root+"/") || onVolume && strings.Contains(rest, "/") {
			under = append(under, point)
		}
	}
	return under
}

// devices sorts the attached disk images (macDevices): an image under the
// run's root, and a RAM disk whose run volume is mounted at one of points,
// are the run's to detach; any other RAM disk is left, since nothing shows
// it is the run's.
func (m macRun) devices(devices, points []string) (detach, left []string) {
	for _, d := range devices {
		device, image, _ := strings.Cut(d, " ")
		switch {
		case strings.HasPrefix(image, "ram://"):
			if slices.Contains(points, m.volume+path.Base(device)) {
				detach = append(detach, device)
			} else {
				left = append(left, device)
			}
		case strings.HasPrefix(image, m.root+"/"):
			detach = append(detach, device)
		}
	}
	return detach, left
}

// macProcesses lists the Mac's processes as "<pid> <command>" lines.
const macProcesses = "ps -A -o pid=,command="

// cleanupMac ends, by PID, the Mac processes whose command line names a
// path of the run (macRun), unmounts what is mounted under the run's root
// and volumes, detaches the run's disk images, removes the run's root and
// ends caffeinate.
func (r *runner) cleanupMac(ctx context.Context) {
	h, lane := r.cfg.mac, r.mac
	run := newMacRun(lane.root, r.runID)
	devices, err := macDevices(ctx, h)
	if err != nil {
		r.log("cleanup: %v", err)
		return
	}
	mounts, err := h.run(ctx, "mount")
	if err != nil {
		r.log("cleanup: %v", err)
		return
	}
	detach, left := run.devices(devices, mountPoints(mounts))
	for _, device := range left {
		r.log("cleanup: Mac: leave %s, a RAM disk with no %s volume", device, run.volume+"*")
	}
	listing, err := h.run(ctx, macProcesses)
	if err != nil {
		r.log("cleanup: %v", err)
	}
	var pids []string
	for _, line := range run.processes(listing) {
		pid, command, _ := strings.Cut(line, " ")
		r.log("cleanup: Mac: end process %s %.120s", pid, command)
		pids = append(pids, pid)
	}
	if len(pids) > 0 {
		if out, err := h.run(ctx, "kill "+strings.Join(pids, " ")+"; sleep 1"); err != nil {
			r.log("cleanup: Mac: kill: %v %s", err, out)
		}
	}
	if mounts, err = h.run(ctx, "mount"); err != nil {
		r.log("cleanup: %v", err)
	}
	for _, point := range run.mounts(mountPoints(mounts)) {
		r.log("cleanup: Mac: umount -f %s", point)
		if out, err := h.run(ctx, "umount -f "+shellQuote(point)); err != nil {
			r.log("cleanup: Mac: %v %s", err, out)
		}
	}
	for _, device := range detach {
		r.log("cleanup: Mac: hdiutil detach -force %s", device)
		if out, err := h.run(ctx, "hdiutil detach -force "+shellQuote(device)); err != nil {
			r.log("cleanup: Mac: %v %s", err, out)
		}
	}
	r.log("cleanup: Mac: remove %s", lane.root)
	if out, err := h.run(ctx, "rm -rf "+shellQuote(lane.root)); err != nil {
		r.log("cleanup: Mac: %v %s", err, out)
	}
	if lane.caffeinate != "" {
		if out, err := h.run(ctx, "kill "+lane.caffeinate); err != nil {
			r.log("cleanup: Mac: end caffeinate: %v %s", err, out)
		} else {
			r.log("cleanup: Mac: caffeinate %s ended", lane.caffeinate)
		}
	}
}
