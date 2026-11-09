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
)

// macMachines is the Mac directory under which the harness creates each
// machine's directory and disk image, and the runner the lane directory.
const macMachines = "~/tetral-test-machines"

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
	// entries and devices are the entries of macMachines and the attached
	// disk images before the lane started; the cleanup removes only newer
	// ones.
	entries, devices []string
	// processes are the ps lines of the processes that ran from a RAM disk
	// volume before the lane started; the cleanup spares them.
	processes []string
	// caffeinate is the PID of the caffeinate -i the lane holds.
	caffeinate string
	// dir is the lane directory holding the CLI binaries, which each
	// machine copies after checking their SHA-256.
	dir string
}

// startMacLane notes what the Mac holds, holds caffeinate -i and copies
// the Mac binaries once into the lane directory.
func (r *runner) startMacLane(ctx context.Context) error {
	h := r.cfg.mac
	lane := &macLane{}
	var err error
	if lane.entries, err = macEntries(ctx, h); err != nil {
		return err
	}
	if lane.devices, err = macDevices(ctx, h); err != nil {
		return err
	}
	listing, err := h.run(ctx, macProcesses)
	if err != nil {
		return err
	}
	lane.processes = leftovers(listing, nil, nil)
	// The cleanup spares only what this baseline lists, so a lane without
	// one gets no cleanup.
	r.mac = lane
	if lane.caffeinate, err = h.run(ctx, "nohup caffeinate -i > /dev/null 2>&1 < /dev/null & echo $!"); err != nil {
		return err
	}
	if lane.dir, err = h.run(ctx, "mktemp -d "+macMachines+"/lane.XXXXXXXX"); err != nil {
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

// macEntries lists the entries of macMachines, creating it if needed.
func macEntries(ctx context.Context, h macHost) ([]string, error) {
	out, err := h.run(ctx, "mkdir -p "+macMachines+" && cd "+macMachines+" && pwd && ls -A")
	if err != nil {
		return nil, err
	}
	lines := strings.Split(out, "\n")
	var entries []string
	for _, name := range lines[1:] {
		if name != "" {
			entries = append(entries, path.Join(lines[0], name))
		}
	}
	return entries, nil
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
	disk := regexp.MustCompile(`^(/dev/disk[0-9]+)\s`)
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

// ramVolume starts the mount point of each RAM disk the harness erases,
// /Volumes/tetral-joint-<device>.
const ramVolume = "/Volumes/tetral-joint-"

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

// runDevices sorts the disk images attached now (macDevices) that were not
// attached before: an image under one of the run's new entries, and a RAM
// disk whose tetral-joint volume, named after its device, is mounted, are
// the run's to detach, with that volume; any other RAM disk is left, since
// nothing shows it is the run's.
func runDevices(devices, before, newEntries, points []string) (detach, volumes, left []string) {
	for _, d := range devices {
		if slices.Contains(before, d) {
			continue
		}
		device, image, _ := strings.Cut(d, " ")
		switch {
		case strings.HasPrefix(image, "ram://"):
			if volume := ramVolume + path.Base(device); slices.Contains(points, volume) {
				detach, volumes = append(detach, device), append(volumes, volume)
			} else {
				left = append(left, device)
			}
		case slices.ContainsFunc(newEntries, func(e string) bool { return strings.HasPrefix(image, e+"/") }):
			detach = append(detach, device)
		}
	}
	return detach, volumes, left
}

// macProcesses lists the Mac's processes as "<pid> <command>" lines.
const macProcesses = "ps -A -o pid=,command="

// under reports whether s names a path under one of roots.
func under(roots []string, s string) bool {
	return slices.ContainsFunc(roots, func(root string) bool { return strings.Contains(s, root+"/") || strings.HasSuffix(s, root) })
}

// leftovers returns the lines of listing (macProcesses) of the processes
// whose command line names a path under one of roots, or a RAM disk volume
// unless the line is one of before: a test detaches its RAM disk at its
// cleanup, so the disk can be gone while a process of it still runs.
func leftovers(listing string, roots, before []string) []string {
	var lines []string
	for line := range strings.Lines(listing) {
		line = strings.TrimSpace(line)
		pid, command, _ := strings.Cut(line, " ")
		if _, err := strconv.Atoi(pid); err != nil || strings.Contains(command, macProcesses) {
			continue
		}
		if under(roots, command) || strings.Contains(command, ramVolume) && !slices.Contains(before, line) {
			lines = append(lines, line)
		}
	}
	return lines
}

// cleanupMac ends, by PID, the Mac processes whose command line names an
// entry of macMachines or a tetral-joint RAM disk volume the run created,
// unmounts what is mounted under them, detaches the disk images the run
// attached (runDevices), removes the run's entries and ends caffeinate.
func (r *runner) cleanupMac(ctx context.Context) {
	h, lane := r.cfg.mac, r.mac
	entries, err := macEntries(ctx, h)
	if err != nil {
		r.log("cleanup: %v", err)
		return
	}
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
	newEntries := slices.DeleteFunc(entries, func(e string) bool { return slices.Contains(lane.entries, e) })
	newDevices, volumes, left := runDevices(devices, lane.devices, newEntries, mountPoints(mounts))
	for _, device := range left {
		r.log("cleanup: Mac: leave %s, a RAM disk attached during the run with no %s volume", device, ramVolume+"*")
	}
	roots := append(slices.Clone(newEntries), volumes...)
	listing, err := h.run(ctx, macProcesses)
	if err != nil {
		r.log("cleanup: %v", err)
	}
	var pids []string
	for _, line := range leftovers(listing, roots, lane.processes) {
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
	for _, point := range mountPoints(mounts) {
		if under(roots, point) && !slices.Contains(roots, point) {
			r.log("cleanup: Mac: umount -f %s", point)
			if out, err := h.run(ctx, "umount -f "+shellQuote(point)); err != nil {
				r.log("cleanup: Mac: %v %s", err, out)
			}
		}
	}
	for _, device := range newDevices {
		r.log("cleanup: Mac: hdiutil detach -force %s", device)
		if out, err := h.run(ctx, "hdiutil detach -force "+shellQuote(device)); err != nil {
			r.log("cleanup: Mac: %v %s", err, out)
		}
	}
	for _, entry := range newEntries {
		r.log("cleanup: Mac: remove %s", entry)
		if out, err := h.run(ctx, "rm -rf "+shellQuote(entry)); err != nil {
			r.log("cleanup: Mac: %v %s", err, out)
		}
	}
	if lane.caffeinate != "" {
		if out, err := h.run(ctx, "kill "+lane.caffeinate); err != nil {
			r.log("cleanup: Mac: end caffeinate: %v %s", err, out)
		} else {
			r.log("cleanup: Mac: caffeinate %s ended", lane.caffeinate)
		}
	}
}
