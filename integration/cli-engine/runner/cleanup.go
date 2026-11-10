package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// cleanup removes, by PID, ID or the run's roots, what the run created and
// left: its processes, then the Docker containers and networks of its test
// processes, the tetral FUSE mounts and in-memory machine homes they left,
// and what the Mac lane created on the Mac.
func (r *runner) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	r.endDescendants()
	r.mu.Lock()
	var results []jobResult
	for _, lane := range r.results {
		results = append(results, lane...)
	}
	r.mu.Unlock()
	for _, result := range results {
		if result.pid == 0 {
			continue
		}
		filters := []string{"--filter", "label=tetral.test.owner=testinfra", "--filter", "label=tetral.test.owner-pid=" + strconv.Itoa(result.pid), "--filter", "label=tetral.test.owner-start=" + result.pidStart}
		for _, kind := range []struct{ list, remove []string }{
			{append([]string{"ps", "-aq"}, filters...), []string{"container", "rm", "--force", "--volumes"}},
			{append([]string{"network", "ls", "-q"}, filters...), []string{"network", "rm"}},
		} {
			out, err := exec.CommandContext(ctx, "docker", kind.list...).Output()
			if err != nil {
				r.log("cleanup: docker %s: %v", strings.Join(kind.list, " "), err)
				continue
			}
			if ids := strings.Fields(string(out)); len(ids) > 0 {
				r.log("cleanup: docker %s %s, left by %s", strings.Join(kind.remove, " "), strings.Join(ids, " "), result.job.name)
				if out, err := exec.CommandContext(ctx, "docker", append(kind.remove, ids...)...).CombinedOutput(); err != nil {
					r.log("cleanup: %v %s", err, out)
				}
			}
		}
	}
	mounts, _ := os.ReadFile("/proc/self/mounts")
	r.cleanupLocal(ctx, string(mounts))
	if r.mac != nil {
		r.cleanupMac(ctx)
	}
	for _, machineOS := range r.cfg.lanes {
		r.mu.Lock()
		results := r.results[machineOS]
		r.mu.Unlock()
		if len(results) < len(r.jobs[machineOS]) {
			r.log("cleanup: the %s lane ran %d of its %d jobs", machineOS, len(results), len(r.jobs[machineOS]))
		}
	}
}

// endDescendants ends, with SIGTERM and after 10 s SIGKILL, every process
// still running below the runner; as the child subreaper, the runner is an
// ancestor of every process the run started.
func (r *runner) endDescendants() {
	pids := descendants(os.Getpid())
	for _, pid := range pids {
		exe, _ := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
		r.log("cleanup: end process %d %s", pid, exe)
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(pids) > 0 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		pids = slices.DeleteFunc(pids, func(pid int) bool { return !running(pid) })
	}
	for _, pid := range pids {
		r.log("cleanup: process %d did not end on SIGTERM; SIGKILL", pid)
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// descendants are the running processes below pid; an ended child that
// waits to be reaped is not running.
func descendants(pid int) []int {
	entries, _ := os.ReadDir("/proc")
	children := map[int][]int{}
	for _, entry := range entries {
		child, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if fields := procStatFields(child); len(fields) > 1 {
			parent, _ := strconv.Atoi(fields[1])
			children[parent] = append(children[parent], child)
		}
	}
	var found []int
	queue := []int{pid}
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		for _, child := range children[next] {
			queue = append(queue, child)
			if running(child) {
				found = append(found, child)
			}
		}
	}
	return found
}

func running(pid int) bool {
	fields := procStatFields(pid)
	return len(fields) > 0 && fields[0] != "Z" && fields[0] != "X"
}

// cleanupLocal lazily unmounts the tetral FUSE mounts a killed test process
// left under the run's roots, as mounts (/proc/self/mounts) lists them,
// removes the run's in-memory machine homes and, once empty, its temporary
// root.
func (r *runner) cleanupLocal(ctx context.Context, mounts string) {
	for _, point := range runMounts(mounts, r.tmpRoot, r.memoryRoot) {
		r.log("cleanup: fusermount3 -u -z %s", point)
		if out, err := exec.CommandContext(ctx, "fusermount3", "-u", "-z", point).CombinedOutput(); err != nil {
			r.log("cleanup: %v %s", err, out)
		}
	}
	r.log("cleanup: remove %s", r.memoryRoot)
	if err := os.RemoveAll(r.memoryRoot); err != nil {
		r.log("cleanup: %v", err)
	}
	if err := os.Remove(r.tmpRoot); err != nil && !errors.Is(err, fs.ErrNotExist) {
		r.log("cleanup: leave %s: %v", r.tmpRoot, err)
	}
}

// runMounts are the mount points of the tetral FUSE mounts in mounts that
// lie under one of roots.
func runMounts(mounts string, roots ...string) []string {
	var points []string
	for line := range strings.Lines(mounts) {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[2] != "fuse.tetral" {
			continue
		}
		if slices.ContainsFunc(roots, func(root string) bool { return strings.HasPrefix(fields[1], root+"/") }) {
			points = append(points, fields[1])
		}
	}
	return points
}
