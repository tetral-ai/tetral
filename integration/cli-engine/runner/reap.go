package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

// reapOrphans reaps, at each SIGCHLD until ctx ends, every zombie child of
// the runner whose PID namespace is not own, the runner's. As the child
// subreaper the runner inherits each process whose parent ended, such as
// the first process of a CLI sandbox whose outer process the CLI killed
// first, and reaps each one from another namespace as init would, so that
// it does not stay a zombie for the rest of the run. A zombie of its own
// namespace may be a child it waits for and is left to that Wait, so an
// orphan there stays a zombie.
func reapOrphans(ctx context.Context, own string) {
	exited := make(chan os.Signal, 1)
	signal.Notify(exited, syscall.SIGCHLD)
	defer signal.Stop(exited)
	for {
		reapForeign(own)
		select {
		case <-ctx.Done():
			return
		case <-exited:
		}
	}
}

func reapForeign(own string) {
	self := strconv.Itoa(os.Getpid())
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if fields := procStatFields(pid); len(fields) < 2 || fields[0] != "Z" || fields[1] != self {
			continue
		}
		if namespace, err := os.Readlink("/proc/" + entry.Name() + "/ns/pid"); err == nil && namespace != own {
			var status unix.WaitStatus
			_, _ = unix.Wait4(pid, &status, unix.WNOHANG, nil)
		}
	}
}
