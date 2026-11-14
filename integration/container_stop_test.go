package integration

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// stopContainer stops the fixture container name, giving its process grace
// seconds, and keeps each of its loopback host ports bound until the start it
// returns has started the container again. A stopped container's host ports
// are free, and a listener on 127.0.0.1:0 elsewhere in the run can receive
// one, after which docker start fails with "address already in use". Each
// port is held by a socket bound with SO_REUSEADDR that never listens: a
// client is refused as by the stopped container, no other listener receives
// the port, and docker binds it beside the socket.
func stopContainer(t *testing.T, name string, grace int) (start func()) {
	t.Helper()
	ports := strings.Fields(dockerCommand(t, grace, "port", name))
	dockerCommand(t, grace, "stop", "--time", strconv.Itoa(grace), name)
	var held []int
	release := func() {
		for _, fd := range held {
			_ = syscall.Close(fd)
		}
		held = nil
	}
	t.Cleanup(release)
	for _, published := range ports {
		host, port, err := net.SplitHostPort(published)
		if err != nil {
			continue
		}
		number, err := strconv.Atoi(port)
		if err != nil || host != "127.0.0.1" {
			t.Fatalf("container %s publishes %s; want a 127.0.0.1 port", name, published)
		}
		fd, err := holdLoopbackPort(number)
		if err != nil {
			t.Fatalf("hold host port %d of stopped container %s: %v", number, name, err)
		}
		held = append(held, fd)
	}
	return func() {
		t.Helper()
		dockerCommand(t, grace, "start", name)
		release()
	}
}

func holdLoopbackPort(port int) (int, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err == nil {
		err = syscall.Bind(fd, &syscall.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}})
	}
	if err != nil {
		_ = syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

func dockerCommand(t *testing.T, grace int, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(grace+10)*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput() //nolint:gosec // Fixed operations on run-owned fixture containers.
	if err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}
