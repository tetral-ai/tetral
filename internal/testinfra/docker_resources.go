package testinfra

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DockerResources owns one local fixture's containers and network. Register
// Close immediately after construction, including when later setup fails.
// Resources carry the same PID/start-time ownership as runner dependencies.
type DockerResources struct {
	mu         sync.Mutex
	runID      string
	kind       string
	containers []string
	networks   []string
	closed     bool
}

type DockerMount struct {
	Source   string
	Target   string
	ReadOnly bool
}

type ContainerSpec struct {
	// NetworkContainer shares an already running fixture container's network
	// namespace, as a sidecar does in a Pod. Publish ports on that container.
	// It cannot be combined with Network, Aliases, or Ports.
	NetworkContainer *DockerContainer
	Image            string
	Network          string
	Aliases          []string
	Env              map[string]string
	Mounts           []DockerMount
	Ports            []int
	Command          []string
	Entrypoint       string
	User             string
}

// DockerContainer contains only public fixture identity, never environment or
// command arguments. ImageID identifies the actual local image that ran.
type DockerContainer struct {
	Name      string
	ImageID   string
	owner     *DockerResources
	ownedName string
}

func NewDockerResources(ctx context.Context, kind string) (*DockerResources, error) {
	if kind == "" || strings.Trim(kind, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
		return nil, errors.New("invalid Docker fixture kind")
	}
	if err := dockerAvailable(ctx); err != nil {
		return nil, err
	}
	if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
		return nil, err
	}
	if err := cleanupOrphanedDependencyNetworks(ctx); err != nil {
		return nil, err
	}
	runID, err := randomIdentity(16)
	if err != nil {
		return nil, err
	}
	return &DockerResources{runID: runID, kind: kind}, nil
}

func (r *DockerResources) Network(ctx context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return "", errors.New("docker fixture is closed")
	}
	name, err := dependencyContainerName(r.kind)
	if err != nil {
		return "", err
	}
	// Record before dispatch: cancellation can lose a successful create response.
	r.networks = append(r.networks, name)
	args := append([]string{"network", "create"}, dependencyContainerLabels(r.kind, r.runID)...)
	if err := runQuiet(ctx, "docker", append(args, name)...); err != nil {
		return "", fmt.Errorf("create Docker fixture network: %w", err)
	}
	return name, nil
}

func (r *DockerResources) Run(ctx context.Context, spec ContainerSpec) (*DockerContainer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("docker fixture is closed")
	}
	if spec.Image == "" || strings.HasPrefix(spec.Image, "-") {
		return nil, errors.New("invalid Docker fixture image")
	}
	args := []string{"run", "-d"}
	name, err := dependencyContainerName(r.kind)
	if err != nil {
		return nil, err
	}
	args = append(args, "--name", name)
	args = append(args, dependencyContainerLabels(r.kind, r.runID)...)
	if peer := spec.NetworkContainer; peer != nil {
		if spec.Network != "" || len(spec.Aliases) != 0 || len(spec.Ports) != 0 {
			return nil, errors.New("shared Docker network namespace cannot declare a network, aliases, or ports")
		}
		if peer.owner != r || peer.Name != peer.ownedName || !contains(r.containers, peer.Name) {
			return nil, errors.New("docker network namespace container is not owned by this fixture")
		}
		args = append(args, "--network", "container:"+peer.Name)
	}
	if spec.Network != "" {
		if !contains(r.networks, spec.Network) {
			return nil, errors.New("docker fixture network is not owned by this fixture")
		}
		args = append(args, "--network", spec.Network)
	}
	for _, alias := range spec.Aliases {
		args = append(args, "--network-alias", alias)
	}
	for _, port := range spec.Ports {
		if port < 1 || port > 65535 {
			return nil, errors.New("invalid Docker fixture port")
		}
		args = append(args, "--publish", "127.0.0.1::"+strconv.Itoa(port))
	}
	for _, mount := range spec.Mounts {
		if !filepath.IsAbs(mount.Source) || !filepath.IsAbs(mount.Target) || strings.ContainsAny(mount.Source+mount.Target, ",\n\r") {
			return nil, errors.New("docker fixture mounts require absolute paths without commas")
		}
		value := "type=bind,src=" + mount.Source + ",dst=" + mount.Target
		if mount.ReadOnly {
			value += ",readonly"
		}
		args = append(args, "--mount", value)
	}
	keys := make([]string, 0, len(spec.Env))
	for key := range spec.Env {
		if key == "" || strings.ContainsAny(key, "=\x00") {
			return nil, errors.New("invalid Docker fixture environment key")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--env", key+"="+spec.Env[key])
	}
	if spec.Entrypoint != "" {
		args = append(args, "--entrypoint", spec.Entrypoint)
	}
	if spec.User != "" {
		args = append(args, "--user", spec.User)
	}
	args = append(args, spec.Image)
	args = append(args, spec.Command...)
	r.containers = append(r.containers, name)
	if err := runQuiet(ctx, "docker", args...); err != nil {
		return nil, fmt.Errorf("start Docker fixture container: %w", err)
	}
	identity, err := dockerOutput(ctx, "inspect", "--format", "{{.Image}}", name)
	if err != nil {
		return nil, err
	}
	return &DockerContainer{Name: name, ImageID: identity, owner: r, ownedName: name}, nil
}

func (c *DockerContainer) Port(ctx context.Context, port int) (string, error) {
	return dockerPort(ctx, c.Name, strconv.Itoa(port)+"/tcp")
}

func (c *DockerContainer) Address(ctx context.Context, port int) (string, error) {
	resolved, err := c.Port(ctx, port)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort("127.0.0.1", resolved), nil
}

// Exec returns bounded output for fixture assertions. Callers must not print
// output containing credential material. Errors never embed arguments/output.
func (c *DockerContainer) Exec(ctx context.Context, args ...string) (string, error) {
	return boundedDockerOutput(ctx, append([]string{"exec", c.Name}, args...)...)
}

func (c *DockerContainer) Logs(ctx context.Context) (string, error) {
	return boundedDockerOutput(ctx, "logs", "--tail", "200", c.Name)
}

func (c *DockerContainer) Signal(ctx context.Context, signal string) error {
	return runQuiet(ctx, "docker", "kill", "--signal", signal, c.Name)
}

func (c *DockerContainer) Wait(ctx context.Context) (int, error) {
	output, err := dockerOutput(ctx, "wait", c.Name)
	if err != nil {
		return 0, err
	}
	code, err := strconv.Atoi(output)
	if err != nil {
		return 0, errors.New("docker fixture returned an invalid exit status")
	}
	return code, nil
}

// Close joins removal of containers before their networks, independently of
// the canceled fixture operation. The caller supplies its cleanup deadline.
// Successfully removed resources are forgotten; a later Close may retry any
// cleanup that failed or exceeded that deadline.
func (r *DockerResources) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	var failures []error
	for _, resource := range []struct {
		kind  string
		names *[]string
	}{
		{"container", &r.containers}, {"network", &r.networks},
	} {
		var remaining []string
		for i := len(*resource.names) - 1; i >= 0; i-- {
			name := (*resource.names)[i]
			args := []string{resource.kind, "rm"}
			if resource.kind == "container" {
				args = append(args, "--force")
			}
			if err := runQuiet(ctx, "docker", append(args, name)...); err != nil {
				// A canceled start may have created nothing; list existence
				// successfully before deciding that removal is unnecessary.
				exists, inspectErr := dockerResourceExists(ctx, resource.kind, "name", "^"+name+"$")
				if inspectErr != nil || exists {
					failures = append(failures, fmt.Errorf("remove Docker fixture %s: %w", resource.kind, err))
					remaining = append(remaining, name)
				}
			}
		}
		*resource.names = remaining
	}
	return errors.Join(failures...)
}

func cleanupOrphanedDependencyNetworks(ctx context.Context) error {
	output, err := dockerOutput(ctx, "network", "ls", "-q", "--filter", "label=tetral.test.owner=testinfra")
	if err != nil {
		return err
	}
	for _, network := range strings.Fields(output) {
		identity, err := dockerOutput(ctx, "network", "inspect", "--format", `{{index .Labels "tetral.test.owner-pid"}} {{index .Labels "tetral.test.owner-start"}}`, network)
		if err != nil {
			if exists, checkErr := dockerResourceExists(ctx, "network", "id", network); checkErr == nil && !exists {
				continue
			}
			return err
		}
		fields := strings.Fields(identity)
		if len(fields) != 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || processIdentityAlive(pid, fields[1]) {
			continue
		}
		if err := runQuiet(ctx, "docker", "network", "rm", network); err != nil {
			if exists, checkErr := dockerResourceExists(ctx, "network", "id", network); checkErr == nil && !exists {
				continue
			}
			return fmt.Errorf("remove orphaned Docker fixture network: %w", err)
		}
	}
	return nil
}

// A snapshot can race another package's normal cleanup or orphan collection.
// Absence is success only after a successful daemon query; a dead daemon is
// never mistaken for a successfully cleaned resource.
func dockerResourceExists(ctx context.Context, kind, filter, value string) (bool, error) {
	args := []string{kind, "ls", "-q", "--filter", filter + "=" + value}
	if kind == "container" {
		args = append(args, "--all")
	}
	output, err := dockerOutput(ctx, args...)
	return output != "", err
}

type boundedDockerBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *boundedDockerBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	const limit = 1 << 20
	if len(b.data) < limit {
		remaining := min(len(p), limit-len(b.data))
		b.data = append(b.data, p[:remaining]...)
	}
	return len(p), nil
}

func boundedDockerOutput(ctx context.Context, args ...string) (string, error) {
	// Fixture arguments are passed directly to Docker without a shell.
	//nolint:gosec
	command := exec.CommandContext(ctx, "docker", args...)
	var output boundedDockerBuffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return string(output.data), fmt.Errorf("docker fixture command failed: %w", err)
	}
	return string(output.data), nil
}
