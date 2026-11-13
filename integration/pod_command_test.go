package integration

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/release"
	"github.com/tetral-ai/tetral/internal/testinfra"
)

// servingProcess is a started command the test waits ready, holds and then
// ends.
type servingProcess interface {
	ready(t testing.TB, url string)
	runs(t testing.TB, hold time.Duration)
	terminate(t testing.TB, limit time.Duration)
	written() string
}

// podProcess is a command running as its production build in a container
// laid out as its pod.
type podProcess struct {
	name      string
	container *testinfra.DockerContainer
}

// The paths at which the pod's manifest mounts what the command reads.
const (
	podServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	podStoreTrustDir     = "/var/run/tetral/store-trust"
	podBinary            = "/usr/local/bin/"
)

// startPodCommand builds pkg as the image builds it and runs it in the
// image's base on the host network, as name with env. What its pod adds
// is added alone: the service-account directory, a token and the CA of a
// stand-in Kubernetes API that answers namespace's calls, the two variables
// that point at it, and the store CA at its mounted path in place of env's.
func startPodCommand(t *testing.T, name, pkg, namespace string, env map[string]string) *podProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), filepath.Base(pkg))
	build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", binary, pkg) //nolint:gosec // G204: a repository-owned command and a private output path.
	build.Dir, build.Env = "..", append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v: %s", pkg, err, output)
	}
	const token = "service-account-token"
	api := standInKubernetesAPI(t, namespace, token)
	host, port, err := net.SplitHostPort(api.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	account, trust := commandDataDir(t, "account", 0o755), commandDataDir(t, "trust", 0o755)
	podFile(t, account, "token", []byte(token))
	podFile(t, account, "namespace", []byte(namespace))
	podFile(t, account, "ca.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw}))
	store, err := os.ReadFile(env["TETRAL_DATABASE_TLS_CA_PATH"])
	if err != nil {
		t.Fatal(err)
	}
	podFile(t, trust, "database-ca.crt", store)
	podFile(t, trust, "object-store-ca.crt", store)
	pod := map[string]string{"KUBERNETES_SERVICE_HOST": host, "KUBERNETES_SERVICE_PORT": port,
		"TETRAL_DATABASE_TLS_CA_PATH": podStoreTrustDir + "/database-ca.crt", "TETRAL_BLOB_TLS_CA_PATH": podStoreTrustDir + "/object-store-ca.crt"}
	for key, value := range env {
		if _, replaced := pod[key]; !replaced {
			pod[key] = value
		}
	}
	image := ""
	inventory, err := release.LoadBaseInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range inventory.Entries {
		if entry.Dockerfile == "Dockerfile" && entry.Stage == "final" {
			image = entry.Reference
		}
	}
	if image == "" {
		t.Fatal("the base inventory names no final image of the Engine Dockerfile")
	}
	resources, err := testinfra.NewDockerResources(ctx, "pod-command")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := resources.Close(ctx); err != nil {
			t.Errorf("%s container cleanup: %v", name, err)
		}
	})
	container, err := resources.Run(ctx, testinfra.ContainerSpec{HostNetwork: true, Image: image, Env: pod, Entrypoint: podBinary + filepath.Base(pkg),
		Mounts: []testinfra.DockerMount{{Source: binary, Target: podBinary + filepath.Base(pkg), ReadOnly: true},
			{Source: account, Target: podServiceAccountDir, ReadOnly: true}, {Source: trust, Target: podStoreTrustDir, ReadOnly: true}}})
	if err != nil {
		t.Fatal(err)
	}
	return &podProcess{name: name, container: container}
}

func podFile(t *testing.T, dir, name string, content []byte) {
	t.Helper()
	//nolint:gosec // G306, G703: a test's own directory and fixed names; a projected volume's default mode, so the container's user reads it.
	if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func (p *podProcess) ready(t testing.TB, url string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if response, err := client.Get(url); err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if state, err := p.container.State(t.Context()); err != nil || !state.Running {
			t.Fatalf("%s exited at its start (%+v, %v): %s", p.name, state, err, p.written())
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not ready within 30 seconds: %s", p.name, p.written())
		}
	}
}

// runs waits hold; the container must still be running then.
func (p *podProcess) runs(t testing.TB, hold time.Duration) {
	t.Helper()
	time.Sleep(hold)
	if state, err := p.container.State(t.Context()); err != nil || !state.Running {
		t.Fatalf("%s exited on its own within %s of being ready (%+v, %v): %s", p.name, hold, state, err, p.written())
	}
}

// terminate sends SIGTERM to the process in the container; it must exit 0
// within limit.
func (p *podProcess) terminate(t testing.TB, limit time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	if err := p.container.Signal(ctx, "TERM"); err != nil {
		t.Errorf("%s had exited on its own before its SIGTERM: %v: %s", p.name, err, p.written())
		return
	}
	status, err := p.container.Wait(ctx)
	switch {
	case ctx.Err() != nil:
		t.Errorf("%s did not exit within %s of SIGTERM", p.name, limit)
	case err != nil:
		t.Errorf("wait for %s: %v", p.name, err)
	case status != 0:
		t.Errorf("%s after SIGTERM: exit status %d: %s", p.name, status, p.written())
	}
}

func (p *podProcess) written() string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	logs, err := p.container.Logs(ctx)
	if err != nil {
		return "(no logs: " + err.Error() + ")"
	}
	return logs
}

// standInKubernetesAPI answers, over TLS and to token alone, the calls a
// Runtime-visibility cache makes at its start and while idle: a list of
// namespace's pods and EndpointSlices, then a watch of each that stays open
// with no event. Any other call fails the test.
func standInKubernetesAPI(t *testing.T, namespace, token string) *httptest.Server {
	t.Helper()
	lists := map[string]string{
		"/api/v1/namespaces/" + namespace + "/pods":                             `{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"1"},"items":[]}`,
		"/apis/discovery.k8s.io/v1/namespaces/" + namespace + "/endpointslices": `{"kind":"EndpointSliceList","apiVersion":"discovery.k8s.io/v1","metadata":{"resourceVersion":"1"},"items":[]}`,
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		list, known := lists[r.URL.Path]
		if r.Method != http.MethodGet || !known || r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("the stand-in Kubernetes API was called with %s %s", r.Method, r.URL.RequestURI())
			http.Error(w, "not a call of the command", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") != "true" {
			_, _ = w.Write([]byte(list))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	server.StartTLS()
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
	})
	return server
}
