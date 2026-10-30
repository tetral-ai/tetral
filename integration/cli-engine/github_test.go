package joint

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto/ecdsa"
	"crypto/elliptic"

	"github.com/tetral-ai/tetral/integration/transporttest"
)

// A machine reaches GitHub only through git-proxy, which runs as its own
// process behind a TLS front in the test process: the front's origin is the
// git_proxy_url the hubs return, and machines trust its certificate through
// GIT_SSL_CAINFO. git-proxy, in turn, reaches github.com through a CONNECT
// proxy in the test process that its HTTPS_PROXY names; the proxy tunnels
// every github.com:443 connection to a GitHub stand-in, git http-backend over
// bare repositories behind a github.com certificate that git-proxy trusts
// through SSL_CERT_FILE. The stand-in answers only requests that carry the
// repository's token the way git-proxy injects it.

// github is the GitHub stand-in, its CONNECT proxy and the test CA that
// signs both its certificate and git-proxy's front.
type github struct {
	t         *testing.T
	authority *transporttest.Authority
	// caPath holds the test CA's certificate in PEM.
	caPath string
	// root holds the bare repositories as <owner>/<repo>.git.
	root string
	// proxyURL is the CONNECT proxy's origin, for HTTPS_PROXY.
	proxyURL string
	backend  string

	mu       sync.Mutex
	tokens   map[string]string
	requests []githubRequest
}

// githubRequest is one request the stand-in received: the repository it
// named, its path below the repository, and whether it carried the
// repository's token.
type githubRequest struct {
	repository, path string
	authorized       bool
}

// startGitHub starts the stand-in and its CONNECT proxy; both stop when t
// ends.
func startGitHub(t *testing.T) *github {
	t.Helper()
	authority, err := transporttest.NewAuthorityWithValidity("harness test CA", time.Now().Add(-time.Minute), time.Now().Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	g := &github{t: t, authority: authority, caPath: filepath.Join(dir, "ca.pem"), root: filepath.Join(dir, "repositories"), tokens: map[string]string{}}
	if err := os.WriteFile(g.caPath, authority.PEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(g.root, 0o700); err != nil {
		t.Fatal(err)
	}
	execPath, err := exec.Command("git", "--exec-path").Output()
	if err != nil {
		t.Fatalf("git --exec-path: %v", err)
	}
	backend := &cgi.Handler{
		Path: filepath.Join(strings.TrimSpace(string(execPath)), "git-http-backend"),
		Env:  []string{"GIT_PROJECT_ROOT=" + g.root, "GIT_HTTP_EXPORT_ALL=1"},
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { g.serve(w, r, backend) }))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{g.leaf([]string{"github.com"}, nil)}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	g.backend = server.Listener.Addr().String()

	proxy := httptest.NewServer(http.HandlerFunc(g.tunnel))
	t.Cleanup(proxy.Close)
	g.proxyURL = proxy.URL
	return g
}

// leaf is a server certificate of the test CA for dnsNames and ips.
func (g *github) leaf(dnsNames []string, ips []net.IP) tls.Certificate {
	g.t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		g.t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		g.t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "harness server"}, DNSNames: dnsNames, IPAddresses: ips,
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(3 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, g.authority.Certificate, &key.PublicKey, g.authority.Key)
	if err != nil {
		g.t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		g.t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		g.t.Fatal(err)
	}
	return certificate
}

// tunnel answers CONNECT github.com:443 with a tunnel to the stand-in and
// refuses every other request.
func (g *github) tunnel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect || r.Host != "github.com:443" {
		http.Error(w, "only CONNECT github.com:443", http.StatusForbidden)
		return
	}
	upstream, err := net.Dial("tcp", g.backend)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	client, buffered, err := http.NewResponseController(w).Hijack()
	if err != nil {
		_ = upstream.Close()
		return
	}
	if _, err := client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	go func() {
		_, _ = io.Copy(upstream, buffered)
		_ = upstream.Close()
	}()
	go func() {
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
	}()
}

// serve records the request and passes one that carries its repository's
// token to git http-backend at <owner>/<repo>.git; any other answers 401.
func (g *github) serve(w http.ResponseWriter, r *http.Request, backend http.Handler) {
	segments := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 3)
	if len(segments) < 3 {
		http.NotFound(w, r)
		return
	}
	repository := segments[0] + "/" + strings.TrimSuffix(segments[1], ".git")
	g.mu.Lock()
	token, known := g.tokens[repository]
	authorized := known && r.Header.Get("Authorization") == "Basic "+base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token))
	g.requests = append(g.requests, githubRequest{repository: repository, path: "/" + segments[2], authorized: authorized})
	g.mu.Unlock()
	if !authorized {
		w.Header().Set("WWW-Authenticate", `Basic realm="GitHub"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	r.URL.Path = "/" + repository + ".git/" + segments[2]
	backend.ServeHTTP(w, r)
}

// createRepository creates the bare repository owner/repo with one commit
// on main, which the stand-in serves to requests carrying token, and returns
// that commit.
func (g *github) createRepository(owner, repo, token string) string {
	g.t.Helper()
	bare := filepath.Join(g.root, owner, repo+".git")
	work := filepath.Join(g.t.TempDir(), repo)
	g.git("", "init", "--quiet", "--bare", "--initial-branch=main", bare)
	g.git("", "init", "--quiet", "--initial-branch=main", work)
	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("# "+repo+"\n"), 0o600); err != nil {
		g.t.Fatal(err)
	}
	g.git(work, "add", "README.md")
	g.git(work, "-c", "user.name=Harness", "-c", "user.email=harness@example.com", "commit", "--quiet", "-m", "Initial commit")
	g.git(work, "push", "--quiet", bare, "main")
	g.mu.Lock()
	g.tokens[owner+"/"+repo] = token
	g.mu.Unlock()
	return g.git(work, "rev-parse", "HEAD")
}

// git runs git in dir with a configuration of its own.
func (g *github) git(dir string, args ...string) string {
	g.t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "HOME="+g.root, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	out, err := command.Output()
	if err != nil {
		g.t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// requestsOf is every request the stand-in received for owner/repo.
func (g *github) requestsOf(repository string) []githubRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	var requests []githubRequest
	for _, request := range g.requests {
		if request.repository == repository {
			requests = append(requests, request)
		}
	}
	return requests
}

// startGitProxyFront serves listener over TLS with a 127.0.0.1 certificate
// of the test CA, passing every request on to git-proxy at address; it stops
// when t ends.
func (g *github) startGitProxyFront(listener net.Listener, address string) {
	g.t.Helper()
	origin := &url.URL{Scheme: "http", Host: address}
	proxy := httputil.NewSingleHostReverseProxy(origin)
	proxy.FlushInterval = -1
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: proxy, ReadHeaderTimeout: 10 * time.Second}}
	server.TLS = &tls.Config{Certificates: []tls.Certificate{g.leaf(nil, []net.IP{net.IPv4(127, 0, 0, 1)})}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	g.t.Cleanup(server.Close)
}
