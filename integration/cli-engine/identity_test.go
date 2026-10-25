package joint

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tetral-ai/tetral/internal/kubernetes/kubernetest"
)

// identity stands in for the Kubernetes API's TokenReview, which internal
// gRPC authentication asks to authenticate a caller's service account token.
// Every service process gets its endpoint through the Kubernetes API
// variables (environment), and every workload gets a token of its own
// (issue) that the endpoint authenticates as that workload's service account
// in the tetral-system namespace.
type identity struct {
	server            *httptest.Server
	caPath            string
	reviewerTokenPath string
	reviewerToken     string
	dir               string

	mu       sync.Mutex
	accounts map[string]string
}

const (
	identityNamespace = "tetral-system"
	internalAudience  = "tetral-internal-grpc"
)

func newIdentity(t *testing.T) *identity {
	t.Helper()
	decode, err := kubernetest.NewTokenReviewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	i := &identity{dir: t.TempDir(), reviewerToken: randomHex(t, 32), accounts: map[string]string{}}
	i.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/apis/authentication.k8s.io/v1/tokenreviews" || r.Header.Get("Authorization") != "Bearer "+i.reviewerToken {
			http.Error(w, "not a token review of the reviewer", http.StatusForbidden)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "unreadable review", http.StatusBadRequest)
			return
		}
		review, err := decode(raw)
		if err != nil {
			http.Error(w, "invalid review", http.StatusBadRequest)
			return
		}
		i.mu.Lock()
		account, known := i.accounts[review.Token]
		i.mu.Unlock()
		audienced := len(review.Audiences) == 1 && review.Audiences[0] == internalAudience
		status := map[string]any{"authenticated": known && audienced}
		if known && audienced {
			status["audiences"] = []string{internalAudience}
			status["user"] = map[string]any{
				"username": "system:serviceaccount:" + identityNamespace + ":" + account,
				"extra":    map[string]any{"authentication.kubernetes.io/pod-uid": []string{"pod-" + account}},
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview", "status": status})
	}))
	i.server.StartTLS()
	t.Cleanup(i.server.Close)
	i.caPath = filepath.Join(i.dir, "ca.crt")
	i.reviewerTokenPath = filepath.Join(i.dir, "reviewer-token")
	certificate, err := x509.ParseCertificate(i.server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(i.caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(i.reviewerTokenPath, []byte(i.reviewerToken), 0o600); err != nil {
		t.Fatal(err)
	}
	return i
}

// environment is the Kubernetes API variables every service process gets.
func (i *identity) environment() map[string]string {
	return map[string]string{
		"KUBERNETES_API_SERVER_URL":                   i.server.URL,
		"KUBERNETES_API_CA_CERT_PATH":                 i.caPath,
		"KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH": i.reviewerTokenPath,
	}
}

// issue writes a new token of the service account into a file of its own
// and returns the file's path, which a workload's own token-path setting
// names.
func (i *identity) issue(t *testing.T, account string) string {
	t.Helper()
	token := randomHex(t, 32)
	path := filepath.Join(i.dir, account+"-token")
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.accounts[token] = account
	return path
}
