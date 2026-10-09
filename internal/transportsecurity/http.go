package transportsecurity

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"
)

// HTTPConfig owns the native public-backend transport. Metrics stay on their
// separate internal listener. Mesh-captured RPCs use their existing owner.
type HTTPConfig struct {
	Mode, CAPath, CertPath, KeyPath, EdgeClientURI string
}

func HTTPConfigFromEnv(getenv func(string) string) (HTTPConfig, error) {
	c := HTTPConfig{Mode: getenv("TETRAL_HTTP_TRANSPORT"), CAPath: getenv("TETRAL_HTTP_TLS_CA_PATH"), CertPath: getenv("TETRAL_HTTP_TLS_CERT_PATH"), KeyPath: getenv("TETRAL_HTTP_TLS_KEY_PATH"), EdgeClientURI: getenv("TETRAL_HTTP_TLS_EDGE_CLIENT_URI")}
	if c.Mode == "" {
		c.Mode = "plaintext"
	}
	if err := c.Validate(); err != nil {
		return HTTPConfig{}, err
	}
	return c, nil
}

func (c HTTPConfig) Validate() error {
	if c.Mode == "" || c.Mode == "plaintext" {
		if c.CAPath != "" || c.CertPath != "" || c.KeyPath != "" || c.EdgeClientURI != "" {
			return errors.New("TETRAL_HTTP_TRANSPORT plaintext cannot carry native TLS configuration")
		}
		return nil
	}
	if c.Mode != "native-mtls" {
		return errors.New("TETRAL_HTTP_TRANSPORT must be plaintext or native-mtls")
	}
	if c.CAPath == "" || c.CertPath == "" || c.KeyPath == "" {
		return errors.New("native HTTP requires TETRAL_HTTP_TLS_CA_PATH, CERT_PATH and KEY_PATH")
	}
	u, err := url.Parse(c.EdgeClientURI)
	if err != nil || u.Scheme != "spiffe" || u.Host == "" || u.Path == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return errors.New("TETRAL_HTTP_TLS_EDGE_CLIENT_URI must name the exact trusted edge SPIFFE identity")
	}
	return nil
}

// Open validates mounted credentials before admission and keeps fresh
// handshakes on the current generation. Close the returned owner after the
// HTTP listener and its admitted handlers have joined.
func (c HTTPConfig) Open(ctx context.Context) (*Owner, *tls.Config, error) {
	if err := c.Validate(); err != nil {
		return nil, nil, err
	}
	if c.Mode == "" || c.Mode == "plaintext" {
		return nil, nil, nil
	}
	owner, err := Open(ctx, Config{CAPath: c.CAPath, CertPath: c.CertPath, KeyPath: c.KeyPath, Purpose: "edge-http"})
	if err != nil {
		return nil, nil, err
	}
	config, err := owner.ServerTLSConfig(c.EdgeClientURI)
	if err != nil {
		_ = owner.Close()
		return nil, nil, err
	}
	config.NextProtos = []string{"h2", "http/1.1"}
	current := config.GetConfigForClient
	config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		next, err := current(hello)
		if err != nil {
			return nil, err
		}
		next.NextProtos = []string{"h2", "http/1.1"}
		return next, nil
	}
	return owner, config, nil
}
