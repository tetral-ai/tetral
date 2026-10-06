package integration

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tetral-ai/tetral/integration/transporttest"
)

// These two file SDS resources belong solely to the upstream process Bootstrap.
// Gateway policy secrets come exclusively from the official translator helper.
func writeEnvoyControlSDS(t *testing.T, directory string) *tls.Config {
	t.Helper()
	authority := transporttest.Must(transporttest.NewAuthority("local-upstream-control-plane"))
	server := transporttest.Must(authority.ValidLeaf("envoy-gateway", "spiffe://fixture/control-plane"))
	client := transporttest.Must(authority.ValidLeaf("local-envoy", "spiffe://fixture/control-client"))
	inline := func(body []byte) *corev3.DataSource {
		return &corev3.DataSource{Specifier: &corev3.DataSource_InlineBytes{InlineBytes: body}}
	}
	secrets := map[string]*tlsv3.Secret{
		"xds-certificate.json": {Name: "xds_certificate", Type: &tlsv3.Secret_TlsCertificate{TlsCertificate: &tlsv3.TlsCertificate{CertificateChain: inline(client.Certificate), PrivateKey: inline(client.Key)}}},
		"xds-trusted-ca.json":  {Name: "xds_trusted_ca", Type: &tlsv3.Secret_ValidationContext{ValidationContext: &tlsv3.CertificateValidationContext{TrustedCa: inline(authority.PEM)}}},
	}
	path := filepath.Join(directory, "sds")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	for name, secret := range secrets {
		wrapped := transporttest.Must(anypb.New(secret))
		body := transporttest.Must(protojson.Marshal(&discoveryv3.DiscoveryResponse{Resources: []*anypb.Any{wrapped}}))
		if err := os.WriteFile(filepath.Join(path, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(authority.PEM) {
		t.Fatal("invalid fixture control CA")
	}
	pair := transporttest.Must(tls.X509KeyPair(server.Certificate, server.Key))
	return &tls.Config{Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{"h2"}}
}
