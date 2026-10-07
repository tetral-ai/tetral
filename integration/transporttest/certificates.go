// Package transporttest contains local certificate and proxy fixtures. It does
// not claim to exercise Kubernetes issuance, interception or policy application.
package transporttest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

type Authority struct {
	Certificate *x509.Certificate
	Key         *ecdsa.PrivateKey
	PEM         []byte
}
type Leaf struct {
	Certificate, Key []byte
	Parsed           *x509.Certificate
}

func serial() (*big.Int, error) { return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120)) }
func NewAuthority(name string) (*Authority, error) {
	return NewAuthorityWithValidity(name, time.Now().Add(-30*time.Second), time.Now().Add(time.Hour))
}

// NewAuthorityWithValidity builds a root with an explicit validity window, so
// expired and not-yet-valid anchors need no host clock change.
func NewAuthorityWithValidity(name string, from, until time.Time) (*Authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	id, err := serial()
	if err != nil {
		return nil, err
	}
	cert := &x509.Certificate{SerialNumber: id, Subject: pkix.Name{CommonName: name}, NotBefore: from, NotAfter: until, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Authority{Certificate: parsed, Key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}
func (a *Authority) Issue(dns, uri string, from, until time.Time) (Leaf, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Leaf{}, err
	}
	id, err := serial()
	if err != nil {
		return Leaf{}, err
	}
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return Leaf{}, err
	}
	cert := &x509.Certificate{SerialNumber: id, Subject: pkix.Name{CommonName: dns}, DNSNames: []string{dns}, URIs: []*url.URL{parsedURI}, NotBefore: from, NotAfter: until, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, a.Certificate, &key.PublicKey, a.Key)
	if err != nil {
		return Leaf{}, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Leaf{}, err
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		return Leaf{}, err
	}
	return Leaf{Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), Key: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), Parsed: parsed}, nil
}
func (a *Authority) ValidLeaf(dns, uri string) (Leaf, error) {
	return a.Issue(dns, uri, time.Now().Add(-30*time.Second), time.Now().Add(10*time.Minute))
}

// Project replaces a complete generation using the same parent/symlink shape
// as a projected volume. The old directory remains readable by existing users.
func Project(parent, name string, files map[string][]byte) error {
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	generation := filepath.Join(parent, name)
	if err := os.Mkdir(generation, 0700); err != nil {
		return err
	}
	for file, data := range files {
		if err := os.WriteFile(filepath.Join(generation, file), data, 0600); err != nil {
			return err
		}
		link := filepath.Join(parent, file)
		if _, err := os.Lstat(link); os.IsNotExist(err) {
			if err := os.Symlink(filepath.Join("..data", file), link); err != nil {
				return err
			}
		}
	}
	temporary := filepath.Join(parent, "..data-next")
	_ = os.Remove(temporary)
	if err := os.Symlink(name, temporary); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(parent, "..data"))
}
