package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// CA is a certificate authority that signs node certificates.
type CA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

// NewCA creates a self-signed CA valid for the given duration.
func NewCA(name string, valid time.Duration) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("security: CA key: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(valid),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("security: CA certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{cert: cert, key: key, der: der}, nil
}

// Issue creates a key and a certificate for a node, valid as server and as
// client, for the given host names and IP addresses. It returns both in PEM.
func (ca *CA) Issue(hosts []string, valid time.Duration) (certPEM, keyPEM []byte, err error) {
	if len(hosts) == 0 {
		return nil, nil, errors.New("security: a certificate needs at least one host name or IP")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("security: key: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: hosts[0]},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(valid),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, fmt.Errorf("security: certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pemBlock("CERTIFICATE", der), pemBlock("EC PRIVATE KEY", keyDER), nil
}

// CertPEM returns the CA certificate in PEM.
func (ca *CA) CertPEM() []byte { return pemBlock("CERTIFICATE", ca.der) }

// KeyPEM returns the CA key in PEM.
func (ca *CA) KeyPEM() []byte {
	der, _ := x509.MarshalECPrivateKey(ca.key) // cannot fail for a P-256 key
	return pemBlock("EC PRIVATE KEY", der)
}

func pemBlock(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127)) // crypto/rand does not fail
	return n
}

var fileName = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// WriteFiles creates a CA and one certificate per node in dir: ca.crt,
// ca.key and NODE.crt, NODE.key for every node, where a node is
// "HOST[,HOST...]" (the first host names the files). Keys are written with
// mode 0600. Existing files are not overwritten.
func WriteFiles(dir string, nodes [][]string, valid time.Duration) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("security: %w", err)
	}
	ca, err := NewCA("curntValv CA", valid)
	if err != nil {
		return nil, err
	}
	type file struct {
		name string
		data []byte
		mode os.FileMode
	}
	files := []file{{"ca.crt", ca.CertPEM(), 0o644}, {"ca.key", ca.KeyPEM(), 0o600}}
	for _, hosts := range nodes {
		c, k, err := ca.Issue(hosts, valid)
		if err != nil {
			return nil, err
		}
		base := fileName.ReplaceAllString(hosts[0], "_")
		files = append(files, file{base + ".crt", c, 0o644}, file{base + ".key", k, 0o600})
	}
	for _, f := range files {
		if _, err := os.Stat(filepath.Join(dir, f.name)); err == nil {
			return nil, fmt.Errorf("security: %s already exists; remove it or use another directory", filepath.Join(dir, f.name))
		}
	}
	var written []string
	for _, f := range files {
		p := filepath.Join(dir, f.name)
		if err := os.WriteFile(p, f.data, f.mode); err != nil {
			return written, fmt.Errorf("security: %w", err)
		}
		written = append(written, p)
	}
	return written, nil
}
