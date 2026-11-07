package natscon

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// ca is a test certificate authority that writes its certificates as PEM
// files into dir.
type ca struct {
	t    *testing.T
	dir  string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string // PEM file of the CA certificate
}

func newCA(t *testing.T, name string) *ca {
	t.Helper()
	c := &ca{t: t, dir: t.TempDir()}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	c.cert, c.key, c.file, _ = c.issue(name, tmpl)
	return c
}

// issue creates a certificate from tmpl, signed by c (self-signed while c has
// no certificate yet), and returns it with its key and the PEM files.
func (c *ca) issue(name string, tmpl *x509.Certificate) (*x509.Certificate, *ecdsa.PrivateKey, string, string) {
	c.t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		c.t.Fatal(err)
	}
	parent, signer := tmpl, key
	if c.cert != nil {
		parent, signer = c.cert, c.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signer)
	if err != nil {
		c.t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		c.t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		c.t.Fatal(err)
	}
	certFile := c.write(name+".pem", "CERTIFICATE", der)
	keyFile := c.write(name+"-key.pem", "EC PRIVATE KEY", keyDER)
	return cert, key, certFile, keyFile
}

func (c *ca) write(name, typ string, der []byte) string {
	c.t.Helper()
	path := filepath.Join(c.dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		c.t.Fatal(err)
	}
	return path
}

// leaf issues a server or client certificate and returns its PEM files.
func (c *ca) leaf(name string, usage x509.ExtKeyUsage) (certFile, keyFile string) {
	c.t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	_, _, certFile, keyFile = c.issue(name, tmpl)
	return certFile, keyFile
}

// tlsServer starts a NATS server that requires TLS and a client certificate
// signed by c, and returns its URL.
func tlsServer(t *testing.T, c *ca) string {
	t.Helper()
	certFile, keyFile := c.leaf("server", x509.ExtKeyUsageServerAuth)
	tc, err := server.GenTLSConfig(&server.TLSConfigOpts{CertFile: certFile, KeyFile: keyFile, CaFile: c.file, Verify: true})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		TLS: true, TLSVerify: true, TLSConfig: tc, TLSTimeout: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(2 * time.Second) {
		t.Fatal("nats server not ready")
	}
	return srv.ClientURL()
}

func TestConnectTLS(t *testing.T) {
	c := newCA(t, "test-ca")
	url := tlsServer(t, c)
	certFile, keyFile := c.leaf("client", x509.ExtKeyUsageClientAuth)

	nc, err := Connect(Config{Url: url, TLS: TLSConfig{CAFile: c.file, CertFile: certFile, KeyFile: keyFile}}, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if state, err := nc.TLSConnectionState(); err != nil || state.Version < tls.VersionTLS12 {
		t.Fatalf("TLS state %v, %v", state.Version, err)
	}
}

func TestConnectTLSRejected(t *testing.T) {
	c := newCA(t, "test-ca")
	url := tlsServer(t, c)
	certFile, keyFile := c.leaf("client", x509.ExtKeyUsageClientAuth)
	other := newCA(t, "other-ca")

	for name, cfg := range map[string]TLSConfig{
		"no client certificate": {CAFile: c.file},
		"unknown server CA":     {CAFile: other.file, CertFile: certFile, KeyFile: keyFile},
	} {
		t.Run(name, func(t *testing.T) {
			nc, err := Connect(Config{Url: url, TLS: cfg}, "test")
			if err == nil {
				nc.Close()
				t.Fatal("connected")
			}
		})
	}
}

func TestTLSConfigIncomplete(t *testing.T) {
	for _, cfg := range []TLSConfig{{CertFile: "client.pem"}, {KeyFile: "client-key.pem"}} {
		if _, err := cfg.options(); err == nil || !strings.Contains(err.Error(), "together") {
			t.Fatalf("%+v: got %v", cfg, err)
		}
	}
	if opts, err := (TLSConfig{}).options(); opts != nil || err != nil {
		t.Fatalf("empty config: got %v, %v", opts, err)
	}
}
