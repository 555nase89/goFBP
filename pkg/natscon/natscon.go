// Package natscon reads the NATS part of a config file and connects to NATS.
package natscon

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"os"

	"github.com/nats-io/nats.go"
)

// Config is the NATS connection part of a config file.
type Config struct {
	Url string `json:"url"`
	// NKeyFile is the path to an NKey seed file; empty means no authentication.
	NKeyFile string `json:"nkeyfile"`
	// TLS configures the TLS connection to the server, see TLSConfig.
	TLS TLSConfig `json:"tls"`
}

// TLSConfig holds the files for a TLS connection to NATS. As soon as one of
// them is set, the connection must use TLS (at least TLS 1.2). With all of
// them empty, TLS is used only if the URL starts with tls:// or the server
// requires it, and the server is verified against the system roots.
type TLSConfig struct {
	// CAFile is a PEM file with the CA certificates that verify the server;
	// empty means the system roots.
	CAFile string `json:"cafile"`
	// CertFile and KeyFile are the PEM files of the client certificate for
	// mutual TLS. Both or neither must be set.
	CertFile string `json:"certfile"`
	KeyFile  string `json:"keyfile"`
}

// enabled reports whether any TLS file is set.
func (c TLSConfig) enabled() bool {
	return c.CAFile != "" || c.CertFile != "" || c.KeyFile != ""
}

// options returns the nats.Options for c; none if c is empty.
func (c TLSConfig) options() ([]nats.Option, error) {
	if !c.enabled() {
		return nil, nil
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return nil, errors.New("natscon: tls certfile and keyfile must be set together")
	}
	opts := []nats.Option{nats.Secure(&tls.Config{MinVersion: tls.VersionTLS12})}
	if c.CAFile != "" {
		opts = append(opts, nats.RootCAs(c.CAFile))
	}
	if c.CertFile != "" {
		opts = append(opts, nats.ClientCert(c.CertFile, c.KeyFile))
	}
	return opts, nil
}

// ReadFile decodes the JSON file at path into v.
func ReadFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// Connect connects to NATS and keeps reconnecting without limit. Async errors
// of the connection, e.g. a slow consumer, are logged.
func Connect(cfg Config, name string) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			var subject string
			if sub != nil {
				subject = sub.Subject
			}
			slog.Warn("nats: async error", "subject", subject, "error", err)
		}),
	}
	if cfg.NKeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.NKeyFile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, opt)
	}
	tlsOpts, err := cfg.TLS.options()
	if err != nil {
		return nil, err
	}
	opts = append(opts, tlsOpts...)
	return nats.Connect(cfg.Url, opts...)
}
