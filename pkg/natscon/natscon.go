// Package natscon reads the NATS part of a config file and connects to NATS.
package natscon

import (
	"encoding/json"
	"log/slog"
	"os"

	"github.com/nats-io/nats.go"
)

// Config is the NATS connection part of a config file.
type Config struct {
	Url string `json:"url"`
	// NKeyFile is the path to an NKey seed file; empty means no authentication.
	NKeyFile string `json:"nkeyfile"`
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
	return nats.Connect(cfg.Url, opts...)
}
