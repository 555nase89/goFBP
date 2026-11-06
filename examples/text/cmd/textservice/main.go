// Command textservice runs the example Network text.Pipeline as a Service.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/555nase89/goFBP/examples/text"
	"github.com/555nase89/goFBP/pkg/natscon"
	"github.com/555nase89/goFBP/pkg/service"
)

type config struct {
	NatsCon natscon.Config `json:"natscon"`
}

func main() {
	path := flag.String("config", "config.json", "path to the config file")
	flag.Parse()
	if err := run(*path); err != nil {
		slog.Error("textservice", "error", err)
		os.Exit(1)
	}
}

func run(path string) error {
	var cfg config
	if err := natscon.ReadFile(path, &cfg); err != nil {
		return err
	}
	net, err := text.Pipeline()
	if err != nil {
		return err
	}
	nc, err := natscon.Connect(cfg.NatsCon, "textservice")
	if err != nil {
		return err
	}
	defer nc.Drain()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	s, err := service.Start(ctx, nc, text.PackagePath, net, service.WithVersion("0.0.1"))
	if err != nil {
		return err
	}
	// From here on main and the Bricks also log via NATS (Log subject).
	slog.SetDefault(s.Logger())
	slog.Info("textservice started", "inport", service.InportSubject(s.Base()))
	<-ctx.Done()
	return s.Stop()
}
