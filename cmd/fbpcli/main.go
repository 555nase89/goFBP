// Command fbpcli sends one input to an Inport (-inport, default In) of a
// Network that runs as a Service and prints every Packet of the Run as
// "<Outport> <data>", one per line. It exits with status 1 when a Packet left
// through the Error Outport.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"
	"github.com/555nase89/goFBP/pkg/natscon"
	"github.com/555nase89/goFBP/pkg/service/cli"
)

type config struct {
	NatsCon natscon.Config `json:"natscon"`
}

func main() {
	path := flag.String("config", "config.json", "path to the config file")
	network := flag.String("network", "", "Network as <packagepath>/<networkname>, e.g. gofbp/examples/text/pipeline")
	inport := flag.String("inport", fbp.In, "Network Inport to send the input to")
	input := flag.String("input", "", "input as JSON; empty or - reads stdin")
	asString := flag.Bool("string", false, "send the input as a JSON string")
	timeout := flag.Duration("timeout", 10*time.Second, "how long to wait for the end of the Run")
	first := flag.Bool("first", false, "stop after the first Packet of the Run")
	flag.Parse()
	outports, err := run(*path, *network, *inport, *input, *asString, *first, *timeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fbpcli:", err)
		os.Exit(1)
	}
	if slices.Contains(outports, fbp.Error) {
		os.Exit(1)
	}
}

func run(path, network, inport, input string, asString, first bool, timeout time.Duration) ([]string, error) {
	if network == "" {
		return nil, errors.New("-network is required")
	}
	base, err := cli.ParseNetwork(network)
	if err != nil {
		return nil, err
	}
	data, err := cli.ReadInput(input, os.Stdin, asString)
	if err != nil {
		return nil, err
	}
	var cfg config
	if err := natscon.ReadFile(path, &cfg); err != nil {
		return nil, err
	}
	nc, err := natscon.Connect(cfg.NatsCon, "fbpcli")
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return cli.Run(ctx, nc, base, inport, data, os.Stdout, first)
}
