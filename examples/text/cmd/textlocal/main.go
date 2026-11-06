// Command textlocal runs the example Network text.Pipeline in-process,
// without NATS. Every line read from stdin is sent as one Packet; every
// Packet leaving a Network Outport is printed to stdout.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/555nase89/goFBP/examples/text"
	"github.com/555nase89/goFBP/pkg/fbp"
)

func main() {
	raw := flag.Bool("raw", false, "send each line as raw JSON instead of a JSON string")
	wait := flag.Duration("wait", 2*time.Second, "how long to wait for outstanding results after EOF")
	flag.Parse()
	if err := run(*raw, *wait); err != nil {
		slog.Error("textlocal", "error", err)
		os.Exit(1)
	}
}

func run(raw bool, wait time.Duration) error {
	net, err := text.Pipeline()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// text.Pipeline sends exactly one Packet per input to one of its Outports.
	var pending sync.WaitGroup
	inst, err := net.Start(ctx, fbp.WithOutput(func(_ context.Context, outport string, p fbp.Packet) {
		fmt.Printf("%-7s %s  (run %s)\n", outport, p.Data, p.RunID)
		pending.Done()
	}))
	if err != nil {
		return err
	}
	defer inst.Stop()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var err error
		if raw {
			// Send copies the data, so the buffer of Bytes may be reused by Scan.
			data := json.RawMessage(scanner.Bytes())
			if !json.Valid(data) {
				fmt.Fprintln(os.Stderr, "skipped, not valid JSON:", scanner.Text())
				continue
			}
			pending.Add(1)
			_, err = inst.Send(ctx, fbp.Packet{Data: data})
		} else {
			pending.Add(1)
			_, err = inst.SendAs(ctx, scanner.Text())
		}
		if err != nil {
			if errors.Is(err, fbp.ErrStopped) {
				return nil
			}
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	done := make(chan struct{})
	go func() { pending.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-time.After(wait):
		return errors.New("timeout waiting for results")
	case <-ctx.Done():
		return nil
	}
}
