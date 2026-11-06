// Command crashlocal runs the example Network crash.Stats in-process and
// crashes on the first Packet leaving the Error Outport: it prints the
// ErrorData and exits with status 1. With -panic it panics instead, so the
// Go runtime prints a stack trace and exits with status 2.
//
// Every argument is sent as one Packet (a JSON array of integers). Without
// arguments it sends [1,2,3], [] and [4,-1]; the empty array hits the bug in
// the Average Brick.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/555nase89/goFBP/examples/crash"
	"github.com/555nase89/goFBP/pkg/fbp"
)

type output struct {
	outport string
	packet  fbp.Packet
}

func main() {
	doPanic := flag.Bool("panic", false, "panic on an Error Packet instead of exiting with status 1")
	flag.Parse()
	inputs := flag.Args()
	if len(inputs) == 0 {
		inputs = []string{"[1,2,3]", "[]", "[4,-1]"}
	}

	net, err := crash.Stats()
	if err != nil {
		slog.Error("crashlocal", "error", err)
		os.Exit(1)
	}

	// The output function is called from the goroutine of the sending Brick,
	// inside the runtime's recover. A panic there would only turn into
	// another Error Packet, so the results are handed to main instead.
	results := make(chan output, 1)
	inst, err := net.Start(context.Background(), fbp.WithOutput(func(_ context.Context, outport string, p fbp.Packet) {
		results <- output{outport, p}
	}))
	if err != nil {
		slog.Error("crashlocal", "error", err)
		os.Exit(1)
	}

	// Inputs are sent one at a time, so the crash happens at a known input.
	for _, in := range inputs {
		if _, err := inst.Send(context.Background(), fbp.Packet{Data: json.RawMessage(in)}); err != nil {
			slog.Error("crashlocal", "input", in, "error", err)
			os.Exit(1)
		}
		var o output
		select {
		case o = <-results:
		case <-time.After(2 * time.Second):
			slog.Error("crashlocal", "input", in, "error", "timeout waiting for result")
			os.Exit(1)
		}
		if o.outport != fbp.Error {
			fmt.Printf("%-7s %s -> %s\n", o.outport, in, o.packet.Data)
			continue
		}

		inst.Stop()
		ed, _ := fbp.DataAs[fbp.ErrorData](o.packet)
		if *doPanic {
			panic(fmt.Sprintf("network %s failed in node %s (brick %s) on input %s: %s",
				net.Name, ed.Node, ed.Brick, in, ed.Message))
		}
		slog.Error("network failed", "network", net.Name, "node", ed.Node, "brick", ed.Brick,
			"input", in, "message", ed.Message, "run", o.packet.RunID)
		os.Exit(1)
	}
	inst.Stop()
}
