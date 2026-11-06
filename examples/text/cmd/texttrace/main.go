// Command texttrace runs the example Network text.Pipeline in-process and
// prints every Event of every Run, so the path of each Packet through the
// Network becomes visible. Every line read from stdin is sent as one Packet.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/555nase89/goFBP/examples/text"
	"github.com/555nase89/goFBP/pkg/fbp"
)

func main() {
	if err := run(); err != nil {
		slog.Error("texttrace", "error", err)
		os.Exit(1)
	}
}

func run() error {
	net, err := text.Pipeline()
	if err != nil {
		return err
	}

	// text.Pipeline sends exactly one Packet per input to one of its Outports.
	var pending sync.WaitGroup
	inst, err := net.Start(context.Background(),
		fbp.WithObserver(observe),
		fbp.WithOutput(func(context.Context, string, fbp.Packet) { pending.Done() }),
	)
	if err != nil {
		return err
	}
	defer inst.Stop()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		pending.Add(1)
		if _, err := inst.SendAs(context.Background(), scanner.Text()); err != nil {
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
		// The last Processed Event of a Run comes after its Output.
		time.Sleep(10 * time.Millisecond)
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("timeout waiting for results")
	}
}

// mu keeps the lines of Events from different goroutines apart.
var mu sync.Mutex

// observe prints one line per Event, e.g.
//
//	14:32:07.120031  run=3f9c1a2e  #2 <#1  Received  pipeline / shout / upper . Inport  "hello"
func observe(e fbp.Event) {
	where := e.Network
	if e.Node != "" {
		where += " / " + strings.ReplaceAll(e.Node, "/", " / ")
	}
	if e.Port != "" {
		where += " . " + e.Port
	}
	id := fmt.Sprintf("#%d", e.PacketID)
	if e.ParentID != 0 {
		id += fmt.Sprintf(" <#%d", e.ParentID)
	}
	line := fmt.Sprintf("%s  run=%.8s  %-7s  %-9s %-37s",
		e.Time.Format("15:04:05.000000"), e.RunID, id, e.Kind, where)
	switch {
	case e.Err != nil:
		line += " error: " + e.Err.Error()
	case e.Data != nil:
		line += " " + string(e.Data)
	}

	mu.Lock()
	defer mu.Unlock()
	fmt.Println(strings.TrimRight(line, " "))
}
