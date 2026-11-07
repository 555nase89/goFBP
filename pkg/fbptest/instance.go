package fbptest

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"
)

// Timeout is how long Instance.Run waits for a Run to finish before it fails
// the test.
var Timeout = 5 * time.Second

// Instance is an fbp.Instance started by Start. It records the Packets of the
// Runs started with Run or RunTo; Packets of other Runs are dropped.
type Instance struct {
	*fbp.Instance

	mu   sync.Mutex
	runs map[string]*run
}

type run struct {
	outputs map[string][]json.RawMessage
	done    chan int // receives the number of outputs when the Run is finished
}

// Start starts an Instance of n and stops it when the test ends. A failing
// Start fails the test. opts are applied before the options Start needs
// itself, so a WithOutput or WithRunFinished in opts has no effect.
func Start(t testing.TB, n *fbp.Network, opts ...fbp.Option) *Instance {
	t.Helper()
	i := &Instance{runs: map[string]*run{}}
	opts = append(opts, fbp.WithOutput(i.output), fbp.WithRunFinished(i.finished))
	inst, err := n.Start(context.Background(), opts...)
	if err != nil {
		t.Fatalf("start network %s: %v", n.Name, err)
	}
	t.Cleanup(inst.Stop)
	i.Instance = inst
	return i
}

// Run starts an Instance of n, sends data (JSON text) to its Inport In and
// returns the Packets of the Run once it is finished. Use Start and
// Instance.Run to send several Packets into one Instance.
func Run(t testing.TB, n *fbp.Network, data string, opts ...fbp.Option) Result {
	t.Helper()
	return Start(t, n, opts...).Run(t, data)
}

// Run sends data (JSON text) to the Inport In with a new Run ID, waits until
// the Run is finished and returns its Packets. It fails the test if the
// Packet cannot be sent or the Run does not finish within Timeout. Like
// t.Fatal, it must be called from the goroutine running the test.
func (i *Instance) Run(t testing.TB, data string) Result {
	t.Helper()
	return i.RunTo(t, fbp.In, data)
}

// RunTo is like Run but sends to the Network Inport inport.
func (i *Instance) RunTo(t testing.TB, inport, data string) Result {
	t.Helper()
	id := fbp.NewRunID()
	r := &run{outputs: map[string][]json.RawMessage{}, done: make(chan int, 1)}
	i.mu.Lock()
	i.runs[id] = r
	i.mu.Unlock()
	defer func() {
		i.mu.Lock()
		delete(i.runs, id)
		i.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(t.Context(), Timeout)
	defer cancel()
	if _, err := i.SendTo(ctx, inport, fbp.Packet{Data: json.RawMessage(data), RunID: id}); err != nil {
		t.Fatalf("send %q to inport %s: %v", data, inport, err)
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		i.mu.Lock()
		res := Result{RunID: id, Outputs: r.outputs}.String()
		i.mu.Unlock()
		t.Fatalf("run %s (input %q) not finished after %v; output so far: %s", id, data, Timeout, res)
	}
	// All outputs happened before the Run finished.
	return Result{RunID: id, Outputs: r.outputs}
}

func (i *Instance) output(_ context.Context, outport string, p fbp.Packet) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if r, ok := i.runs[p.RunID]; ok {
		r.outputs[outport] = append(r.outputs[outport], p.Data)
	}
}

func (i *Instance) finished(runID string, outputs int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if r, ok := i.runs[runID]; ok {
		r.done <- outputs
	}
}
