package fbptest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/555nase89/goFBP/pkg/fbp"
)

// CallBrick calls the ProcessFunc of b once with data (JSON text) and returns
// what it emitted, without a Network, channels or goroutines. It follows the
// rules of an Instance: a returned error or a panic becomes ErrorData on the
// Error Outport (with an empty Node, as there is no Network), Emit refuses
// unknown Outports, invalid JSON and calls after the ProcessFunc returned, and
// emitted data is copied. Invalid input data fails the test.
func CallBrick(t testing.TB, b *fbp.Brick, data string) Result {
	t.Helper()
	if !json.Valid([]byte(data)) {
		t.Fatalf("CallBrick %s: input %q: %v", b.Name, data, fbp.ErrInvalidJSON)
	}
	rec := &recorder{
		brick:   b,
		outputs: map[string][]json.RawMessage{},
		ports:   append([]string{fbp.Success, fbp.Error}, b.Outports...),
	}
	p := fbp.Packet{Data: json.RawMessage(data), RunID: fbp.NewRunID()}
	if err := rec.call(t.Context(), p); err != nil {
		ed, _ := json.Marshal(fbp.ErrorData{Brick: b.Name, Message: err.Error(), Input: p.Data})
		rec.outputs[fbp.Error] = append(rec.outputs[fbp.Error], ed)
	}
	return Result{RunID: p.RunID, Outputs: rec.outputs}
}

// recorder is the Emitter handed to the ProcessFunc by CallBrick.
type recorder struct {
	brick   *fbp.Brick
	ports   []string
	mu      sync.Mutex
	closed  bool // the ProcessFunc has returned
	outputs map[string][]json.RawMessage
}

func (r *recorder) call(ctx context.Context, p fbp.Packet) (err error) {
	defer func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
	}()
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("panic: %v", v)
		}
	}()
	return r.brick.Process(ctx, p, r)
}

func (r *recorder) Emit(_ context.Context, outport string, data json.RawMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("fbptest: brick %q, outport %q: %w", r.brick.Name, outport, fbp.ErrEmitAfterProcess)
	}
	if !slices.Contains(r.ports, outport) {
		return fmt.Errorf("fbptest: brick %q has no outport %q", r.brick.Name, outport)
	}
	if !json.Valid(data) {
		return fmt.Errorf("fbptest: brick %q, outport %q: %w", r.brick.Name, outport, fbp.ErrInvalidJSON)
	}
	r.outputs[outport] = append(r.outputs[outport], slices.Clone(data))
	return nil
}
