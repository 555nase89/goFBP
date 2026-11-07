package fbptest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"
)

// fakeT records failures instead of failing the test, so the helpers can be
// checked for reporting them.
type fakeT struct {
	testing.TB
	msgs  []string
	fatal bool
}

func (f *fakeT) Helper() {}

func (f *fakeT) Errorf(format string, args ...any) {
	f.msgs = append(f.msgs, fmt.Sprintf(format, args...))
}

func (f *fakeT) Fatalf(format string, args ...any) {
	f.Errorf(format, args...)
	f.fatal = true
	runtime.Goexit()
}

// check runs fn with a fakeT in its own goroutine, so Fatalf can end it.
func check(t *testing.T, fn func(ft *fakeT)) *fakeT {
	ft := &fakeT{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(ft)
	}()
	<-done
	return ft
}

func brick(name string, process fbp.ProcessFunc, outports ...string) *fbp.Brick {
	return &fbp.Brick{Name: name, Outports: outports, Process: process}
}

// double emits the number twice its input; odd numbers are an error.
var double = brick("double", func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
	n, err := fbp.DataAs[int](p)
	if err != nil {
		return err
	}
	if n%2 != 0 {
		return fmt.Errorf("odd number %d", n)
	}
	return fbp.EmitAs(ctx, out, fbp.Success, n*2)
})

func TestCallBrick(t *testing.T) {
	CallBrick(t, double, `4`).Want(t, Outputs{fbp.Success: {`8`}})
	CallBrick(t, double, `3`).WantError(t, "", "odd number 3")

	panics := brick("panics", func(context.Context, fbp.Packet, fbp.Emitter) error { panic("boom") })
	CallBrick(t, panics, `1`).WantError(t, "", "panic: boom")

	multi := brick("multi", func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		return errors.Join(
			out.Emit(ctx, "Other", []byte(`{"b":1, "a":2}`)),
			out.Emit(ctx, fbp.Success, []byte(`1`)),
			out.Emit(ctx, fbp.Success, []byte(`2`)),
		)
	}, "Other")
	CallBrick(t, multi, `null`).Want(t, Outputs{
		fbp.Success: {`2`, `1`}, // order does not matter
		"Other":     {`{"a":2,"b":1}`},
	})
}

func TestCallBrickRefusesEmit(t *testing.T) {
	unknown := brick("unknown", func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		return out.Emit(ctx, "Nope", p.Data)
	})
	CallBrick(t, unknown, `1`).WantError(t, "", `has no outport "Nope"`)

	invalid := brick("invalid", func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		return out.Emit(ctx, fbp.Success, []byte(`{`))
	})
	CallBrick(t, invalid, `1`).WantError(t, "", "not valid JSON")

	var kept fbp.Emitter
	keeps := brick("keeps", func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		kept = out
		return nil
	})
	CallBrick(t, keeps, `1`).Want(t, Outputs{})
	if err := kept.Emit(t.Context(), fbp.Success, []byte(`1`)); !errors.Is(err, fbp.ErrEmitAfterProcess) {
		t.Errorf("Emit after return: got %v", err)
	}
}

func TestCallBrickInvalidInput(t *testing.T) {
	ft := check(t, func(ft *fakeT) { CallBrick(ft, double, `{`) })
	if !ft.fatal {
		t.Error("invalid input did not fail the test")
	}
}

// network builds In -> double -> Success, with errors to Error.
func network(t *testing.T) *fbp.Network {
	n := &fbp.Network{Name: "net"}
	if err := errors.Join(
		n.Add("double", double),
		n.ConnectIn("double"),
		n.ConnectOut("double", fbp.Success, fbp.Success),
		n.ConnectOut("double", fbp.Error, fbp.Error),
	); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRun(t *testing.T) {
	Run(t, network(t), `2`).Want(t, Outputs{fbp.Success: {`4`}})

	inst := Start(t, network(t))
	inst.Run(t, `5`).WantError(t, "double", "odd number 5")
	inst.Run(t, `6`).Want(t, Outputs{fbp.Success: {`12`}})
}

func TestRunFanOut(t *testing.T) {
	n := &fbp.Network{Name: "fanout"}
	if err := errors.Join(
		n.Add("a", double),
		n.Add("b", double),
		n.ConnectIn("a"),
		n.ConnectIn("b"),
		n.ConnectOut("a", fbp.Success, fbp.Success),
		n.ConnectOut("b", fbp.Success, fbp.Success),
	); err != nil {
		t.Fatal(err)
	}
	Run(t, n, `1`).Want(t, Outputs{}) // both errors go to unconnected Error Outports
	Run(t, n, `2`).Want(t, Outputs{fbp.Success: {`4`, `4`}})
}

func TestRunTimeout(t *testing.T) {
	old := Timeout
	Timeout = 50 * time.Millisecond
	t.Cleanup(func() { Timeout = old })

	blocks := brick("blocks", func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		<-ctx.Done()
		return ctx.Err()
	})
	n := &fbp.Network{Name: "blocking"}
	if err := errors.Join(n.Add("blocks", blocks), n.ConnectIn("blocks")); err != nil {
		t.Fatal(err)
	}
	ft := check(t, func(ft *fakeT) { Run(ft, n, `1`) })
	if !ft.fatal || !strings.Contains(strings.Join(ft.msgs, "\n"), "not finished") {
		t.Errorf("got %v", ft.msgs)
	}
}

func TestWantReportsMismatch(t *testing.T) {
	res := Result{RunID: "r", Outputs: map[string][]json.RawMessage{fbp.Success: {[]byte(`1`)}}}
	tests := []struct {
		name string
		want Outputs
	}{
		{"other data", Outputs{fbp.Success: {`2`}}},
		{"extra packet", Outputs{fbp.Success: {`1`, `1`}}},
		{"missing outport", Outputs{fbp.Success: {`1`}, fbp.Error: {`1`}}},
		{"unexpected outport", Outputs{}},
		{"invalid want", Outputs{fbp.Success: {`{`}}},
	}
	for _, tt := range tests {
		ft := check(t, func(ft *fakeT) { res.Want(ft, tt.want) })
		if len(ft.msgs) == 0 {
			t.Errorf("%s: mismatch not reported", tt.name)
		}
	}
	if ft := check(t, func(ft *fakeT) { res.Want(ft, Outputs{fbp.Success: {` 1 `}}) }); len(ft.msgs) != 0 {
		t.Errorf("match reported: %v", ft.msgs)
	}
}

func TestWantErrorReportsMismatch(t *testing.T) {
	ed := []byte(`{"node":"a/b","brick":"B","message":"bad input"}`)
	tests := []struct {
		name    string
		outputs map[string][]json.RawMessage
		node    string
		message string
		ok      bool
	}{
		{"match", map[string][]json.RawMessage{fbp.Error: {ed}}, "a/b", "bad", true},
		{"any node", map[string][]json.RawMessage{fbp.Error: {ed}}, "", "input", true},
		{"other node", map[string][]json.RawMessage{fbp.Error: {ed}}, "a", "bad", false},
		{"other message", map[string][]json.RawMessage{fbp.Error: {ed}}, "a/b", "good", false},
		{"two errors", map[string][]json.RawMessage{fbp.Error: {ed, ed}}, "", "", false},
		{"also success", map[string][]json.RawMessage{fbp.Error: {ed}, fbp.Success: {[]byte(`1`)}}, "", "", false},
		{"no ErrorData", map[string][]json.RawMessage{fbp.Error: {[]byte(`1`)}}, "", "", false},
	}
	for _, tt := range tests {
		res := Result{RunID: "r", Outputs: tt.outputs}
		ft := check(t, func(ft *fakeT) { res.WantError(ft, tt.node, tt.message) })
		if ok := len(ft.msgs) == 0; ok != tt.ok {
			t.Errorf("%s: got ok %v, messages %v", tt.name, ok, ft.msgs)
		}
	}
}
