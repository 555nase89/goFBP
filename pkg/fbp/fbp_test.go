package fbp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const timeout = 2 * time.Second

// null is valid Packet data for tests that do not look at the data.
var null = json.RawMessage(`null`)

type output struct {
	outport string
	p       Packet
}

// collector returns an OutputFunc that forwards everything to a channel.
func collector() (OutputFunc, chan output) {
	ch := make(chan output, 100)
	return func(_ context.Context, outport string, p Packet) { ch <- output{outport, p} }, ch
}

func next(t *testing.T, ch <-chan output) output {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(timeout):
		t.Fatal("timeout waiting for output")
		return output{}
	}
}

func str(s string) json.RawMessage {
	data, _ := json.Marshal(s)
	return data
}

// stringBrick builds a Brick that transforms a JSON string with fn.
func stringBrick(name string, fn func(string) (string, error)) *Brick {
	return &Brick{
		Name: name,
		Process: func(ctx context.Context, p Packet, out Emitter) error {
			var s string
			if err := json.Unmarshal(p.Data, &s); err != nil {
				return err
			}
			r, err := fn(s)
			if err != nil {
				return err
			}
			return out.Emit(ctx, Success, str(r))
		},
	}
}

func upper() *Brick {
	return stringBrick("Upper", func(s string) (string, error) { return strings.ToUpper(s), nil })
}

func exclaim() *Brick {
	return stringBrick("Exclaim", func(s string) (string, error) { return s + "!", nil })
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func start(t *testing.T, n *Network, opts ...Option) *Instance {
	t.Helper()
	inst, err := n.Start(context.Background(), opts...)
	must(t, err)
	t.Cleanup(inst.Stop)
	return inst
}

func TestPipeline(t *testing.T) {
	n := &Network{Name: "pipeline"}
	must(t, n.Add("upper", upper()))
	must(t, n.Add("exclaim", exclaim()))
	must(t, n.ConnectIn("upper"))
	must(t, n.Connect("upper", Success, "exclaim"))
	must(t, n.ConnectOut("exclaim", Success, Success))

	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))

	runID, err := inst.Send(context.Background(), Packet{Data: str("hello")})
	must(t, err)
	if runID == "" {
		t.Fatal("no run ID assigned")
	}
	o := next(t, ch)
	if o.outport != Success || string(o.p.Data) != `"HELLO!"` || o.p.RunID != runID {
		t.Fatalf("got %s %s %s", o.outport, o.p.Data, o.p.RunID)
	}

	// An existing run ID is kept.
	_, err = inst.Send(context.Background(), Packet{Data: str("x"), RunID: "run-1"})
	must(t, err)
	if o := next(t, ch); o.p.RunID != "run-1" {
		t.Fatalf("run ID %q, want run-1", o.p.RunID)
	}
}

func TestSameBrickTwice(t *testing.T) {
	b := exclaim()
	n := &Network{Name: "twice"}
	must(t, n.Add("first", b))
	must(t, n.Add("second", b))
	must(t, n.ConnectIn("first"))
	must(t, n.Connect("first", Success, "second"))
	must(t, n.ConnectOut("second", Success, Success))

	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))
	_, err := inst.Send(context.Background(), Packet{Data: str("a")})
	must(t, err)
	if o := next(t, ch); string(o.p.Data) != `"a!!"` {
		t.Fatalf("got %s", o.p.Data)
	}
}

func TestFanOutCopiesData(t *testing.T) {
	// mutate overwrites its input in place before emitting it.
	mutate := &Brick{Name: "Mutate", Process: func(ctx context.Context, p Packet, out Emitter) error {
		p.Data[1] = 'X'
		return out.Emit(ctx, Success, p.Data)
	}}
	pass := &Brick{Name: "Pass", Process: func(ctx context.Context, p Packet, out Emitter) error {
		return out.Emit(ctx, Success, p.Data)
	}}
	n := &Network{Name: "fanout", Outports: []string{"Left", "Right"}}
	must(t, n.Add("source", pass))
	must(t, n.Add("mutate", mutate))
	must(t, n.Add("pass", pass))
	must(t, n.ConnectIn("source"))
	must(t, n.Connect("source", Success, "mutate"))
	must(t, n.Connect("source", Success, "pass"))
	must(t, n.ConnectOut("mutate", Success, "Left"))
	must(t, n.ConnectOut("pass", Success, "Right"))

	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))
	_, err := inst.Send(context.Background(), Packet{Data: str("abc")})
	must(t, err)

	got := map[string]string{}
	for range 2 {
		o := next(t, ch)
		got[o.outport] = string(o.p.Data)
	}
	if got["Left"] != `"Xbc"` || got["Right"] != `"abc"` {
		t.Fatalf("got %v", got)
	}
}

func TestErrorAndPanicGoToErrorOutport(t *testing.T) {
	fail := stringBrick("Fail", func(s string) (string, error) {
		if s == "panic" {
			panic("kaputt")
		}
		return "", errors.New("boom")
	})
	n := &Network{Name: "errors"}
	must(t, n.Add("fail", fail))
	must(t, n.ConnectIn("fail"))
	must(t, n.ConnectOut("fail", Error, Error))

	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))

	for input, want := range map[string]string{"x": "boom", "panic": "panic: kaputt"} {
		runID, err := inst.Send(context.Background(), Packet{Data: str(input)})
		must(t, err)
		o := next(t, ch)
		var e ErrorData
		must(t, json.Unmarshal(o.p.Data, &e))
		if o.outport != Error || o.p.RunID != runID || e.Node != "fail" || e.Brick != "Fail" ||
			e.Message != want || string(e.Input) != string(str(input)) {
			t.Fatalf("got %s %s %+v", o.outport, o.p.RunID, e)
		}
	}
}

// recordHandler is a slog.Handler that forwards records to a channel.
type recordHandler struct{ ch chan slog.Record }

func (h recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.ch <- r.Clone()
	return nil
}
func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordHandler) WithGroup(string) slog.Handler      { return h }

func TestUnconnectedErrorIsLogged(t *testing.T) {
	n := &Network{Name: "unconnected"}
	must(t, n.Add("upper", upper()))
	must(t, n.ConnectIn("upper"))

	logs := make(chan slog.Record, 10)
	inst := start(t, n, WithLogger(slog.New(recordHandler{logs})))
	_, err := inst.Send(context.Background(), Packet{Data: json.RawMessage(`42`)})
	must(t, err)

	select {
	case r := <-logs:
		if r.Level != slog.LevelWarn || !strings.Contains(r.Message, "unconnected") {
			t.Fatalf("unexpected log record %v %q", r.Level, r.Message)
		}
	case <-time.After(timeout):
		t.Fatal("error packet was not logged")
	}
}

func TestConcurrency(t *testing.T) {
	const copies = 3
	arrived := make(chan struct{})
	release := make(chan struct{})
	wait := &Brick{Name: "Wait", Concurrency: copies, Process: func(ctx context.Context, p Packet, out Emitter) error {
		arrived <- struct{}{}
		<-release
		return out.Emit(ctx, Success, p.Data)
	}}
	n := &Network{Name: "concurrency"}
	must(t, n.Add("wait", wait))
	must(t, n.ConnectIn("wait"))
	must(t, n.ConnectOut("wait", Success, Success))

	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))
	for range copies {
		_, err := inst.Send(context.Background(), Packet{Data: json.RawMessage(`1`)})
		must(t, err)
	}
	// Every copy blocks until release, so all of them must be inside Process
	// at the same time.
	for range copies {
		select {
		case <-arrived:
		case <-time.After(timeout):
			t.Fatal("packets are not processed in parallel")
		}
	}
	close(release)
	for range copies {
		next(t, ch)
	}
}

func TestBackpressureAndStop(t *testing.T) {
	started := make(chan struct{}, 1)
	block := &Brick{Name: "Block", Process: func(ctx context.Context, p Packet, out Emitter) error {
		started <- struct{}{}
		<-ctx.Done()
		return nil
	}}
	n := &Network{Name: "backpressure"}
	must(t, n.Add("block", block))
	must(t, n.ConnectIn("block"))

	inst, err := n.Start(context.Background(), WithBufferSize(1))
	must(t, err)

	// First packet is taken by the Brick, second fills the buffer.
	_, err = inst.Send(context.Background(), Packet{Data: null})
	must(t, err)
	<-started
	_, err = inst.Send(context.Background(), Packet{Data: null})
	must(t, err)

	// Third one has to wait for space.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := inst.Send(ctx, Packet{Data: null}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}

	// A blocked Send is released by Stop.
	sendErr := make(chan error)
	go func() {
		_, err := inst.Send(context.Background(), Packet{Data: null})
		sendErr <- err
	}()
	time.Sleep(20 * time.Millisecond)

	stopped := make(chan struct{})
	go func() { inst.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(timeout):
		t.Fatal("Stop did not return")
	}
	if err := <-sendErr; !errors.Is(err, ErrStopped) {
		t.Fatalf("blocked Send: got %v, want ErrStopped", err)
	}
	if _, err := inst.Send(context.Background(), Packet{Data: null}); !errors.Is(err, ErrStopped) {
		t.Fatalf("Send after Stop: got %v, want ErrStopped", err)
	}
}

func TestEmitReleasedByStop(t *testing.T) {
	emitErr := make(chan error, 1)
	// The Brick passes its own context, so only the Instance can release it.
	src := &Brick{Name: "Source", Process: func(ctx context.Context, p Packet, out Emitter) error {
		for {
			if err := out.Emit(context.Background(), Success, p.Data); err != nil {
				emitErr <- err
				return err
			}
		}
	}}
	block := &Brick{Name: "Block", Process: func(ctx context.Context, p Packet, out Emitter) error {
		<-ctx.Done()
		return nil
	}}
	n := &Network{Name: "emitstop"}
	must(t, n.Add("source", src))
	must(t, n.Add("block", block))
	must(t, n.ConnectIn("source"))
	must(t, n.Connect("source", Success, "block"))

	inst, err := n.Start(context.Background(), WithBufferSize(1))
	must(t, err)
	_, err = inst.Send(context.Background(), Packet{Data: null})
	must(t, err)
	time.Sleep(20 * time.Millisecond)

	stopped := make(chan struct{})
	go func() { inst.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(timeout):
		t.Fatal("Stop did not return while Emit was blocked")
	}
	if err := <-emitErr; !errors.Is(err, ErrStopped) {
		t.Fatalf("blocked Emit: got %v, want ErrStopped", err)
	}
}

func TestBricksShareName(t *testing.T) {
	n := &Network{Name: "samename"}
	must(t, n.Add("a", upper()))
	must(t, n.Add("b", upper()))
	must(t, n.ConnectIn("a"))
	must(t, n.Connect("a", Success, "b"))
	must(t, n.ConnectOut("b", Success, Success))

	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))
	_, err := inst.SendAs(context.Background(), "x")
	must(t, err)
	next(t, ch)
}

func TestBuildErrors(t *testing.T) {
	tests := map[string]func(n *Network) error{
		"empty node name":  func(n *Network) error { return n.Add("", upper()) },
		"duplicate node":   func(n *Network) error { n.Add("a", upper()); return n.Add("a", exclaim()) },
		"no process":       func(n *Network) error { return n.Add("a", &Brick{Name: "X"}) },
		"unknown node":     func(n *Network) error { n.Add("a", upper()); return n.Connect("a", Success, "b") },
		"unknown outport":  func(n *Network) error { n.Add("a", upper()); return n.ConnectOut("a", "Nope", Success) },
		"unknown net port": func(n *Network) error { n.Add("a", upper()); return n.ConnectOut("a", Success, "Nope") },
		"duplicate connection": func(n *Network) error {
			n.Add("a", upper())
			n.Add("b", exclaim())
			n.Connect("a", Success, "b")
			return n.Connect("a", Success, "b")
		},
	}
	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			if err := build(&Network{Name: "n"}); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestValidate(t *testing.T) {
	n := &Network{Name: "cycle"}
	must(t, n.Add("a", upper()))
	must(t, n.Add("b", exclaim()))
	must(t, n.ConnectIn("a"))
	must(t, n.Connect("a", Success, "b"))
	must(t, n.Connect("b", Success, "a"))
	if _, err := n.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("got %v, want cycle error", err)
	}

	unconnected := &Network{Name: "unconnected"}
	must(t, unconnected.Add("a", upper()))
	if err := unconnected.Validate(); err == nil {
		t.Fatal("expected error for unconnected inport")
	}

	if err := (&Network{}).Validate(); err == nil {
		t.Fatal("expected error for missing name")
	}
}

// shout is a nested Network: Upper -> Exclaim, with an extra Outport for errors
// of its inner Bricks.
func shout(t *testing.T) *Network {
	t.Helper()
	n := &Network{Name: "shout"}
	must(t, n.Add("upper", upper()))
	must(t, n.Add("exclaim", exclaim()))
	must(t, n.ConnectIn("upper"))
	must(t, n.Connect("upper", Success, "exclaim"))
	must(t, n.ConnectOut("exclaim", Success, Success))
	must(t, n.ConnectOut("upper", Error, Error))
	return n
}

func TestNestedNetwork(t *testing.T) {
	inner := shout(t)
	outer := &Network{Name: "outer"}
	// The same nested Network twice gets separate runtime copies.
	must(t, outer.Add("first", inner))
	must(t, outer.Add("second", inner))
	must(t, outer.ConnectIn("first"))
	must(t, outer.Connect("first", Success, "second"))
	must(t, outer.ConnectOut("second", Success, Success))
	must(t, outer.ConnectOut("first", Error, Error))

	fn, ch := collector()
	inst := start(t, outer, WithOutput(fn))

	runID, err := inst.Send(context.Background(), Packet{Data: str("hi")})
	must(t, err)
	if o := next(t, ch); o.outport != Success || string(o.p.Data) != `"HI!!"` || o.p.RunID != runID {
		t.Fatalf("got %s %s %s", o.outport, o.p.Data, o.p.RunID)
	}

	// Errors keep the full node path.
	_, err = inst.Send(context.Background(), Packet{Data: json.RawMessage(`42`)})
	must(t, err)
	o := next(t, ch)
	var e ErrorData
	must(t, json.Unmarshal(o.p.Data, &e))
	if o.outport != Error || e.Node != "first/upper" {
		t.Fatalf("got %s %+v", o.outport, e)
	}
}

func TestNetworkInportFanOut(t *testing.T) {
	n := &Network{Name: "fanin", Outports: []string{"Upper", "Exclaim"}}
	must(t, n.Add("upper", upper()))
	must(t, n.Add("exclaim", exclaim()))
	must(t, n.ConnectIn("upper"))
	must(t, n.ConnectIn("exclaim"))
	must(t, n.ConnectOut("upper", Success, "Upper"))
	must(t, n.ConnectOut("exclaim", Success, "Exclaim"))

	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))
	_, err := inst.Send(context.Background(), Packet{Data: str("a")})
	must(t, err)
	got := map[string]string{}
	for range 2 {
		o := next(t, ch)
		got[o.outport] = string(o.p.Data)
	}
	if got["Upper"] != `"A"` || got["Exclaim"] != `"a!"` {
		t.Fatalf("got %v", got)
	}
}

func TestNetworkMustNotContainItself(t *testing.T) {
	a := &Network{Name: "a"}
	b := &Network{Name: "b"}
	if err := a.Add("self", a); err == nil {
		t.Fatal("direct self-containment accepted")
	}
	must(t, a.Add("b", b))
	if err := b.Add("a", a); err == nil {
		t.Fatal("indirect self-containment accepted")
	}
}

func TestNestedValidate(t *testing.T) {
	broken := &Network{Name: "broken"}
	must(t, broken.Add("upper", upper()))
	outer := &Network{Name: "outer"}
	must(t, outer.Add("broken", broken))
	must(t, outer.ConnectIn("broken"))
	if err := outer.Validate(); err == nil || !strings.Contains(err.Error(), "broken") {
		t.Fatalf("got %v, want error of nested network", err)
	}
}

func TestInvalidJSONIsRejected(t *testing.T) {
	bad := &Brick{Name: "Bad", Process: func(ctx context.Context, p Packet, out Emitter) error {
		return out.Emit(ctx, Success, json.RawMessage(`hello`))
	}}
	n := &Network{Name: "invalid"}
	must(t, n.Add("bad", bad))
	must(t, n.ConnectIn("bad"))
	must(t, n.ConnectOut("bad", Success, Success))
	must(t, n.ConnectOut("bad", Error, Error))
	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))

	if _, err := inst.Send(context.Background(), Packet{Data: json.RawMessage(`hello`)}); !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("Send: got %v, want ErrInvalidJSON", err)
	}

	// Emit refuses the data, so the Brick fails with a complete ErrorData.
	_, err := inst.Send(context.Background(), Packet{Data: str("x")})
	must(t, err)
	o := next(t, ch)
	var e ErrorData
	if err := json.Unmarshal(o.p.Data, &e); err != nil {
		t.Fatal(err)
	}
	if o.outport != Error || e.Brick != "Bad" || !strings.Contains(e.Message, "not valid JSON") || string(e.Input) != `"x"` {
		t.Fatalf("got %s %s", o.outport, o.p.Data)
	}
}

func TestOutportWithoutName(t *testing.T) {
	b := upper()
	b.Outports = []string{""}
	if err := (&Network{Name: "n"}).Add("a", b); err == nil {
		t.Fatal("brick with unnamed outport accepted")
	}

	n := &Network{Name: "n", Outports: []string{""}}
	must(t, n.Add("a", upper()))
	must(t, n.ConnectIn("a"))
	if err := n.Validate(); err == nil {
		t.Fatal("network with unnamed outport accepted")
	}
}

func TestDataIsCopied(t *testing.T) {
	// reuse emits three numbers from one buffer that it overwrites each time.
	reuse := &Brick{Name: "Reuse", Process: func(ctx context.Context, p Packet, out Emitter) error {
		buf := make([]byte, 0, 8)
		for _, c := range "123" {
			buf = append(buf[:0], byte(c))
			if err := out.Emit(ctx, Success, buf); err != nil {
				return err
			}
		}
		return nil
	}}
	block := make(chan struct{})
	slow := &Brick{Name: "Slow", Process: func(ctx context.Context, p Packet, out Emitter) error {
		<-block
		return out.Emit(ctx, Success, p.Data)
	}}
	n := &Network{Name: "copy"}
	must(t, n.Add("reuse", reuse))
	must(t, n.Add("slow", slow))
	must(t, n.ConnectIn("reuse"))
	must(t, n.Connect("reuse", Success, "slow"))
	must(t, n.ConnectOut("slow", Success, Success))
	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))

	in := json.RawMessage(`"abc"`)
	_, err := inst.Send(context.Background(), Packet{Data: in})
	must(t, err)
	in[1] = 'X' // changing the slice after Send must not affect the Packet

	// Slow only starts once all three Packets wait in its buffer.
	time.Sleep(20 * time.Millisecond)
	close(block)
	for _, want := range []string{"1", "2", "3"} {
		if o := next(t, ch); string(o.p.Data) != want {
			t.Fatalf("got %s, want %s", o.p.Data, want)
		}
	}
}

func TestDataAs(t *testing.T) {
	s, err := DataAs[string](Packet{Data: json.RawMessage(`"hi"`)})
	if err != nil || s != "hi" {
		t.Fatalf("DataAs[string] = %q, %v; want \"hi\", nil", s, err)
	}
	if _, err := DataAs[string](Packet{Data: json.RawMessage(`42`)}); err == nil {
		t.Fatal("DataAs[string] of a number: want error, got nil")
	}
}

// recorder is an Emitter that keeps the last emitted data.
type recorder struct {
	outport string
	data    json.RawMessage
}

func (r *recorder) Emit(_ context.Context, outport string, data json.RawMessage) error {
	r.outport, r.data = outport, data
	return nil
}

func TestEmitAs(t *testing.T) {
	var r recorder
	must(t, EmitAs(context.Background(), &r, Success, map[string]int{"a": 1}))
	if r.outport != Success || string(r.data) != `{"a":1}` {
		t.Fatalf("got %s %s", r.outport, r.data)
	}
	r = recorder{}
	if err := EmitAs(context.Background(), &r, Success, make(chan int)); err == nil || r.data != nil {
		t.Fatalf("EmitAs of a channel: got err %v, data %s; want error and nothing sent", err, r.data)
	}
}

func TestSendAs(t *testing.T) {
	n := &Network{Name: "sendas"}
	must(t, n.Add("upper", upper()))
	must(t, n.ConnectIn("upper"))
	must(t, n.ConnectOut("upper", Success, Success))
	fn, ch := collector()
	inst := start(t, n, WithOutput(fn))

	runID, err := inst.SendAs(context.Background(), "hi")
	must(t, err)
	o := next(t, ch)
	if o.outport != Success || string(o.p.Data) != `"HI"` || runID == "" || o.p.RunID != runID {
		t.Fatalf("got %s %s (run %q), want Success \"HI\" (run %q)", o.outport, o.p.Data, o.p.RunID, runID)
	}
	if _, err := inst.SendAs(context.Background(), make(chan int)); err == nil {
		t.Fatal("SendAs of a channel: want error, got nil")
	}
}
