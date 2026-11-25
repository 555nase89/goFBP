package service

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go"
)

func TestCall(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, c := range []struct{ in, outport string }{
		{`"hello"`, fbp.Success},
		{`42`, fbp.Error},
	} {
		outport, p, err := Call(ctx, nc, s.Base(), []byte(c.in))
		if err != nil {
			t.Fatalf("input %s: %v", c.in, err)
		}
		if outport != c.outport || p.RunID == "" {
			t.Fatalf("input %s: got %s %s (run %q), want outport %s", c.in, outport, p.Data, p.RunID, c.outport)
		}
		if c.outport == fbp.Success && string(p.Data) != `"HELLO"` {
			t.Fatalf("got %s", p.Data)
		}
	}

	if _, _, err := Call(ctx, nc, s.Base(), []byte(`not json`)); !errors.Is(err, fbp.ErrInvalidJSON) {
		t.Fatalf("invalid JSON: got %v", err)
	}
}

func TestCallWithoutService(t *testing.T) {
	nc := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := Call(ctx, nc, "fbp.gofbp.test.none", []byte(`"x"`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

// fanOutNetwork sends every input string twice to Success and once to Copy.
func fanOutNetwork(t *testing.T) *fbp.Network {
	t.Helper()
	twice := &fbp.Brick{Name: "Twice", Outports: []string{"Copy"}, Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		if _, err := fbp.DataAs[string](p); err != nil {
			return err
		}
		return errors.Join(out.Emit(ctx, fbp.Success, p.Data), out.Emit(ctx, fbp.Success, p.Data), out.Emit(ctx, "Copy", p.Data))
	}}
	n := &fbp.Network{Name: "fanout", Outports: []string{"Copy"}}
	for _, err := range []error{
		n.Add("twice", twice),
		n.ConnectIn("twice"),
		n.ConnectOut("twice", fbp.Success, fbp.Success),
		n.ConnectOut("twice", "Copy", "Copy"),
		n.ConnectOut("twice", fbp.Error, fbp.Error),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	return n
}

func TestCallAll(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", fanOutNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	for _, c := range []struct {
		in   string
		want []string
	}{
		{`"a"`, []string{`Copy "a"`, `Success "a"`, `Success "a"`}},
		{`42`, []string{"Error"}},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		var got []string
		err := CallAll(ctx, nc, s.Base(), []byte(c.in), func(outport string, p fbp.Packet) {
			if outport == fbp.Error {
				got = append(got, outport)
				return
			}
			got = append(got, outport+" "+string(p.Data))
		})
		cancel()
		if err != nil {
			t.Fatalf("input %s: %v", c.in, err)
		}
		slices.Sort(got)
		if !slices.Equal(got, c.want) {
			t.Errorf("input %s: got %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCallAllWithoutService(t *testing.T) {
	nc := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := CallAll(ctx, nc, "fbp.gofbp.test.none", []byte(`"x"`), func(string, fbp.Packet) {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestRejectedInputIsDone(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	sub, err := nc.SubscribeSync(DoneSubject(s.Base(), "run-1"))
	if err != nil {
		t.Fatal(err)
	}
	msg := nats.NewMsg(InportSubject(s.Base()))
	msg.Data = []byte(`not json`)
	msg.Header.Set(RunIDHeader, "run-1")
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
	var d DoneData
	if err := json.Unmarshal(nextMsg(t, sub).Data, &d); err != nil || d.Outputs != 1 {
		t.Fatalf("got %+v, %v; want 1 output", d, err)
	}
}

// TestCallAllLateOutput sends an Outport message after the Done message has
// been counted: the handler of CallAll must not block on it once CallAll has
// returned, even with a ctx that is never done.
func TestCallAllLateOutput(t *testing.T) {
	nc := connect(t)
	const base = "fbp.gofbp.test.late"
	fake, err := nc.Subscribe(InportSubject(base), func(msg *nats.Msg) {
		runID := msg.Header.Get(RunIDHeader)
		nc.Publish(DoneSubject(base, runID), []byte(`{"outputs":1}`))
		nc.Publish(OutportSubject(base, fbp.Success, runID), []byte(`1`))
		nc.Publish(OutportSubject(base, fbp.Success, runID), []byte(`2`))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fake.Unsubscribe() })

	if err := CallAll(context.Background(), nc, base, []byte(`"x"`), func(string, fbp.Packet) {}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		buf := make([]byte, 1<<20)
		if !strings.Contains(string(buf[:runtime.Stack(buf, true)]), "service.CallAllTo.func") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("handler of CallAll still blocked after it returned")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
