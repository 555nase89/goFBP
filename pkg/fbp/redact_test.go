package fbp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const secret = "s3cret"

// passOn emits the incoming data unchanged, like a Brick that merges its
// result into the payload without removing anything.
func passOn(name string) *Brick {
	return &Brick{Name: name, Process: func(ctx context.Context, p Packet, out Emitter) error {
		return out.Emit(ctx, Success, p.Data)
	}}
}

// addSecret adds the field key with the secret to the incoming object and
// declares it with Redact, like a Brick that loads a key from a vault.
func addSecret() *Brick {
	return &Brick{
		Name:   "AddSecret",
		Redact: RedactFields("key"),
		Process: func(ctx context.Context, p Packet, out Emitter) error {
			var obj map[string]any
			if err := json.Unmarshal(p.Data, &obj); err != nil {
				return err
			}
			obj["key"] = secret
			data, _ := json.Marshal(obj)
			return out.Emit(ctx, Success, data)
		},
	}
}

func failing(name string) *Brick {
	return &Brick{Name: name, Process: func(context.Context, Packet, Emitter) error {
		return errors.New("failed")
	}}
}

// allEvents collects Events until none arrived for a while.
func allEvents(ch <-chan Event) []Event {
	var got []Event
	for {
		select {
		case e := <-ch:
			e.Data = append(json.RawMessage(nil), e.Data...)
			got = append(got, e)
		case <-time.After(100 * time.Millisecond):
			return got
		}
	}
}

func TestRedactFields(t *testing.T) {
	redact := RedactFields("key", "token")
	tests := map[string]struct{ in, want string }{
		"object":        {`{"a":1,"key":{"private":"x"},"token":"t"}`, `{"a":1,"key":"[redacted]","token":"[redacted]"}`},
		"no such field": {`{"a":1}`, `{"a":1}`},
		"nested":        {`{"a":{"key":"x"}}`, `{"a":{"key":"x"}}`},
		"no object":     {`["key"]`, `["key"]`},
		"string":        {`"key"`, `"key"`},
	}
	for name, tc := range tests {
		if got := string(redact(json.RawMessage(tc.in))); got != tc.want {
			t.Errorf("%s: got %s, want %s", name, got, tc.want)
		}
	}
}

// TestRedactFollowsPacket checks that the Redact of a Brick hides the field
// in the Events of its own emit and of every later station of the Run, also
// behind a Brick without Redact, while the Packets keep the real data.
func TestRedactFollowsPacket(t *testing.T) {
	n := &Network{Name: "redact"}
	must(t, n.Add("addsecret", addSecret()))
	must(t, n.Add("passon", passOn("PassOn")))
	must(t, n.ConnectIn("addsecret"))
	must(t, n.Connect("addsecret", Success, "passon"))
	must(t, n.ConnectOut("passon", Success, Success))

	fn, events := observed()
	outFn, outputs := collector()
	inst := start(t, n, WithObserver(fn), WithOutput(outFn))
	_, err := inst.Send(context.Background(), Packet{Data: json.RawMessage(`{"a":1}`)})
	must(t, err)

	if o := next(t, outputs); !strings.Contains(string(o.p.Data), secret) {
		t.Errorf("output lost the real data: %s", o.p.Data)
	}
	stations := map[string]bool{}
	for _, e := range allEvents(events) {
		if strings.Contains(string(e.Data), secret) {
			t.Errorf("%s %s shows the secret: %s", e.Kind, e.Node, e.Data)
		}
		if strings.Contains(string(e.Data), `"key":"[redacted]"`) {
			stations[e.Kind.String()+" "+e.Node] = true
		}
	}
	for _, want := range []string{"Emitted addsecret", "Received passon", "Emitted passon", "Output "} {
		if !stations[want] {
			t.Errorf("no redacted Event %q, got %v", want, stations)
		}
	}
}

// TestRedactErrorData checks the Input of the ErrorData of a Brick behind
// the redacting Brick, and of a Brick that redacts its own input.
func TestRedactErrorData(t *testing.T) {
	behind := &Network{Name: "behind"}
	must(t, behind.Add("addsecret", addSecret()))
	must(t, behind.Add("fail", failing("Fail")))
	must(t, behind.ConnectIn("addsecret"))
	must(t, behind.Connect("addsecret", Success, "fail"))
	must(t, behind.ConnectOut("fail", Error, Error))

	own := failing("Fail")
	own.Redact = RedactFields("key")
	self := &Network{Name: "self"}
	must(t, self.Add("fail", own))
	must(t, self.ConnectIn("fail"))
	must(t, self.ConnectOut("fail", Error, Error))

	for name, n := range map[string]*Network{"behind": behind, "self": self} {
		fn, events := observed()
		outFn, outputs := collector()
		inst := start(t, n, WithObserver(fn), WithOutput(outFn))
		_, err := inst.Send(context.Background(), Packet{Data: json.RawMessage(`{"a":1,"key":"` + secret + `"}`)})
		must(t, err)

		o := next(t, outputs)
		ed, err := DataAs[ErrorData](o.p)
		must(t, err)
		if ed.Node != "fail" || string(ed.Input) != `{"a":1,"key":"[redacted]"}` {
			t.Errorf("%s: ErrorData = %+v, input %s", name, ed, ed.Input)
		}
		for _, e := range allEvents(events) {
			if e.Kind != Sent && strings.Contains(string(e.Data), secret) {
				t.Errorf("%s: %s %s shows the secret: %s", name, e.Kind, e.Node, e.Data)
			}
		}
	}
}

// TestRedactFailsClosed checks that a panicking RedactFunc hides the whole
// data instead of showing it.
func TestRedactFailsClosed(t *testing.T) {
	b := failing("Fail")
	b.Redact = func(json.RawMessage) json.RawMessage { panic("boom") }
	n := &Network{Name: "closed"}
	must(t, n.Add("fail", b))
	must(t, n.ConnectIn("fail"))
	must(t, n.ConnectOut("fail", Error, Error))

	fn, events := observed()
	outFn, outputs := collector()
	inst := start(t, n, WithObserver(fn), WithOutput(outFn))
	_, err := inst.Send(context.Background(), Packet{Data: str(secret)})
	must(t, err)

	ed, err := DataAs[ErrorData](next(t, outputs).p)
	must(t, err)
	if ed.Message != "failed" || ed.Input != nil {
		t.Errorf("ErrorData = %+v, input %s", ed, ed.Input)
	}
	for _, e := range allEvents(events) {
		if e.Kind == Received && e.Data != nil {
			t.Errorf("Received shows data: %s", e.Data)
		}
	}
}

func TestRedactionHoldsBrickOnce(t *testing.T) {
	a, b := addSecret(), addSecret()
	var r *redaction
	r = r.with(a, a.Redact).with(b, b.Redact).with(a, a.Redact).with(b, b.Redact)
	if r.with(passOn("x"), nil) != r {
		t.Error("a Brick without Redact changed the chain")
	}
	count := 0
	for c := r; c != nil; c = c.next {
		count++
	}
	if count != 2 {
		t.Errorf("chain length = %d, want 2", count)
	}
}
