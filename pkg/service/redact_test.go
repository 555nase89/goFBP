package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"
	"github.com/nats-io/nats.go"
)

// TestTraceIsRedacted checks that a field declared with Brick.Redact does
// not appear on the Trace subject, while the Outport result keeps it.
func TestTraceIsRedacted(t *testing.T) {
	const secret = "s3cret"
	addSecret := &fbp.Brick{
		Name:   "AddSecret",
		Redact: fbp.RedactFields("key"),
		Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
			return out.Emit(ctx, fbp.Success, json.RawMessage(`{"a":1,"key":"`+secret+`"}`))
		},
	}
	passOn := &fbp.Brick{Name: "PassOn", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		return out.Emit(ctx, fbp.Success, p.Data)
	}}
	n := &fbp.Network{Name: "redact"}
	for _, err := range []error{
		n.Add("addsecret", addSecret),
		n.Add("passon", passOn),
		n.ConnectIn("addsecret"),
		n.Connect("addsecret", fbp.Success, "passon"),
		n.ConnectOut("passon", fbp.Success, fbp.Success),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}

	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	events, _ := nc.SubscribeSync(TraceSubject(s.Base()))
	success, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Success, "*"))
	if err := nc.Publish(ObserveSubject(s.Base()), []byte(`true`)); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	msg := nats.NewMsg(InportSubject(s.Base()))
	msg.Data = []byte(`{}`)
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
	if got := nextMsg(t, success); !strings.Contains(string(got.Data), secret) {
		t.Errorf("result lost the real data: %s", got.Data)
	}

	redacted := 0
	for {
		m, err := events.NextMsg(200 * time.Millisecond)
		if err != nil {
			break
		}
		if strings.Contains(string(m.Data), secret) {
			t.Errorf("trace shows the secret: %s", m.Data)
		}
		if strings.Contains(string(m.Data), `[redacted]`) {
			redacted++
		}
	}
	// Emitted addsecret, Received passon, Emitted passon, Output.
	if redacted != 4 {
		t.Errorf("%d redacted Events, want 4", redacted)
	}
}
