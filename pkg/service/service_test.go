package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const timeout = 2 * time.Second

func connect(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(timeout) {
		t.Fatal("nats server not ready")
	}
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func upperNetwork(t *testing.T) *fbp.Network {
	t.Helper()
	upper := &fbp.Brick{Name: "Upper", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		var s string
		if err := json.Unmarshal(p.Data, &s); err != nil {
			return err
		}
		data, _ := json.Marshal(strings.ToUpper(s))
		return out.Emit(ctx, fbp.Success, data)
	}}
	n := &fbp.Network{Name: "upper"}
	for _, err := range []error{
		n.Add("upper", upper),
		n.ConnectIn("upper"),
		n.ConnectOut("upper", fbp.Success, fbp.Success),
		n.ConnectOut("upper", fbp.Error, fbp.Error),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	return n
}

func nextMsg(t *testing.T, sub *nats.Subscription) *nats.Msg {
	t.Helper()
	msg, err := sub.NextMsg(timeout)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func TestSubject(t *testing.T) {
	got, err := Subject("github.com/acme/flows", "orders")
	if err != nil || got != "fbp.github_com.acme.flows.orders" {
		t.Fatalf("got %q, %v", got, err)
	}
	for _, bad := range [][2]string{{"", "n"}, {"a//b", "n"}, {"a", ""}, {"a", "x.y"}, {"a*", "n"}, {"a", "n m"}, {"github_com/acme", "n"}, {"a/my_pkg", "n"}} {
		if _, err := Subject(bad[0], bad[1]); err == nil {
			t.Errorf("Subject(%q, %q) accepted", bad[0], bad[1])
		}
	}
}

func TestService(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	if s.Base() != "fbp.gofbp.test.upper" {
		t.Fatalf("base %q", s.Base())
	}

	success, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Success, "*"))
	failure, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Error, "*"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// Run ID from the header is kept.
	msg := nats.NewMsg(InportSubject(s.Base()))
	msg.Data = []byte(`"hello"`)
	msg.Header.Set(RunIDHeader, "run-1")
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
	got := nextMsg(t, success)
	if string(got.Data) != `"HELLO"` || got.Header.Get(RunIDHeader) != "run-1" {
		t.Fatalf("got %s %q", got.Data, got.Header.Get(RunIDHeader))
	}
	if want := OutportSubject(s.Base(), fbp.Success, "run-1"); got.Subject != want {
		t.Fatalf("subject %q, want %q", got.Subject, want)
	}

	// A Run ID that is no valid subject token is rejected under a new Run ID.
	msg = nats.NewMsg(InportSubject(s.Base()))
	msg.Data = []byte(`"hello"`)
	msg.Header.Set(RunIDHeader, "run.1")
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
	got = nextMsg(t, failure)
	var rejected fbp.ErrorData
	if err := json.Unmarshal(got.Data, &rejected); err != nil || !strings.Contains(rejected.Message, "run ID") {
		t.Fatalf("got %s, %v", got.Data, err)
	}
	if id := got.Header.Get(RunIDHeader); id == "" || id == "run.1" || got.Subject != OutportSubject(s.Base(), fbp.Error, id) {
		t.Fatalf("subject %q, run ID %q", got.Subject, id)
	}

	// Without a Run ID a new one is created.
	if err := nc.Publish(InportSubject(s.Base()), []byte(`"x"`)); err != nil {
		t.Fatal(err)
	}
	if got := nextMsg(t, success); got.Header.Get(RunIDHeader) == "" {
		t.Fatal("no run ID assigned")
	}

	// Brick errors and invalid JSON are published on the Error subject.
	for _, data := range []string{`42`, `not json`} {
		if err := nc.Publish(InportSubject(s.Base()), []byte(data)); err != nil {
			t.Fatal(err)
		}
		got := nextMsg(t, failure)
		var e fbp.ErrorData
		if err := json.Unmarshal(got.Data, &e); err != nil || e.Message == "" {
			t.Fatalf("input %s: got %+v, %v", data, e, err)
		}
		if got.Header.Get(RunIDHeader) == "" {
			t.Fatalf("input %s: no run ID", data)
		}
	}
}

func TestQueueGroup(t *testing.T) {
	nc := connect(t)
	net := upperNetwork(t)
	for range 2 {
		s, err := Start(context.Background(), nc, "gofbp/test", net)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Stop() })
	}
	success, _ := nc.SubscribeSync(OutportSubject("fbp.gofbp.test.upper", fbp.Success, "*"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	const n = 20
	for range n {
		if err := nc.Publish(InportSubject("fbp.gofbp.test.upper"), []byte(`"a"`)); err != nil {
			t.Fatal(err)
		}
	}
	for range n {
		nextMsg(t, success)
	}
	// Each Packet is processed by only one of the two Services.
	if msg, err := success.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("unexpected extra message %s", msg.Data)
	}
}

// A Service whose ctx is done leaves the queue group, so the other Service
// gets every Packet instead of half of them being discarded.
func TestContextDoneUnsubscribes(t *testing.T) {
	nc := connect(t)
	net := upperNetwork(t)
	ctx, cancel := context.WithCancel(context.Background())
	done, err := Start(ctx, nc, "gofbp/test", net)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { done.Stop() })
	live, err := Start(context.Background(), nc, "gofbp/test", net)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { live.Stop() })

	cancel()
	deadline := time.Now().Add(timeout)
	for done.inports[0].sub.IsValid() || done.obsSub.IsValid() {
		if time.Now().After(deadline) {
			t.Fatal("service did not unsubscribe after ctx was done")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := nc.Request("$SRV.PING.fbp_gofbp_test_upper_366cd53d."+done.ID(), nil, 100*time.Millisecond); err == nil {
		t.Fatal("service answered on $SRV after ctx was done")
	}

	success, _ := nc.SubscribeSync(OutportSubject(live.Base(), fbp.Success, "*"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	const n = 20
	for range n {
		if err := nc.Publish(InportSubject(live.Base()), []byte(`"a"`)); err != nil {
			t.Fatal(err)
		}
	}
	for range n {
		nextMsg(t, success)
	}
	// Stop after the ctx is done still works and reports no error.
	if err := done.Stop(); err != nil {
		t.Fatal(err)
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

func TestSlowConsumerIsLogged(t *testing.T) {
	nc := connect(t)
	release := make(chan struct{})
	block := &fbp.Brick{Name: "Block", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}}
	n := &fbp.Network{Name: "slow"}
	if err := errors.Join(n.Add("block", block), n.ConnectIn("block")); err != nil {
		t.Fatal(err)
	}

	logs := make(chan slog.Record, 100)
	s, err := Start(context.Background(), nc, "gofbp/test", n,
		WithLogger(slog.New(recordHandler{logs})),
		WithPendingLimits(1, -1),
		WithNetworkOptions(fbp.WithBufferSize(1)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	// One message in the Brick, one in the buffer, one blocked in Send, one
	// pending in the client; the rest is dropped.
	for range 20 {
		if err := nc.Publish(InportSubject(s.Base()), []byte(`1`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	close(release)

	deadline := time.After(timeout)
	for {
		select {
		case r := <-logs:
			if strings.Contains(r.Message, "slow consumer") {
				return
			}
		case <-deadline:
			t.Fatal("dropped messages were not logged")
		}
	}
}

func TestStartRejectsInvalidOutport(t *testing.T) {
	nc := connect(t)
	for _, name := range []string{"not found", "a.b", "*", ">"} {
		n := upperNetwork(t)
		n.Outports = []string{name}
		if s, err := Start(context.Background(), nc, "gofbp/test", n); err == nil {
			s.Stop()
			t.Errorf("outport %q accepted", name)
		}
	}
}

func TestSetObserver(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	events := make(chan fbp.Event, 10)
	s.SetObserver(func(e fbp.Event) { events <- e })

	if err := nc.Publish(InportSubject(s.Base()), []byte(`"hi"`)); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-events:
		if e.Kind != fbp.Sent || e.Network != "upper" {
			t.Fatalf("got %+v", e)
		}
	case <-time.After(timeout):
		t.Fatal("no event")
	}
}

func TestObserveSubject(t *testing.T) {
	nc := connect(t)
	net := upperNetwork(t)
	for range 2 {
		s, err := Start(context.Background(), nc, "gofbp/test", net)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Stop() })
	}
	base := "fbp.gofbp.test.upper"
	events, _ := nc.SubscribeSync(TraceSubject(base))
	success, _ := nc.SubscribeSync(OutportSubject(base, fbp.Success, "*"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// send publishes one Packet and waits for its result, so all its Events
	// except possibly the last Processed have been published.
	send := func(runID string) {
		t.Helper()
		msg := nats.NewMsg(InportSubject(base))
		msg.Data = []byte(`"hi"`)
		msg.Header.Set(RunIDHeader, runID)
		if err := nc.PublishMsg(msg); err != nil {
			t.Fatal(err)
		}
		nextMsg(t, success)
	}
	observe := func(on string) {
		t.Helper()
		if err := nc.Publish(ObserveSubject(base), []byte(on)); err != nil {
			t.Fatal(err)
		}
		// A round trip makes sure both Services got the message before the
		// next Packet; the observe subscription runs in its own goroutine,
		// so give it a moment to switch.
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	send("off-1")
	observe("true")
	services := map[string]bool{}
	// The queue group picks a Service at random; 20 Packets reach both.
	for i := range 20 {
		runID := fmt.Sprintf("on-%d", i)
		send(runID)
		var e EventData
		if err := json.Unmarshal(nextMsg(t, events).Data, &e); err != nil {
			t.Fatal(err)
		}
		if e.Kind != "Sent" || e.RunID != runID || e.Network != "upper" || e.PacketID == 0 || string(e.Data) != `"hi"` {
			t.Fatalf("got %+v", e)
		}
		services[e.Service] = true
		for e.Kind != "Processed" {
			if err := json.Unmarshal(nextMsg(t, events).Data, &e); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(services) != 2 {
		t.Fatalf("events from %d services, want 2", len(services))
	}

	observe("false")
	send("off-2")
	if msg, err := events.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("event after switching off: %s", msg.Data)
	}
}
