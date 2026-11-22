package service

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// connectJS starts an embedded nats-server with JetStream and connects to it.
func connectJS(t testing.TB) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: t.TempDir()})
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
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return nc, js
}

// gateNetwork returns a Network "upper" whose Brick reports every call on
// started and then waits until release is closed or the Instance stops.
func gateNetwork(t *testing.T, started chan<- string, release <-chan struct{}) *fbp.Network {
	t.Helper()
	gate := &fbp.Brick{Name: "Gate", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		started <- p.RunID
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return out.Emit(ctx, fbp.Success, p.Data)
	}}
	n := &fbp.Network{Name: "upper"}
	for _, err := range []error{
		n.Add("gate", gate),
		n.ConnectIn("gate"),
		n.ConnectOut("gate", fbp.Success, fbp.Success),
		n.ConnectOut("gate", fbp.Error, fbp.Error),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	return n
}

func startJS(t *testing.T, nc *nats.Conn, net *fbp.Network, c JetStreamConfig) *Service {
	t.Helper()
	s, err := Start(context.Background(), nc, "gofbp/test", net, WithJetStream(c))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	return s
}

// eventually fails t unless cond becomes true within timeout.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// streamEmpty reports whether the work queue stream holds no message any
// more, i.e. all were acknowledged or terminated.
func streamEmpty(t *testing.T, js jetstream.JetStream, name string) func() bool {
	return func() bool {
		st, err := js.Stream(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := st.Info(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return info.State.Msgs == 0
	}
}

func publishRun(t *testing.T, nc *nats.Conn, subject, runID string, data []byte) {
	t.Helper()
	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header.Set(RunIDHeader, runID)
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
}

func TestJetStreamCall(t *testing.T) {
	nc, js := connectJS(t)
	s := startJS(t, nc, upperNetwork(t), JetStreamConfig{})

	st, err := js.Stream(context.Background(), StreamName(s.Base()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg := st.CachedInfo().Config; cfg.Retention != jetstream.WorkQueuePolicy || len(cfg.Subjects) != 1 || cfg.Subjects[0] != s.Base()+".ip.>" {
		t.Fatalf("stream config %+v", cfg)
	}
	cons, err := js.Consumer(context.Background(), StreamName(s.Base()), ConsumerName(s.Base(), fbp.In))
	if err != nil {
		t.Fatal(err)
	}
	if cfg := cons.CachedInfo().Config; cfg.AckWait != DefaultAckWait || cfg.MaxDeliver != DefaultMaxDeliver || cfg.FilterSubject != InportSubject(s.Base()) {
		t.Fatalf("consumer config %+v", cfg)
	}

	var got []string
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := CallAll(ctx, nc, s.Base(), []byte(`"hello"`), func(outport string, p fbp.Packet) {
		got = append(got, outport+" "+string(p.Data))
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != `Success "HELLO"` {
		t.Fatalf("got %q", got)
	}
	eventually(t, "acknowledgement", streamEmpty(t, js, StreamName(s.Base())))

	info := s.disc.info()
	if info.Metadata["stream"] != StreamName(s.Base()) || info.Endpoints[0].QueueGroup != "" ||
		info.Endpoints[0].Metadata["consumer"] != ConsumerName(s.Base(), fbp.In) {
		t.Fatalf("info %+v", info)
	}
}

// A message sent while no Service runs is stored and processed by the next
// Service.
func TestJetStreamKeepsMessages(t *testing.T) {
	nc, _ := connectJS(t)
	first, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t), WithJetStream(JetStreamConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	base := first.Base()
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	success, _ := nc.SubscribeSync(OutportSubject(base, fbp.Success, "run-1"))
	publishRun(t, nc, InportSubject(base), "run-1", []byte(`"later"`))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	startJS(t, nc, upperNetwork(t), JetStreamConfig{})
	if got := nextMsg(t, success); string(got.Data) != `"LATER"` {
		t.Fatalf("got %s", got.Data)
	}
}

// A message whose Run did not finish when its Service stopped is delivered
// again, to another Service.
func TestJetStreamRedeliversAfterStop(t *testing.T) {
	nc, js := connectJS(t)
	started := make(chan string, 1)
	blocked, err := Start(context.Background(), nc, "gofbp/test", gateNetwork(t, started, nil), WithJetStream(JetStreamConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	success, _ := nc.SubscribeSync(OutportSubject(blocked.Base(), fbp.Success, "run-1"))
	publishRun(t, nc, InportSubject(blocked.Base()), "run-1", []byte(`"again"`))
	select {
	case <-started:
	case <-time.After(timeout):
		t.Fatal("blocked Service got no message")
	}
	if err := blocked.Stop(); err != nil {
		t.Fatal(err)
	}

	s := startJS(t, nc, upperNetwork(t), JetStreamConfig{})
	if got := nextMsg(t, success); string(got.Data) != `"AGAIN"` {
		t.Fatalf("got %s", got.Data)
	}
	eventually(t, "acknowledgement", streamEmpty(t, js, StreamName(s.Base())))
	s.stats.mu.Lock()
	redelivered := s.stats.inports[fbp.In].redelivered
	s.stats.mu.Unlock()
	if redelivered != 1 {
		t.Fatalf("redelivered %d, want 1", redelivered)
	}
}

// A message is acknowledged only when its Run is finished, also when a
// second message joins the Run.
func TestJetStreamAckAtRunEnd(t *testing.T) {
	nc, js := connectJS(t)
	started := make(chan string, 2)
	release := make(chan struct{})
	s := startJS(t, nc, gateNetwork(t, started, release), JetStreamConfig{})
	done, _ := nc.SubscribeSync(DoneSubject(s.Base(), "run-1"))
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`1`))
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`2`))
	<-started
	cons, err := js.Consumer(context.Background(), StreamName(s.Base()), ConsumerName(s.Base(), fbp.In))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "both messages delivered", func() bool {
		info, err := cons.Info(context.Background())
		return err == nil && info.NumAckPending == 2
	})
	time.Sleep(50 * time.Millisecond)
	if info, _ := cons.Info(context.Background()); info.NumAckPending != 2 {
		t.Fatalf("acknowledged before the end of the Run: %d pending", info.NumAckPending)
	}

	close(release)
	var d DoneData
	if err := json.Unmarshal(nextMsg(t, done).Data, &d); err != nil || d.Outputs != 2 {
		t.Fatalf("done %+v, %v", d, err)
	}
	eventually(t, "acknowledgement", streamEmpty(t, js, StreamName(s.Base())))
}

// While a Run takes longer than AckWait, the Service reports its message in
// progress, so it is not delivered again.
func TestJetStreamInProgress(t *testing.T) {
	nc, js := connectJS(t)
	var calls atomic.Int32
	slow := &fbp.Brick{Name: "Slow", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		calls.Add(1)
		time.Sleep(700 * time.Millisecond)
		return out.Emit(ctx, fbp.Success, p.Data)
	}}
	n := &fbp.Network{Name: "upper"}
	for _, err := range []error{
		n.Add("slow", slow), n.ConnectIn("slow"),
		n.ConnectOut("slow", fbp.Success, fbp.Success), n.ConnectOut("slow", fbp.Error, fbp.Error),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	s := startJS(t, nc, n, JetStreamConfig{AckWait: 200 * time.Millisecond})
	done, _ := nc.SubscribeSync(DoneSubject(s.Base(), "run-1"))
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`1`))
	nextMsg(t, done)
	eventually(t, "acknowledgement", streamEmpty(t, js, StreamName(s.Base())))
	time.Sleep(300 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("Brick called %d times, want 1", n)
	}
}

// A rejected message is answered on the Error subject and terminated, so it
// is not delivered again.
func TestJetStreamRejectTerminates(t *testing.T) {
	nc, js := connectJS(t)
	s := startJS(t, nc, upperNetwork(t), JetStreamConfig{})
	failure, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Error, "run-1"))
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`not json`))
	if got := nextMsg(t, failure); !json.Valid(got.Data) {
		t.Fatalf("got %s", got.Data)
	}
	cons, err := js.Consumer(context.Background(), StreamName(s.Base()), ConsumerName(s.Base(), fbp.In))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "termination", func() bool {
		info, err := cons.Info(context.Background())
		return err == nil && info.NumAckPending == 0 && info.NumPending == 0
	})
}

// Several Services of the same Network share the consumer; each message is
// processed once.
func TestJetStreamShared(t *testing.T) {
	nc, js := connectJS(t)
	a := startJS(t, nc, upperNetwork(t), JetStreamConfig{})
	b := startJS(t, nc, upperNetwork(t), JetStreamConfig{})
	sub, _ := nc.SubscribeSync(OutportSubject(a.Base(), fbp.Success, "*"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := range n {
		publishRun(t, nc, InportSubject(a.Base()), fbp.NewRunID(), []byte(`"x`+string(rune('a'+i%26))+`"`))
	}
	for range n {
		nextMsg(t, sub)
	}
	if msg, err := sub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("extra result %s", msg.Data)
	}
	eventually(t, "acknowledgement", streamEmpty(t, js, StreamName(a.Base())))
	count := func(s *Service) int {
		s.stats.mu.Lock()
		defer s.stats.mu.Unlock()
		return s.stats.inports[fbp.In].received
	}
	if count(a)+count(b) != n {
		t.Fatalf("received %d + %d, want %d", count(a), count(b), n)
	}
}

// When the ctx given to Start is done, the messages of Runs going on are
// given back too.
func TestJetStreamRedeliversAfterCtxDone(t *testing.T) {
	nc, _ := connectJS(t)
	started := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	blocked, err := Start(ctx, nc, "gofbp/test", gateNetwork(t, started, nil), WithJetStream(JetStreamConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { blocked.Stop() })
	success, _ := nc.SubscribeSync(OutportSubject(blocked.Base(), fbp.Success, "run-1"))
	publishRun(t, nc, InportSubject(blocked.Base()), "run-1", []byte(`"again"`))
	select {
	case <-started:
	case <-time.After(timeout):
		t.Fatal("blocked Service got no message")
	}
	cancel()

	startJS(t, nc, upperNetwork(t), JetStreamConfig{})
	if got := nextMsg(t, success); string(got.Data) != `"AGAIN"` {
		t.Fatalf("got %s", got.Data)
	}
}

// brickNetwork returns a Network "upper" with the single Brick b, its
// Success and Error Outports connected to the Network's.
func brickNetwork(t *testing.T, b *fbp.Brick) *fbp.Network {
	t.Helper()
	n := &fbp.Network{Name: "upper"}
	for _, err := range []error{
		n.Add("b", b), n.ConnectIn("b"),
		n.ConnectOut("b", fbp.Success, fbp.Success), n.ConnectOut("b", fbp.Error, fbp.Error),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	return n
}

// failingNetwork returns a Network whose Brick fails for the first fails
// calls and then echoes its input; calls counts all calls.
func failingNetwork(t *testing.T, fails int32, calls *atomic.Int32) *fbp.Network {
	return brickNetwork(t, &fbp.Brick{Name: "Flaky", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		if calls.Add(1) <= fails {
			return errors.New("not yet")
		}
		return out.Emit(ctx, fbp.Success, p.Data)
	}})
}

func createStream(t *testing.T, js jetstream.JetStream, name string, subjects ...string) jetstream.Stream {
	t.Helper()
	st, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: name, Subjects: subjects})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func streamMsgs(t *testing.T, st jetstream.Stream) uint64 {
	t.Helper()
	info, err := st.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

// By default a Run that ends with an Error Packet is acknowledged.
func TestJetStreamErrorAck(t *testing.T) {
	nc, js := connectJS(t)
	var calls atomic.Int32
	s := startJS(t, nc, failingNetwork(t, 1, &calls), JetStreamConfig{})
	failure, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Error, "run-1"))
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`1`))
	nextMsg(t, failure)
	eventually(t, "acknowledgement", streamEmpty(t, js, StreamName(s.Base())))
	time.Sleep(100 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("Brick called %d times, want 1", n)
	}
}

// With ErrorNak a failed Run's message is delivered again until its Run
// succeeds.
func TestJetStreamErrorNakRetries(t *testing.T) {
	nc, js := connectJS(t)
	dlq := createStream(t, js, "DLQ", "dlq.>")
	var calls atomic.Int32
	s := startJS(t, nc, failingNetwork(t, 2, &calls), JetStreamConfig{OnError: ErrorNak, DeadLetter: "dlq.upper"})
	success, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Success, "run-1"))
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`"x"`))
	if got := nextMsg(t, success); string(got.Data) != `"x"` {
		t.Fatalf("got %s", got.Data)
	}
	eventually(t, "acknowledgement", streamEmpty(t, js, StreamName(s.Base())))
	if n := calls.Load(); n != 3 {
		t.Fatalf("Brick called %d times, want 3", n)
	}
	if n := streamMsgs(t, dlq); n != 0 {
		t.Fatalf("%d dead letters, want 0", n)
	}
}

// With ErrorNak a message whose Run fails on its last delivery goes to the
// Dead letter subject and is terminated.
func TestJetStreamErrorNakDeadLetter(t *testing.T) {
	nc, js := connectJS(t)
	dlq := createStream(t, js, "DLQ", "dlq.>")
	var calls atomic.Int32
	s := startJS(t, nc, failingNetwork(t, 100, &calls), JetStreamConfig{
		OnError: ErrorNak, MaxDeliver: 3, DeadLetter: "dlq.upper"})
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`{"a":1}`))
	eventually(t, "dead letter", func() bool { return streamMsgs(t, dlq) == 1 })
	eventually(t, "termination", streamEmpty(t, js, StreamName(s.Base())))
	if n := calls.Load(); n != 3 {
		t.Fatalf("Brick called %d times, want 3", n)
	}
	m, err := dlq.GetMsg(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	var ed fbp.ErrorData
	if err := json.Unmarshal([]byte(m.Header.Get(ErrorHeader)), &ed); err != nil || ed.Message != "not yet" || ed.Input != nil {
		t.Fatalf("error header %q: %+v, %v", m.Header.Get(ErrorHeader), ed, err)
	}
	if string(m.Data) != `{"a":1}` || m.Header.Get(RunIDHeader) != "run-1" ||
		m.Header.Get(SubjectHeader) != InportSubject(s.Base()) || m.Header.Get(StreamHeader) != StreamName(s.Base()) ||
		m.Header.Get(SequenceHeader) != "1" || m.Header.Get(DeliveredHeader) != "3" {
		t.Fatalf("dead letter %s %v", m.Data, m.Header)
	}
}

// With ErrorTerm a failed Run's message goes to the Dead letter subject at
// once, also when it has no Run ID and its Run finishes right away.
func TestJetStreamErrorTerm(t *testing.T) {
	nc, js := connectJS(t)
	dlq := createStream(t, js, "DLQ", "dlq.>")
	var calls atomic.Int32
	s := startJS(t, nc, failingNetwork(t, 100, &calls), JetStreamConfig{OnError: ErrorTerm, DeadLetter: "dlq.upper"})
	const n = 20
	for i := range n {
		if err := nc.Publish(InportSubject(s.Base()), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "dead letters", func() bool { return streamMsgs(t, dlq) == n })
	eventually(t, "termination", streamEmpty(t, js, StreamName(s.Base())))
	if got := calls.Load(); got != n {
		t.Fatalf("Brick called %d times, want %d", got, n)
	}
	s.acks.mu.Lock()
	defer s.acks.mu.Unlock()
	if len(s.acks.msgs)+len(s.acks.runs)+len(s.acks.entering)+len(s.acks.errs) != 0 {
		t.Fatalf("acks not empty: %+v", s.acks)
	}
}

// With OnError other than ErrorAck, a rejected message goes to the Dead
// letter subject too.
func TestJetStreamRejectDeadLetter(t *testing.T) {
	nc, js := connectJS(t)
	dlq := createStream(t, js, "DLQ", "dlq.>")
	s := startJS(t, nc, upperNetwork(t), JetStreamConfig{OnError: ErrorNak, DeadLetter: "dlq.upper"})
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`not json`))
	eventually(t, "dead letter", func() bool { return streamMsgs(t, dlq) == 1 })
	eventually(t, "termination", streamEmpty(t, js, StreamName(s.Base())))
}

// An Inport can read any stream through its own filter subject and get the
// messages as Envelopes; the stream is only read.
func TestJetStreamInportSourceEnvelope(t *testing.T) {
	nc, js := connectJS(t)
	events := createStream(t, js, "EVENTS", "events.>")
	echo := &fbp.Brick{Name: "Echo", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		return out.Emit(ctx, fbp.Success, p.Data)
	}}
	s := startJS(t, nc, brickNetwork(t, echo), JetStreamConfig{
		Inports:  map[string]InportSource{fbp.In: {Stream: "EVENTS", FilterSubject: "events.order.>"}},
		Envelope: true,
	})
	success, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Success, "*"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := js.Publish(ctx, "events.customer.7", []byte(`{"skip":true}`)); err != nil {
		t.Fatal(err)
	}
	msg := nats.NewMsg("events.order.42")
	msg.Data = []byte(`{"type":"OrderPlaced"}`)
	msg.Header.Set("Event-Type", "OrderPlaced")
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatal(err)
	}

	var e Envelope
	if err := json.Unmarshal(nextMsg(t, success).Data, &e); err != nil {
		t.Fatal(err)
	}
	if e.Subject != "events.order.42" || e.Stream != "EVENTS" || e.Sequence != 2 || e.Delivered != 1 ||
		e.Time.IsZero() || string(e.Data) != `{"type":"OrderPlaced"}` || e.Header["Event-Type"][0] != "OrderPlaced" {
		t.Fatalf("envelope %+v", e)
	}
	if m, err := success.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("extra result %s", m.Data)
	}
	cons, err := js.Consumer(ctx, "EVENTS", ConsumerName(s.Base(), fbp.In))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "acknowledgement", func() bool {
		info, err := cons.Info(ctx)
		return err == nil && info.NumAckPending == 0 && info.NumPending == 0
	})
	if n := streamMsgs(t, events); n != 2 {
		t.Fatalf("EVENTS holds %d messages, want 2", n)
	}
	if _, err := js.Stream(ctx, StreamName(s.Base())); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("default stream created although no Inport reads it: %v", err)
	}
	info := s.disc.info()
	if info.Endpoints[0].Subject != "events.order.>" || info.Endpoints[0].Metadata["stream"] != "EVENTS" {
		t.Fatalf("endpoint %+v", info.Endpoints[0])
	}
}

// Start refuses a JetStreamConfig it cannot carry out.
func TestJetStreamConfigErrors(t *testing.T) {
	nc, js := connectJS(t)
	createStream(t, js, "EVENTS", "events.>")
	if _, err := js.CreateConsumer(context.Background(), "EVENTS", jetstream.ConsumerConfig{
		Durable: ConsumerName("fbp.gofbp.test.upper", fbp.In), FilterSubject: "events.a.>", AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]JetStreamConfig{
		"missing stream":      {Inports: map[string]InportSource{fbp.In: {Stream: "NOPE"}}},
		"unknown inport":      {Inports: map[string]InportSource{"other": {}}},
		"other filter":        {Inports: map[string]InportSource{fbp.In: {Stream: "EVENTS", FilterSubject: "events.b.>"}}},
		"dead letter unbound": {DeadLetter: "nowhere"},
		"error policy":        {OnError: 7},
	} {
		s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t), WithJetStream(c))
		if err == nil {
			s.Stop()
			t.Errorf("%s: Start succeeded", name)
			continue
		}
		t.Logf("%s: %v", name, err)
	}
}

// A Service without JetStream keeps no state for failed Runs.
func TestCoreErrorKeepsNoState(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	failure, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Error, "run-1"))
	publishRun(t, nc, InportSubject(s.Base()), "run-1", []byte(`42`))
	nextMsg(t, failure)
	if s.acks != nil || s.jsc != nil {
		t.Fatalf("JetStream state without WithJetStream")
	}
}
