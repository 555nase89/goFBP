package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a logger.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func nextLog(t *testing.T, sub *nats.Subscription) LogData {
	t.Helper()
	var d LogData
	if err := json.Unmarshal(nextMsg(t, sub).Data, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestLogSubject(t *testing.T) {
	nc := connect(t)
	var out syncBuffer
	inner := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t), WithLogger(inner))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	sub, _ := nc.SubscribeSync(LogSubject(s.Base(), "*"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	l := s.Logger().With("brick", "upper").WithGroup("req")
	l.Debug("not published")
	l.Info("hello", "n", 3, "err", errors.New("boom"), slog.Group("http", "status", 200), "wait", time.Second)
	d := nextLog(t, sub)
	if d.Service != s.ID() || d.Level != "INFO" || d.Message != "hello" || d.Seq == 0 || d.Time.IsZero() {
		t.Fatalf("got %+v", d)
	}
	want := map[string]any{"brick": "upper", "req.n": 3.0, "req.err": "boom", "req.http.status": 200.0, "req.wait": "1s"}
	for k, v := range want {
		if d.Attrs[k] != v {
			t.Errorf("attr %s = %v (%T), want %v", k, d.Attrs[k], d.Attrs[k], v)
		}
	}
	if len(d.Attrs) != len(want) {
		t.Errorf("attrs %v", d.Attrs)
	}
	// Debug goes to the inner handler only.
	if !strings.Contains(out.String(), "not published") || !strings.Contains(out.String(), "hello") {
		t.Fatalf("inner handler got %q", out.String())
	}
	if msg, err := sub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("unexpected record %s", msg.Data)
	}
}

func TestLogLevel(t *testing.T) {
	nc := connect(t)
	inner := slog.New(slog.NewTextHandler(&syncBuffer{}, &slog.HandlerOptions{Level: slog.LevelError}))
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t), WithLogger(inner), WithLogLevel(slog.LevelDebug))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	sub, _ := nc.SubscribeSync(LogSubject(s.Base(), s.ID()))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	// Published although the inner handler drops Debug.
	s.Logger().Debug("debug")
	if d := nextLog(t, sub); d.Level != "DEBUG" || d.Message != "debug" {
		t.Fatalf("got %+v", d)
	}
}

func TestLogHistory(t *testing.T) {
	nc := connect(t)
	inner := slog.New(slog.NewTextHandler(&syncBuffer{}, nil))
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t), WithLogger(inner), WithLogHistory(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	for _, m := range []string{"a", "b", "c", "d"} {
		s.Logger().Info(m)
	}
	var history []LogData
	request(t, nc, LogHistorySubject(s.Base(), s.ID()), &history)
	if len(history) != 3 {
		t.Fatalf("got %+v", history)
	}
	for i, m := range []string{"b", "c", "d"} {
		if history[i].Message != m || history[i].Service != s.ID() {
			t.Fatalf("history[%d] = %+v, want %s", i, history[i], m)
		}
		if i > 0 && history[i].Seq != history[i-1].Seq+1 {
			t.Fatalf("seq not consecutive: %+v", history)
		}
	}
	s.Stop()
	if _, err := nc.Request(LogHistorySubject(s.Base(), s.ID()), nil, 100*time.Millisecond); err == nil {
		t.Fatal("stopped service answered")
	}
}

func TestLogStartRecords(t *testing.T) {
	nc := connect(t)
	inner := slog.New(slog.NewTextHandler(&syncBuffer{}, nil))
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t), WithLogger(inner))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	sub, _ := nc.SubscribeSync(LogSubject(s.Base(), s.ID()))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	// Records of the Service itself are published, here from handleObserve.
	if err := nc.Publish(ObserveSubject(s.Base()), []byte(`true`)); err != nil {
		t.Fatal(err)
	}
	if d := nextLog(t, sub); d.Message != "service: observing switched" || d.Attrs["on"] != true {
		t.Fatalf("got %+v", d)
	}
}

func TestLogSetDefault(t *testing.T) {
	nc := connect(t)
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	// Without WithLogger: the case that would deadlock, see innerHandler.
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	sub, _ := nc.SubscribeSync(LogSubject(s.Base(), s.ID()))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	// slog.SetDefault redirects the log package; restore it afterwards.
	flags, writer := log.Flags(), log.Writer()
	t.Cleanup(func() { log.SetFlags(flags); log.SetOutput(writer) })
	slog.SetDefault(s.Logger())
	done := make(chan struct{})
	go func() {
		log.Print("via log")
		slog.Info("via slog")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("logging blocked")
	}
	for _, want := range []string{"via log", "via slog"} {
		if d := nextLog(t, sub); d.Message != want {
			t.Fatalf("got %+v, want %s", d, want)
		}
	}
}

// TestLogUnencodableAttr logs a value JSON cannot encode while the Log
// history is requested; the record must be published as text, without a
// data race on the stored record.
func TestLogUnencodableAttr(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t), WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	sub, _ := nc.SubscribeSync(LogSubject(s.Base(), s.ID()))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			s.Logger().Info("nan", "v", math.NaN(), "n", 1)
		}
	})
	wg.Go(func() {
		for range 20 {
			var history []LogData
			request(t, nc, LogHistorySubject(s.Base(), s.ID()), &history)
		}
	})
	wg.Wait()
	if d := nextLog(t, sub); d.Attrs["v"] != "NaN" || d.Attrs["n"] != "1" {
		t.Fatalf("got %+v", d.Attrs)
	}
}
