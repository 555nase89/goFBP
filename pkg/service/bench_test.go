package service

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go"
)

// Performance tests of a Service end to end over an embedded nats-server.
// Run with
//
//	/snap/go/current/bin/go test ./pkg/service -run '^$' -bench . -benchmem

// benchService starts a Service of the upper Network on its own connection
// and returns a second connection for the client.
func benchService(b *testing.B, opts ...Option) (*nats.Conn, *Service) {
	b.Helper()
	nc := connect(b)
	client, err := nats.Connect(nc.ConnectedUrl())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(client.Close)
	s, err := Start(context.Background(), nc, "gofbp/bench", upperNetwork(b), opts...)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Stop() })
	return client, s
}

// BenchmarkCall measures one Call after another: subscribe, flush, publish
// and wait for the first result.
func BenchmarkCall(b *testing.B) {
	client, s := benchService(b)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := Call(ctx, client, s.Base(), []byte(`"hello"`)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCallAll measures one CallAll after another, which also waits for
// the Done message of the Run.
func BenchmarkCallAll(b *testing.B) {
	client, s := benchService(b)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := CallAll(ctx, client, s.Base(), []byte(`"hello"`), func(string, fbp.Packet) {}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCallParallel runs Calls from many goroutines at once over one
// client connection. Each Call has its own timeout, so a lost result is
// reported with the time it was sent.
func BenchmarkCallParallel(b *testing.B) {
	client, s := benchService(b)
	start := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sent := time.Since(start)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, _, err := Call(ctx, client, s.Base(), []byte(`"hello"`))
			cancel()
			if err != nil {
				b.Errorf("Call sent %v after Start (b.N %d): %v", sent, b.N, err)
				return
			}
		}
	})
}

// BenchmarkPublishThroughput publishes b.N messages on the Inport subject
// without waiting and counts the results on op.Success.*. Pending limits are
// switched off so no message is dropped.
func BenchmarkPublishThroughput(b *testing.B) {
	client, s := benchService(b, WithPendingLimits(-1, -1))
	var got atomic.Int64
	done := make(chan struct{})
	want := int64(b.N)
	sub, err := client.Subscribe(OutportSubject(s.Base(), fbp.Success, "*"), func(*nats.Msg) {
		if got.Add(1) == want {
			close(done)
		}
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := sub.SetPendingLimits(-1, -1); err != nil {
		b.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		msg := nats.NewMsg(InportSubject(s.Base()))
		msg.Data = []byte(`"hello"`)
		msg.Header.Set(RunIDHeader, fmt.Sprintf("run-%d", i))
		if err := client.PublishMsg(msg); err != nil {
			b.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		b.Fatalf("got %d of %d results", got.Load(), want)
	}
}
