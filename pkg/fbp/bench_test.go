package fbp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// Performance tests. Run with
//
//	/snap/go/current/bin/go test ./pkg/fbp -run '^$' -bench . -benchmem
//
// "Latency" benchmarks send one Packet and wait for its Output before the
// next one (time per Run end to end). "Throughput" benchmarks keep the
// Network busy and wait only for the last Output (time per Packet while the
// Bricks work in parallel).

// pass emits its input unchanged.
func pass() *Brick {
	return &Brick{Name: "Pass", Process: func(ctx context.Context, p Packet, out Emitter) error {
		return out.Emit(ctx, Success, p.Data)
	}}
}

// chain builds a Network of length Bricks connected one after another.
func chain(tb testing.TB, length int, mk func() *Brick) *Network {
	tb.Helper()
	n := &Network{Name: "chain"}
	prev := ""
	for i := range length {
		name := fmt.Sprintf("b%d", i)
		mustTB(tb, n.Add(name, mk()))
		if prev == "" {
			mustTB(tb, n.ConnectIn(name))
		} else {
			mustTB(tb, n.Connect(prev, Success, name))
		}
		prev = name
	}
	mustTB(tb, n.ConnectOut(prev, Success, Success))
	return n
}

func mustTB(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}

// counter counts Outputs and signals when want of them have arrived.
type counter struct {
	mu   sync.Mutex
	n    int
	want int
	done chan struct{}
}

func (c *counter) reset(want int) {
	c.mu.Lock()
	c.n, c.want, c.done = 0, want, make(chan struct{})
	c.mu.Unlock()
}

func (c *counter) output(context.Context, string, Packet) {
	c.mu.Lock()
	c.n++
	if c.n == c.want {
		close(c.done)
	}
	c.mu.Unlock()
}

func (c *counter) wait(tb testing.TB) {
	tb.Helper()
	select {
	case <-c.done:
	case <-time.After(30 * time.Second):
		tb.Fatal("timeout waiting for outputs")
	}
}

func startTB(tb testing.TB, n *Network, opts ...Option) *Instance {
	tb.Helper()
	inst, err := n.Start(context.Background(), opts...)
	mustTB(tb, err)
	tb.Cleanup(inst.Stop)
	return inst
}

// latency sends b.N Packets one at a time, each after the previous one's
// outputsPerRun Outputs have arrived.
func latency(b *testing.B, n *Network, data json.RawMessage, outputsPerRun int, opts ...Option) {
	c := &counter{}
	inst := startTB(b, n, append(opts, WithOutput(c.output))...)
	ctx := context.Background()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.reset(outputsPerRun)
		if _, err := inst.Send(ctx, Packet{Data: data}); err != nil {
			b.Fatal(err)
		}
		c.wait(b)
	}
}

// throughput sends b.N Packets as fast as the Network accepts them and waits
// for all b.N*outputsPerRun Outputs.
func throughput(b *testing.B, n *Network, data json.RawMessage, outputsPerRun int, opts ...Option) {
	c := &counter{}
	inst := startTB(b, n, append(opts, WithOutput(c.output))...)
	ctx := context.Background()
	c.reset(b.N * outputsPerRun)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := inst.Send(ctx, Packet{Data: data}); err != nil {
			b.Fatal(err)
		}
	}
	c.wait(b)
}

// payload returns a JSON string of about size bytes.
func payload(size int) json.RawMessage {
	return str(strings.Repeat("x", max(size-2, 0)))
}

func BenchmarkChainLatency(b *testing.B) {
	for _, length := range []int{1, 5, 20} {
		b.Run(fmt.Sprintf("bricks=%d", length), func(b *testing.B) {
			latency(b, chain(b, length, pass), null, 1)
		})
	}
}

func BenchmarkChainThroughput(b *testing.B) {
	for _, length := range []int{1, 5, 20} {
		b.Run(fmt.Sprintf("bricks=%d", length), func(b *testing.B) {
			throughput(b, chain(b, length, pass), null, 1)
		})
	}
}

func BenchmarkBufferSize(b *testing.B) {
	for _, size := range []int{1, DefaultBufferSize, 100, 1000} {
		b.Run(fmt.Sprintf("buffer=%d", size), func(b *testing.B) {
			throughput(b, chain(b, 5, pass), null, 1, WithBufferSize(size))
		})
	}
}

func BenchmarkPayloadSize(b *testing.B) {
	for _, size := range []int{16, 1 << 10, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			throughput(b, chain(b, 5, pass), payload(size), 1)
		})
	}
}

// BenchmarkFanOut connects one Brick to width Bricks that each lead to the
// Success Outport, so every Run produces width Outputs.
func BenchmarkFanOut(b *testing.B) {
	for _, width := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("width=%d", width), func(b *testing.B) {
			n := &Network{Name: "fanout"}
			mustTB(b, n.Add("src", pass()))
			mustTB(b, n.ConnectIn("src"))
			for i := range width {
				name := fmt.Sprintf("dst%d", i)
				mustTB(b, n.Add(name, pass()))
				mustTB(b, n.Connect("src", Success, name))
				mustTB(b, n.ConnectOut(name, Success, Success))
			}
			throughput(b, n, payload(1<<10), width)
		})
	}
}

// BenchmarkConcurrency runs a single Brick whose work takes 1 ms of
// waiting (I/O-like) with different Concurrency.
func BenchmarkConcurrency(b *testing.B) {
	for _, conc := range []int{1, 2, 4, 8, 32} {
		b.Run(fmt.Sprintf("concurrency=%d", conc), func(b *testing.B) {
			slow := &Brick{Name: "Slow", Concurrency: conc, Process: func(ctx context.Context, p Packet, out Emitter) error {
				time.Sleep(time.Millisecond)
				return out.Emit(ctx, Success, p.Data)
			}}
			throughput(b, chain(b, 1, func() *Brick { return slow }), null, 1, WithBufferSize(64))
		})
	}
}

// BenchmarkConcurrencyCPU is like BenchmarkConcurrency, but the work is CPU
// bound (JSON decode and encode of a small object, repeated).
func BenchmarkConcurrencyCPU(b *testing.B) {
	data := json.RawMessage(`{"name":"goFBP","values":[1,2,3,4,5,6,7,8,9,10],"nested":{"a":true,"b":"text"}}`)
	for _, conc := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("concurrency=%d", conc), func(b *testing.B) {
			work := &Brick{Name: "Work", Concurrency: conc, Process: func(ctx context.Context, p Packet, out Emitter) error {
				var v map[string]any
				for range 20 {
					if err := json.Unmarshal(p.Data, &v); err != nil {
						return err
					}
					if _, err := json.Marshal(v); err != nil {
						return err
					}
				}
				return out.Emit(ctx, Success, p.Data)
			}}
			throughput(b, chain(b, 1, func() *Brick { return work }), data, 1, WithBufferSize(64))
		})
	}
}

// BenchmarkRunTracking compares a 5-Brick chain with and without
// WithRunFinished (always on in a Service).
func BenchmarkRunTracking(b *testing.B) {
	b.Run("off/latency", func(b *testing.B) { latency(b, chain(b, 5, pass), null, 1) })
	b.Run("on/latency", func(b *testing.B) {
		latency(b, chain(b, 5, pass), null, 1, WithRunFinished(func(string, int) {}))
	})
	b.Run("off/throughput", func(b *testing.B) { throughput(b, chain(b, 5, pass), null, 1) })
	b.Run("on/throughput", func(b *testing.B) {
		throughput(b, chain(b, 5, pass), null, 1, WithRunFinished(func(string, int) {}))
	})
}

// BenchmarkRunTrackingParallel sends from many goroutines, each with its own
// Run IDs, into a chain of Bricks with Concurrency 8, to put pressure on the
// mutex of the Run tracking (see tasks/todo.md).
func BenchmarkRunTrackingParallel(b *testing.B) {
	for _, on := range []bool{false, true} {
		b.Run(fmt.Sprintf("tracking=%t", on), func(b *testing.B) {
			p := pass()
			p.Concurrency = 8
			var opts []Option
			if on {
				opts = append(opts, WithRunFinished(func(string, int) {}))
			}
			c := &counter{}
			inst := startTB(b, chain(b, 5, func() *Brick { return p }), append(opts, WithOutput(c.output), WithBufferSize(64))...)
			c.reset(b.N)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.Background()
				for pb.Next() {
					if _, err := inst.Send(ctx, Packet{Data: null}); err != nil {
						b.Error(err)
						return
					}
				}
			})
			c.wait(b)
		})
	}
}

// BenchmarkObserver compares a 5-Brick chain without and with an Observer
// that does nothing.
func BenchmarkObserver(b *testing.B) {
	b.Run("off", func(b *testing.B) { throughput(b, chain(b, 5, pass), null, 1) })
	b.Run("on", func(b *testing.B) {
		throughput(b, chain(b, 5, pass), null, 1, WithObserver(func(Event) {}))
	})
}

// BenchmarkNested compares a flat 8-Brick chain with the same 8 Bricks in
// three levels of nested Networks; nesting is flattened at Start, so both
// should cost the same.
func BenchmarkNested(b *testing.B) {
	b.Run("flat", func(b *testing.B) { latency(b, chain(b, 8, pass), null, 1) })
	b.Run("nested", func(b *testing.B) {
		inner := chain(b, 2, pass)
		inner.Name = "inner"
		middle := &Network{Name: "middle"}
		mustTB(b, middle.Add("i1", inner))
		mustTB(b, middle.Add("i2", inner))
		mustTB(b, middle.ConnectIn("i1"))
		mustTB(b, middle.Connect("i1", Success, "i2"))
		mustTB(b, middle.ConnectOut("i2", Success, Success))
		outer := &Network{Name: "outer"}
		mustTB(b, outer.Add("m1", middle))
		mustTB(b, outer.Add("m2", middle))
		mustTB(b, outer.ConnectIn("m1"))
		mustTB(b, outer.Connect("m1", Success, "m2"))
		mustTB(b, outer.ConnectOut("m2", Success, Success))
		latency(b, outer, null, 1)
	})
}

// BenchmarkStart measures Start and Stop of an Instance of a 20-Brick chain.
func BenchmarkStart(b *testing.B) {
	n := chain(b, 20, pass)
	b.ReportAllocs()
	for range b.N {
		inst, err := n.Start(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		inst.Stop()
	}
}
