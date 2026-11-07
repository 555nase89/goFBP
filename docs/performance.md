# Performance

Benchmarks live next to the code they measure:

- `pkg/fbp/bench_test.go`: an Instance in-process (no NATS).
- `pkg/service/bench_test.go`: a Service end to end over an embedded nats-server.

```sh
/snap/go/current/bin/go test ./pkg/fbp ./pkg/service -run '^$' -bench . -benchmem
```

Use `-bench <regexp>` for a single benchmark and `-count 5` with `benchstat` to compare
two versions.

"Latency" benchmarks send one Packet and wait for its Output before sending the next one.
"Throughput" benchmarks keep the Network busy and wait only for the last Output. All Bricks
used are cheap pass-through Bricks unless stated otherwise, so the numbers show the cost of
goFBP itself.

## Baseline (2026-11-07)

AMD Ryzen 7 1700X (8 cores), Linux, Go 1.27.3, `-benchtime 0.3s`. Times are per Run.

### pkg/fbp

| Benchmark | Result | Notes |
|-----------|--------|-------|
| ChainLatency, 1 / 5 / 20 Bricks | 6.1 / 16.5 / 50 µs | about 2.3 µs per hop |
| ChainThroughput, 1 / 5 / 20 Bricks | 3.5 / 10.9 / 57 µs | about 10 allocations per hop |
| BufferSize 1 / 10 / 100 / 1000 (5 Bricks) | 12.3 / 11.1 / 10.9 / 10.6 µs | buffer size hardly matters |
| PayloadSize 16 B / 1 KB / 64 KB / 1 MB (5 Bricks) | 10.5 µs / 17 µs / 174 µs / 1.3 ms | 6 copies per Run (Send + 5 Emits) |
| FanOut width 1 / 4 / 16 (1 KB) | 8.9 / 19.9 / 63 µs | |
| Concurrency 1 / 2 / 4 / 8 / 32, Brick sleeps 1 ms | 1.74 ms / 735 / 385 / 193 / 58 µs | scales with Concurrency |
| ConcurrencyCPU 1 / 2 / 4 / 8, CPU-bound Brick | 259 / 131 / 74 / 60 µs | about 4.3× with 8 copies |
| RunTracking (`WithRunFinished`), off / on, latency | 15.4 / 17.1 µs | |
| RunTracking, off / on, throughput | 12.2 / 11.5 µs | difference within noise |
| RunTrackingParallel, off / on (Concurrency 8) | 14.2 / 15.5 µs | about 9 % from the `runs` mutex |
| Observer off / on (no-op Observer) | 10.1 / 12.0 µs | |
| Nested, flat / 3 levels (8 Bricks) | 24.6 / 26.0 µs | nested Networks are flattened at Start |
| Start + Stop, 20 Bricks | 55 µs | |

### pkg/service

| Benchmark | Result | Notes |
|-----------|--------|-------|
| Call, sequential | 750 µs, 537 KB/op | see below |
| CallAll, sequential | 400 µs, 11 KB/op | waits for the Done message as well |
| CallParallel (8 goroutines, one connection) | 340 µs | |
| PublishThroughput (publish without waiting) | 13 µs per message | about 75 000 messages/s |

`Call` is slower than `CallAll` although it does less. A likely cause: `nats.Conn.SubscribeSync`
allocates a channel of `Options.SubChanLen` entries (default `nats.DefaultMaxChanLen`, 64k
pointers = 512 KB) for every call. See the open item in `tasks/todo.md`.
