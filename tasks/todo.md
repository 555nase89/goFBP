# Open tasks

## Done
- `pkg/fbp` core: Packet, Inport/Outport, Brick (ProcessFunc, Concurrency), Network, Instance
  (Start/Send/Stop), Run ID, Fan-out with data copy (also from a Network Inport), backpressure,
  cycle check, logging of unconnected Error packets, ErrorData.
- Nested Networks, flattened at Start; self-containment rejected. Brick names need not be unique
  (rule dropped; Nodes are identified by node name).
- `pkg/service`: queue subscription on `ip.in`, publish on `op.<outport>`, Run ID header,
  package path mapping (`/` -> `.`, `.` -> `_`).
- Example `examples/text` with `examples/text/cmd/textservice`.
- `docs/concept.md`.
- Review fixes: Packet data must be valid JSON (`ErrInvalidJSON` from Send/Emit); Send and Emit
  copy the data; unnamed Outports rejected, Service checks Outport names as subject tokens; no data
  race on `Service.sub` (handler uses `msg.Sub`); invalid JSON in a Service gets a Run ID
  (`fbp.NewRunID`); `text.Shout` forwards `suffix` errors.
- Observer: `fbp.WithObserver`, Events `Sent`/`Received`/`Processed`/`Emitted`/`Output` with
  Packet ID and parent, example `examples/text/cmd/texttrace`. Switchable at runtime with
  `Instance.SetObserver` / `Service.SetObserver`.
- `Emit` returns `ErrStopped` when the Instance stops, whatever `ctx` the Brick passes
  (before, `Stop` could hang forever).
- Service observing via NATS: `observe` (`true`/`false`, all Services of the Network) switches
  publishing of `EventData` on `trace`.
- Discovery: every Service answers `$SRV.PING/INFO/STATS` (NATS micro protocol, own
  implementation because micro stops on slow consumers), metadata with package, network, outports,
  trace; stats with drops and Packets per Outport; `WithVersion`, `Service.ID`.
- Service unsubscribes (also from `$SRV`) as soon as the `ctx` of `service.Start` is done, instead
  of silently discarding the queue group's messages until `Stop`.
- `ServiceName` appends 8 hex digits of the SHA-256 of the base subject, so ambiguous names like
  `fbp.a_b.c` / `fbp.a.b_c` no longer collide on `$SRV`.
- `Subject` rejects package paths containing `_`, so `github.com/acme` and `github_com/acme` can no
  longer share subjects and a queue group.
- Example `examples/crash`: Network `stats` (Check -> Average) with an ordinary Brick error and a
  runtime panic (divide by zero on `[]`); `examples/crash/cmd/crashlocal` exits on the first Error
  Packet (status 1, or with `-panic` a Go panic with stack trace, status 2).

- Outport subject carries the Run ID: `<base>.op.<outport>.<runid>`; a Run ID from the header that
  is no valid subject token is rejected on the Error subject under a new Run ID (2026-10-27).
- Clients create the Run ID: `service.Call` (subscribe `op.*.<runid>`, flush, publish, first
  result) and the pattern in `docs/service.md` ("Calling a Service") (2026-10-27).
- Service documentation moved from `docs/concept.md` to `docs/service.md` (2026-10-27).
- CLI `cmd/fbpcli` with `pkg/service/cli`: sends one input to a Network Service (`-network
  <packagepath>/<networkname>`, `-input`/stdin, `-string`) and prints the first Packet as
  `<Outport> <data>`; exit status 1 on `Error` (2026-10-29).
- End of a Run: `fbp.WithRunFinished` (Instance counts Packets in flight per Run, reports the number
  of Outputs), Service publishes `DoneData` on `<base>.done.<runid>` (also after rejected input),
  `service.CallAll` collects all Packets of a Run; `fbpcli` prints all of them, `-first` for the
  old behaviour (2026-10-29).
- Several Inports per Network: default Inport `fbp.In` plus `Network.Inports`; `ConnectInport`,
  `ConnectInportTo`, `ConnectTo` (Connection to a named Inport of a nested Network), `SendTo`/`SendAsTo`;
  every Inport must be connected. Service subscribes `<base>.ip.<inport>` per extra Inport (default
  stays `ip.in`), one `$SRV` endpoint per Inport, `CallTo`/`CallAllTo`, `fbpcli -inport`. Observer
  `Sent` Event: `Port` is now the Network Inport name (`In` instead of `Inport`) (2026-10-29).
- Observing state on `$SRV.STATS`: the `observe` endpoint carries `service.ObserveStats`
  (`{"observing":bool}`), so tools like gofbpSRVLS can show whether a Service traces (2026-10-31).
- Logs via NATS: `s.Logger()` wraps the logger from `WithLogger`, publishes records from
  `WithLogLevel` (default Info) as `LogData` on `<base>.log.<serviceid>` and keeps the last
  `WithLogHistory` (default 500) for requests on `<base>.loghistory.<serviceid>`; `$SRV` metadata
  `log`. slog's default handler is replaced by a stderr text handler to avoid the
  `slog.SetDefault` deadlock; examples call `slog.SetDefault(s.Logger())` (2026-10-31).
- Review fixes (2026-10-31): a log attribute JSON cannot encode (e.g. NaN) is turned into text
  before the record enters the Log history, so a concurrent history request no longer races on
  its map (`TestLogUnencodableAttr`); `CallAll` handlers no longer block on a message arriving
  after `CallAll` returned, which leaked a goroutine with a ctx without deadline
  (`TestCallAllLateOutput`).

- Checked against nats-server v2.15.2 and natscli v0.5.1 with two `textservice` instances (2026-10-26):
  `nats micro ls/info/stats/ping` show both instances with metadata and stats, the queue group
  splits the load, Success/Empty/Error outports, Run ID header, invalid JSON, `observe`/`trace` and
  shutdown on SIGTERM (instance disappears from `$SRV`) work.
- Module path is now `github.com/555nase89/goFBP`, so the library can be fetched with `go get` (2026-11-06).
- Performance tests: `pkg/fbp/bench_test.go` (chain latency/throughput, buffer size, payload size,
  Fan-out, Concurrency I/O and CPU, Run tracking, Observer, nested Networks, Start) and
  `pkg/service/bench_test.go` (Call, CallAll, parallel Calls, publish throughput over an embedded
  nats-server); baseline numbers in `docs/performance.md`. `connect` and `upperNetwork` in the
  service tests take `testing.TB` (2026-11-07).
- `service.Call` subscribes asynchronously with a 1-entry channel instead of `SubscribeSync`, which
  allocated a 64k-entry channel (512 KB) per call: sequential Call ~930 -> ~470 µs, parallel Calls
  ~355 -> ~160 µs, 537 KB -> 8 KB per call (2026-11-07).

## Open
- `BenchmarkCallParallel` hung once until its 1-minute ctx expired (one Call got no result) in the
  first full run on 2026-11-07; not reproduced in 27 further runs nor in 24 000 parallel Calls.
  Watch for a lost result under parallel Calls.
- `TestSetObserverWhileRunning` (pkg/fbp) is flaky under `-race`: about 1 in 20 runs reports an
  unexpected extra `Processed` Event right after switching the Observer (seen 2026-10-31).
- `nats micro stats` always shows Processing Time 0s: the Service does not measure it. Decide whether
  to report it (e.g. time from receive to the last Output of a Run) or leave it out.
- Run tracking (`fbp.WithRunFinished`, always on in a Service) uses one mutex per Instance: two
  lock/unlock pairs per hop. Not measurable on 2 cores (5-Brick chain, ~7–13 µs per Run either way),
  but it may become a bottleneck with many cores, high Concurrency and very cheap Bricks. If so,
  shard the `runs` map by Run ID across several mutexes.
