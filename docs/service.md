# goFBP Service

A Service (`pkg/service`) makes one running Instance of a Network reachable via NATS. See
[concept.md](concept.md) for Bricks, Networks and Instances, [service-package.md](service-package.md)
for the types of the package, and [GLOSSARY.md](GLOSSARY.md) for the terms.

## Starting a Service

```go
s, err := service.Start(ctx, nc, "gofbp/examples/text", net,
    service.WithLogger(logger),                          // default slog.Default()
    service.WithPendingLimits(10_000, 64*1024*1024),     // the defaults
    service.WithVersion("1.0.0"),                        // reported on $SRV, default 0.0.0
    service.WithNetworkOptions(fbp.WithBufferSize(10)),  // passed on to Network.Start
)
defer s.Stop()
```

- The package path becomes subject tokens: every `/` separates a token and every `.` inside a path
  element becomes `_`, e.g. `github.com/acme/flows` -> `fbp.github_com.acme.flows.<networkname>`.
  `Start` rejects package paths containing `_`, because e.g. `github_com/acme` would otherwise get
  the same subjects (and queue group) as `github.com/acme`. Network names may contain `_`.
- Subscribes to `fbp.<packagepath>.<networkname>.ip.in` (Network Inport `In`) and to
  `fbp.<packagepath>.<networkname>.ip.<inport>` for every extra Inport in `Network.Inports`, each in
  a queue group named after the base subject, so several Services of the same Network share the
  load and each Packet is processed once. Extra Inport names must be valid subject tokens and must
  not be `in` or `observe`.
- Publishes every Packet leaving a Network Outport on
  `fbp.<packagepath>.<networkname>.op.<outport>.<runid>`, so a caller can subscribe to the results
  of one Run only.
- The Run ID travels in the header `Fbp-Run-Id`. NATS headers are case-sensitive, so the name must
  be written exactly like this; otherwise the Packet gets a new Run ID. Because the Run ID is a
  subject token, a Run ID containing white space, `.`, `*` or `>` is rejected with an `ErrorData`
  Packet on the Error subject under a new Run ID.
- Messages with invalid JSON are answered with an `ErrorData` Packet on the Error subject, with a
  new Run ID if the message had none.
- `Start` rejects a Network whose Outport names are not valid subject tokens (empty, white space,
  `.`, `*`, `>`).
- Publishes `service.DoneData` (`{"outputs":N}`) on `fbp.<packagepath>.<networkname>.done.<runid>`
  once the Run is finished (`fbp.WithRunFinished`), N being the number of Packets the Run published
  on Outport subjects. Rejected messages get it too, with N = 1. The Service sets
  `fbp.WithRunFinished` itself; one passed in `WithNetworkOptions` is replaced.
- Subscribes to `fbp.<packagepath>.<networkname>.observe` without queue group, so a message
  there reaches every Service of the Network. Data `true` switches observing on, `false` switches it
  off; anything else is logged and ignored. Observing is off after `Start` unless
  `fbp.WithObserver` is passed in `WithNetworkOptions`.
- While observing is on, every Event is published as `service.EventData` (JSON) on
  `fbp.<packagepath>.<networkname>.trace`, with the Run ID in the `Fbp-Run-Id` header.
  `service` is a random ID per Service: Packet IDs are counted per Instance, so `packet_id` and
  `parent_id` are only unique together with `service`. `s.SetObserver(fn)` sets a local Observer
  instead; the next message on `observe` replaces it.
- `Stop` unsubscribes (also from `$SRV`), stops the Instance and flushes the connection.
- When the `ctx` given to `service.Start` is done, the Instance stops and the Service unsubscribes
  at once (also from `$SRV`), so the queue group no longer sends it messages. `Stop` must still be
  called.

## Calling a Service

The client creates the Run ID. Only then can it subscribe to the results of its Run before they are
published. The Service creates a Run ID only as a fallback, and the client has no way to learn it in
time. A client therefore takes these steps, in this order:

1. Create a Run ID, e.g. a UUID (`fbp.NewRunID()`). It must be a valid subject token.
2. Subscribe to `<base>.op.*.<runid>` (or to the single Outports it expects).
3. Flush the connection, so the server knows the subscription before the Service can publish.
   Without this, a fast Network may publish its result before the subscription is active.
4. Publish the data on `<base>.ip.in` (or `<base>.ip.<inport>`) with the header
   `Fbp-Run-Id: <runid>`.

`service.Call(ctx, nc, base, data)` does exactly this and returns the first Packet of the Run with
the name of its Outport; `service.CallTo(ctx, nc, base, inport, data)` sends to another Inport.

Networks may publish several Packets per Run (Fan-out to Network Outports, extra Outports). To get
all of them, the client also subscribes to `<base>.done.<runid>` in step 2 and collects until the
Done message has arrived *and* as many Packets as its `outputs` field says. NATS does not order
messages across two subscriptions, so the Done message may arrive before the last Packets.
`service.CallAll(ctx, nc, base, data, fn)` does this and calls `fn` for every Packet.

```go
outport, p, err := service.Call(ctx, nc, "fbp.gofbp.examples.text.pipeline", []byte(`"  hello "`))
// outport == fbp.Success, p.Data == `"HELLO!"`

err = service.CallAll(ctx, nc, base, data, func(outport string, p fbp.Packet) { /* every Packet */ })
```

With the NATS CLI:

```sh
RUN=$(uuidgen)
nats sub "fbp.gofbp.examples.text.pipeline.op.*.$RUN" &
nats sub "fbp.gofbp.examples.text.pipeline.done.$RUN" &
nats pub fbp.gofbp.examples.text.pipeline.ip.in '"  hello "' -H "Fbp-Run-Id:$RUN"
```

### Command line (fbpcli)

`cmd/fbpcli` (logic in `pkg/service/cli`) calls a Network via `service.CallAll` and prints every
Packet of the Run as `<Outport> <data>`, one per line, until the Run is finished. `-first` stops
after the first Packet (`service.Call`). The Network is given as `<packagepath>/<networkname>`; the input as
JSON with `-input`, or read from stdin when `-input` is empty or `-`. `-string` sends the input as a
JSON string. `-inport` names the Network Inport to send to (default `In`). The NATS connection comes from `config.json` (see `example.json`), `-timeout`
(default 10s) limits the wait. Exit status is 1 on failure or when a Packet left through `Error`.

```sh
fbpcli -network gofbp/examples/text/pipeline -string -input "  hello "
Success "HELLO!"
echo '42' | fbpcli -network gofbp/examples/text/pipeline
Error {"node":"trim","brick":"Trim","message":"input is not a JSON string","input":42}
```

## Discovery ($SRV)

Every Service answers the [NATS micro](https://docs.nats.io/using-nats/developer/services)
discovery requests `$SRV.PING`, `$SRV.INFO` and `$SRV.STATS`, each on all three levels (all
services, `.<name>`, `.<name>.<id>`). Tools like `nats micro ls` therefore list the running
Networks and their instances.

| Field | Value |
|-------|-------|
| `name` | Base subject with every character other than letters, digits, `-` and `_` replaced by `_`, plus `_` and the first 8 hex digits of the SHA-256 of the base subject, so that e.g. `fbp.a_b.c` and `fbp.a.b_c` get different names (`service.ServiceName`), e.g. `fbp_gofbp_examples_text_pipeline_bf241440` |
| `id` | `s.ID()`, the same ID as `service` in `EventData` |
| `version` | `WithVersion`, default `0.0.0` |
| `metadata` | `package`, `network`, `base`, `inports` (comma-separated, including In), `outports` (comma-separated, including Success and Error), `trace`, `log` (Log subject of this Service) |
| `endpoints` | `in` (subject of Inport `In`), one endpoint named after each extra Inport (its subject), all with queue group = base subject, and `observe` (no queue group) |

`$SRV.STATS` counts per endpoint: for each Inport endpoint, received messages and errors (invalid
JSON, Packet not delivered); its `data` is `service.InportStats` with the messages dropped by the
pending limits, and for `in` also the Packets published per Outport (for all Inports together). The `data` of
the `observe` endpoint is `service.ObserveStats` (`{"observing":true|false}`): whether the Service
currently publishes Events on the Trace subject. A local Observer set with `s.SetObserver` counts as
off. Brick errors are not Inport errors; they show up as Packets on
the Error Outport. `processing_time` stays 0, since a Packet's results leave on Outport subjects,
not as a reply.

The Service does not use `micro.AddService`: micro stops the whole service on any asynchronous
subscription error, including a slow consumer, and replaces the connection's error and closed
handlers. A Service keeps running and only drops messages (see
[Where Packets can be lost](#where-packets-can-be-lost)). Sending a request to the `in` endpoint
gets no reply.

## Logs

A Service publishes its log records on NATS, so tools like gofbpSRVLS show them without access to the
host or container:

- `s.Logger()` passes every record on to the logger from `WithLogger` (default `slog.Default()`)
  and publishes records at or above `WithLogLevel` (default `slog.LevelInfo`) as
  `service.LogData` (JSON) on `fbp.<packagepath>.<networkname>.log.<serviceid>`
  (`LogSubject(base, s.ID())`; `LogSubject(base, "*")` for all Services of the Network). The
  Instance logs through the same logger.
- `Seq` counts the published records of a Service from 1. Attributes and groups are flattened into
  `attrs` as `group.key`; errors become their text, durations e.g. `"1s"`.
- The Service keeps the last `WithLogHistory` records (default 500, `DefaultLogHistory`) and
  answers a request on `fbp.<packagepath>.<networkname>.loghistory.<serviceid>` with them as a
  JSON array, oldest first. A reader subscribes to the Log subject first, then requests the history
  and drops live records whose `seq` it already got.
- Call `slog.SetDefault(s.Logger())` after `Start`, so `main`, Bricks and the `log` package log
  via NATS too (the examples do). Records before `Start` (config, NATS connect) go to stderr only.
- slog's built-in default handler writes through the `log` package and would deadlock after
  `slog.SetDefault(s.Logger())`. Without `WithLogger`, the Service therefore writes to stderr with
  `slog.NewTextHandler` (`time=… level=INFO msg=…`) instead of the default format.
- Publish errors of log records are not logged (that would publish again); the records are only
  lost on NATS and still reach the logger from `WithLogger`.

```sh
nats sub 'fbp.gofbp.examples.text.pipeline.log.*'
nats req fbp.gofbp.examples.text.pipeline.loghistory.<serviceid> ''
```

## Where Packets can be lost

The output function of a Service publishes to NATS and never waits for the NATS handler, so the
deadlock described in [concept.md](concept.md#running-an-instance) cannot happen. Packets can still be lost:

| Situation | What happens | Visible as |
|-----------|--------------|------------|
| The Network is slower than incoming messages | The NATS handler blocks in `Send`; further messages wait in the NATS client. Beyond the pending limits (default 10,000 messages / 64 MB, `WithPendingLimits`) the client drops them. The queue group does **not** redeliver them to another Service | Warning `service: slow consumer, messages dropped` with the number of dropped messages |
| NATS is slow while connected | Publishing writes a full outbound buffer to the socket synchronously; the Brick waits meanwhile and backpressure slows the Network down. Nothing is lost | – |
| The connection is down | Results are kept in the client's reconnect buffer (8 MB). Once it is full, publishing fails and the result is lost | Error `service: publish failed, packet lost` |
| `Stop` | Messages still waiting in the NATS client and Packets in Inport buffers are discarded | – |

The pending limits are kept far below the NATS client defaults (500,000 messages / 64 MB) so that
an overloaded Service starts dropping and logging early instead of hoarding messages. `natscon.Connect`
additionally logs all async errors of the connection. Reliable delivery would need JetStream with
per-message acknowledgement; this is not part of goFBP.

## Running the example

```sh
cd examples/text/cmd/textservice
cp example.json config.json        # adjust url / nkeyfile
/snap/go/current/bin/go build -o textservice .
./textservice -config config.json

nats sub 'fbp.gofbp.examples.text.pipeline.op.>'
nats pub fbp.gofbp.examples.text.pipeline.ip.in '"  hello "'   # -> op.Success.<runid> "HELLO!"

nats sub fbp.gofbp.examples.text.pipeline.trace
nats pub fbp.gofbp.examples.text.pipeline.observe true     # observing on
nats pub fbp.gofbp.examples.text.pipeline.observe false    # observing off

nats micro ls                                              # all running Networks
nats micro info fbp_gofbp_examples_text_pipeline_bf241440
nats micro stats fbp_gofbp_examples_text_pipeline_bf241440
```
