# goFBP concept

goFBP is a small Flow Based Programming library for Go. Networks of Bricks run in-process and talk
through Go channels; a Service makes one running Network reachable via NATS. Terms are defined in
[GLOSSARY.md](GLOSSARY.md).

## Packages

| Package / folder     | Contents |
|----------------------|----------|
| `pkg/fbp`            | Core: `Packet`, `Brick`, `Network`, `Inport`/`Outport`, `Instance`. Standard library only |
| `pkg/fbptest`        | Test helpers: call a Brick without a Network, run a Packet through an Instance and compare its outputs. Standard library only |
| `pkg/service`        | Wraps one Instance and connects it to NATS subjects |
| `pkg/natscon`        | Reads the `natscon` config part and connects to NATS (optional NKey and TLS) |
| `examples/text`      | Example Bricks and Networks, including a nested Network |
| `examples/text/cmd/textservice` | Runs `text.Pipeline` as a Service |
| `examples/text/cmd/textlocal`   | Runs `text.Pipeline` in-process: stdin lines in, results on stdout |

## Definition and runtime

`Brick` and `Network` are definitions only. They hold no channels and no goroutines, so the same
Brick or Network can be used as several Nodes and in several Instances. `Network.Start` validates the
definition and builds an `Instance`: one buffered channel per Brick Node and `Concurrency`
goroutines reading from it.

Packet data is always valid JSON: `Instance.Send` returns `ErrInvalidJSON` for anything else, and
`Emit` returns an error wrapping it, so the Brick fails and its `ErrorData` reaches the Error Outport.
Outport names must not be empty.

`Send` and `Emit` copy the data, so the caller may reuse or change its slice right after the call,
and every receiver owns the data it gets. A Brick may therefore change `p.Data` in place without
affecting anyone else. On Fan-out every target gets its own copy.

## Writing a Brick

The business logic is a `ProcessFunc` in the Brick struct. Configuration is captured by a
constructor function:

```go
func Suffix(suffix string) *fbp.Brick {
    return &fbp.Brick{
        Name: "Suffix",
        Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
            s, err := fbp.DataAs[string](p)
            if err != nil {
                return err // goes to the Error Outport as ErrorData
            }
            return fbp.EmitAs(ctx, out, fbp.Success, s+suffix)
        },
    }
}
```

- `fbp.DataAs` decodes the incoming data, `fbp.EmitAs` encodes and sends a value; `Emit` sends
  data that is already JSON.
- `Emit` keeps the Run ID of the incoming Packet.
- Additional Outports are listed in `Brick.Outports`; `Success` and `Error` always exist.
- A returned error or a panic is sent through the Error Outport as `fbp.ErrorData`.
- The `Emitter` is valid only while the function runs. It may be used from goroutines the function
  waits for (e.g. with a `sync.WaitGroup`), but once the function has returned, `Emit` returns an
  error wrapping `fbp.ErrEmitAfterProcess`. A Packet emitted later would count for a Run that was
  already reported finished.
- The function must return normally or panic. `runtime.Goexit` (e.g. via `testing.T.FailNow` or
  `log.Fatal`-like helpers that end only the goroutine) is not caught: that copy of the Brick ends
  without an Error Packet, processes no further Packets, and the Run never finishes. With
  `Concurrency` 1 the Inport buffer then fills up and backpressure stops the Network in front of it.
- The function may run in parallel (Concurrency, several Instances). Shared state must be guarded
  by the Brick.

## Building a Network

```go
n := &fbp.Network{Name: "pipeline", Outports: []string{"Empty"}}
n.Add("trim", Trim())
n.Add("shout", shoutNetwork)              // nested Network
n.ConnectIn("trim")                       // Network Inport -> trim
n.Connect("trim", fbp.Success, "shout")   // trim.Success -> shout
n.ConnectOut("trim", "Empty", "Empty")    // trim.Empty -> Network Outport Empty
n.ConnectOut("shout", fbp.Success, fbp.Success)
```

Besides its default Inport `fbp.In`, a Network can have additional Inports, listed in
`Network.Inports`. Each one is connected like `In`; a nested Network's Inport is targeted with
`ConnectTo`:

```go
n := &fbp.Network{Name: "pipeline", Inports: []string{"Raw"}}
n.ConnectIn("trim")                           // Inport In -> trim
n.ConnectInport("Raw", "shout")               // Inport Raw -> shout (its Inport In)
n.ConnectInportTo("Raw", "nested", "Other")   // Inport Raw -> Inport Other of a nested Network
n.ConnectTo("trim", fbp.Success, "nested", "Other")
```

`Add` and the `Connect*` methods check their arguments immediately (unknown nodes or ports,
duplicate node names, duplicate connections, a Network containing itself). Different Bricks may
share a name, e.g. `text.Suffix("!")` and `text.Suffix("?")` in one Network. `Validate`, called by `Start`, checks the whole tree: every Network needs a name
and every one of its Inports connected, and no Network may contain a cycle.

## Running an Instance

```go
inst, err := n.Start(ctx,
    fbp.WithOutput(func(ctx context.Context, outport string, p fbp.Packet) { ... }),
    fbp.WithBufferSize(10),
    fbp.WithLogger(slog.Default()),
)
runID, err := inst.Send(ctx, fbp.Packet{Data: json.RawMessage(`"hello"`)})
runID, err = inst.SendAs(ctx, "hello") // same, marshals the value to JSON
runID, err = inst.SendTo(ctx, "Raw", fbp.Packet{Data: json.RawMessage(`"hello"`)}) // other Inport
inst.Stop()
```

- `Send` delivers to the Inport `In`, `SendTo`/`SendAsTo` to any Inport of the Network. They assign
  a new Run ID if the Packet has none and block while the Inport buffers are full. `SendAs` always
  starts a new Run; to keep a Run ID, use `Send` with a Packet.
- The `WithOutput` function receives every Packet leaving a Network Outport. Without it, such
  Packets are dropped; Error Packets are logged. It is called synchronously from the sending Brick's
  goroutine, possibly concurrently, so shared state must be guarded. While it runs, that Brick
  stands still; if it blocks, backpressure stops the whole Network up to `Send`.
  **It must return quickly and must never wait for the caller of `Send`.** Example of a deadlock:
  the function writes into a bounded channel that is only read after all `Send` calls are done.
  Once the channel is full, the function blocks, the Bricks block, the buffers fill up and `Send`
  blocks, too. Count with a `sync.WaitGroup`, or hand Packets to a goroutine of their own instead.
- `Stop` (or cancelling `ctx`) stops all Bricks immediately and waits for their goroutines.
  Buffered Packets are lost; a `Send` or `Emit` that is still waiting returns `fbp.ErrStopped`,
  whatever `ctx` was passed to it.

### Observing an Instance

`fbp.WithObserver` makes the path of every Packet visible:

```go
inst, err := n.Start(ctx, fbp.WithObserver(func(e fbp.Event) { ... }))
```

| Event | When | `Node` | `Port` |
|---|---|---|---|
| `Sent` | `Send`/`SendTo` delivers a Packet | empty | Network Inport, e.g. `In` |
| `Received` | a Brick takes a Packet from its Inport | node path | `Inport` |
| `Processed` | the `ProcessFunc` returned (`Err` set on error or panic) | node path | empty |
| `Emitted` | a Brick emits a new Packet, including Error Packets | node path | Outport |
| `Output` | a Packet leaves an Outport of the top-level Network | empty | Outport |

- `Network` is always the name of the top-level Network; `Node` is the path within it, e.g.
  `shout/upper`.
- `PacketID` is a counter per Instance. `Sent` and `Emitted` create a new ID; `Emitted` also sets
  `ParentID` to the Packet being processed. Following `ParentID` back from an `Output` gives the
  exact path of a result, even with several Packets of the same Run in flight. The copies of a
  Fan-out share the ID and differ in `Node`.
- `Copy` is the number of the Brick copy (see Concurrency), so `Received` and `Processed` of the
  same Packet can be paired.
- An `Emitted` without a matching `Received` or `Output` is a Packet that was lost, e.g. on an
  unconnected Outport or in a buffer at `Stop`.
- The rules of `WithOutput` apply: the function is called synchronously, possibly concurrently, and
  must return quickly. Events of different Bricks may arrive out of order; sort by `Time`. `Data`
  is only valid during the call.
- Without an Observer, no Events and no Packet IDs are created. Packet IDs do not travel over NATS;
  across Services, Events can only be joined by Run ID. A Service passes the option on with
  `service.WithNetworkOptions(fbp.WithObserver(...))`.
- `inst.SetObserver(fn)` (or `s.SetObserver(fn)` on a Service) switches observing on or replaces
  the Observer while the Instance runs; `SetObserver(nil)` switches it off. Packets already in
  flight when it is switched on have Packet ID 0, so their chain starts there; Runs sent afterwards
  are complete. After switching off, an Event that another goroutine is just reporting may still
  arrive.

### Nested Networks

A nested Network does not get channels or goroutines of its own. At `Start` each of its Inports becomes a
forwarder to the Inports of the inner Nodes it is connected to, and each of its Outports forwards to
the targets in the enclosing Network. Nesting therefore adds no extra hop. Node paths in logs and
`ErrorData` contain the nesting, e.g. `shout/upper`.

## Service

A Service makes one running Instance reachable via NATS: Packets arrive on an Inport subject and
leave on one subject per Network Outport and Run. Subjects, the client side (`service.Call`),
discovery via `$SRV` and where Packets can be lost are described in [service.md](service.md).

## Running the example

Without NATS, in-process:

```sh
cd examples/text/cmd/textlocal
/snap/go/current/bin/go build -o textlocal .
printf '  hello \n   \n' | ./textlocal          # each line is sent as a JSON string
printf '42\n"raw"\n' | ./textlocal -raw         # each line is sent as raw JSON
```

From the project root, with every Event printed (see `examples/text/cmd/texttrace`):

```sh
printf '  hello \n   \n' | /snap/go/current/bin/go run ./examples/text/cmd/texttrace
```

As a Service, see [service.md](service.md#running-the-example).

## Tests

```sh
/snap/go/current/bin/go test -race ./...
```

Package `pkg/fbptest` keeps tests of Bricks and Networks short and free of sleeps:

```go
// A Brick alone: no Network, no goroutines. A returned error or panic becomes
// ErrorData on the Error Outport, like in an Instance.
fbptest.CallBrick(t, text.Trim(), `"  hi "`).Want(t, fbptest.Outputs{fbp.Success: {`"hi"`}})

// A Network: Run waits until the Run is finished (WithRunFinished), so it sees
// every Packet of the Run and fails on missing or additional ones.
inst := fbptest.Start(t, n) // stopped when the test ends
inst.Run(t, `"  hello "`).Want(t, fbptest.Outputs{fbp.Success: {`"HELLO!"`}})
inst.Run(t, `42`).WantError(t, "trim", "not a JSON string")
```

`Want` compares the data as JSON (white space and key order do not matter) and ignores the order
of Packets within an Outport, because Fan-out and Concurrency may change it. A Run that does not
finish within `fbptest.Timeout` (5 s) fails the test and shows the Packets that left so far.

`pkg/service` tests start an embedded NATS server, so no external server is needed.
