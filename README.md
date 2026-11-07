# goFBP

A small [Flow Based Programming](https://en.wikipedia.org/wiki/Flow-based_programming) library for Go.

- **Bricks** hold the business logic: one Inport, the Outports `Success` and `Error`, plus any you add.
- **Networks** connect Bricks and other Networks into a graph. They run in-process, and the Packets travel over buffered Go channels.
- **Services** make one running Network reachable via [NATS](https://nats.io). Several Services of the same Network share the load through a queue group.

Packet data is always valid JSON. Every Packet carries a **Run ID**, so all results of one input can be followed through the Network and across NATS.

The core package `pkg/fbp` uses the standard library only. NATS is used only in `pkg/service` and `pkg/natscon`.

## Install

```sh
go get github.com/555nase89/goFBP
```

Requires Go 1.27 or later.

## Quick start

### A Brick

A Brick is a struct with a `Process` function. Configuration is captured by a constructor:

```go
func Suffix(suffix string) *fbp.Brick {
    return &fbp.Brick{
        Name: "Suffix",
        Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
            s, err := fbp.DataAs[string](p)
            if err != nil {
                return err // sent through the Error Outport as fbp.ErrorData
            }
            return fbp.EmitAs(ctx, out, fbp.Success, s+suffix)
        },
    }
}
```

A returned error or a panic does not stop the Network. It becomes a Packet on the Brick's `Error` Outport.

### A Network

```go
n := &fbp.Network{Name: "shout"}
err := errors.Join(
    n.Add("upper", Upper()),
    n.Add("suffix", Suffix("!")),
    n.ConnectIn("upper"),                            // Network Inport In -> upper
    n.Connect("upper", fbp.Success, "suffix"),       // upper.Success -> suffix
    n.ConnectOut("suffix", fbp.Success, fbp.Success), // suffix.Success -> Network Outport Success
)
```

A Network can be a Node in another Network. Nesting adds no extra channel hop.

### Running it in-process

```go
inst, err := n.Start(ctx, fbp.WithOutput(func(ctx context.Context, outport string, p fbp.Packet) {
    fmt.Println(outport, string(p.Data))
}))
defer inst.Stop()

runID, err := inst.SendAs(ctx, "hello") // -> Success "HELLO!"
```

### Running it as a Service

```go
s, err := service.Start(ctx, nc, "github.com/acme/flows", n)
defer s.Stop()
```

The Service listens on `fbp.github_com.acme.flows.shout.ip.in`. It publishes the results on `...op.<outport>.<runid>` and reports the end of each Run on `...done.<runid>`. Clients use `service.Call`/`service.CallAll` or the `fbpcli` command. Every Service also answers NATS micro discovery (`nats micro ls`). It publishes its logs on NATS and can be switched to publish trace Events.

## Features

- Fan-out: one Outport connected to many Inports. Every receiver gets its own copy of the data.
- Several Inports and extra Outports per Network.
- `Concurrency` per Brick: several copies of a Brick process Packets in parallel.
- Backpressure: a full Inport buffer makes the sender wait.
- Observers: an Event for every step of every Packet, with Packet IDs and parent links. Observers can be switched on and off at runtime, also via NATS for all Services of a Network.
- Done notification per Run, with the number of results, so a client knows when it has everything.
- Logs published on NATS, with a log history that can be requested.

Delivery is best effort. Read [Where Packets can be lost](docs/service.md#where-packets-can-be-lost) before you rely on it.

## Project layout

| Path | Contents |
|------|----------|
| `pkg/fbp` | Core: `Packet`, `Brick`, `Network`, `Instance`, Observer |
| `pkg/fbptest` | Test helpers for Bricks and Networks (`CallBrick`, `Start`/`Run`, `Want`) |
| `pkg/service` | Service: runs one Instance on NATS. Client helpers `Call`/`CallAll` |
| `pkg/service/cli` | Logic of `fbpcli` |
| `pkg/natscon` | Reads the `natscon` config section and connects to NATS (optional NKey and TLS) |
| `cmd/fbpcli` | Command line client that calls a Network running as a Service |
| `examples/text` | String Bricks and a nested Network (`text.Pipeline`) |
| `examples/crash` | A Network whose Bricks fail and panic, to show the Error Outport |
| `docs/` | Concept and package documentation |

## Examples

In-process, without NATS:

```sh
printf '  hello \n   \n' | go run ./examples/text/cmd/textlocal     # results on stdout
printf '  hello \n'      | go run ./examples/text/cmd/texttrace     # every Event of every Run
go run ./examples/crash/cmd/crashlocal                              # error handling
```

As a Service (needs a running NATS server):

```sh
cp examples/text/cmd/textservice/example.json examples/text/cmd/textservice/config.json
go run ./examples/text/cmd/textservice -config examples/text/cmd/textservice/config.json

cp cmd/fbpcli/example.json cmd/fbpcli/config.json
go run ./cmd/fbpcli -config cmd/fbpcli/config.json \
    -network gofbp/examples/text/pipeline -string -input "  hello "
# Success "HELLO!"
```

`config.json` holds the NATS URL, an optional NKey file and optional TLS files (CA, client certificate and key). It is not versioned. Use `example.json` as the template.

## Tests

```sh
go test -race ./...
```

The `pkg/service` tests start an embedded NATS server, so you do not need an external NATS server.

To test your own Bricks and Networks, use `pkg/fbptest`; see [docs/concept.md](docs/concept.md#tests).

## Documentation

- [docs/GLOSSARY.md](docs/GLOSSARY.md): the domain terms and behaviour rules. This is the reference for how goFBP behaves.
- [docs/concept.md](docs/concept.md): definition vs. runtime, writing Bricks, building and observing Networks.
- [docs/service.md](docs/service.md): subjects, calling a Service, discovery, logs, delivery guarantees.
- [docs/fbp-package.md](docs/fbp-package.md), [docs/service-package.md](docs/service-package.md): the types in each package.
- [docs/claude-skills.md](docs/claude-skills.md): Claude Code skills that create Networks from a short arrow notation or an image and draw them as SVG.

## License

BSD 3-Clause, see [LICENSE](LICENSE).
