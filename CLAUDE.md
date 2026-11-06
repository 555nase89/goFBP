# goFBP

Small Flow Based Programming library for Go: Networks of Bricks run in-process over Go channels;
a Service makes one Instance reachable via NATS. Standalone library, unrelated to `../natsFBP`:
its concepts, names and subject scheme do not apply here.

## Language

[docs/GLOSSARY.md](docs/GLOSSARY.md) is the single source of truth for the domain language
(Brick, Network, Node, Packet, Run, Instance, Observer, Service …) and for the behaviour
rules (channels, Fan-out, copying, stop, Service subjects). Read it before you:
- name a type, function, field, test or doc section after a domain concept,
- change behaviour in `pkg/fbp` or `pkg/service`,
- answer a question about how goFBP behaves.

Use the glossary terms exactly, in code and prose. Each term lists the alternatives to avoid under
`_Avoid_`. If a new concept or a changed rule is needed, update GLOSSARY.md in the same change.

## Docs

- `docs/concept.md`: package overview, definition vs. runtime, how to write a Brick.
- `docs/fbp-package.md`, `docs/service-package.md`, `docs/service.md`: per-package details.
  When behaviour of a package changes, update its doc (and the matching `.svg` if the diagram is affected).
- `tasks/todo.md`: done/open work; add a line under "Done" for each finished change.

## Rules

- `pkg/fbp` uses the standard library only. NATS stays in `pkg/service` and `pkg/natscon`.
- `Brick` and `Network` are definitions: no channels, goroutines or mutable state in them.
  Runtime state belongs to `Instance`.
- Packet data is always valid JSON; `Send`/`Emit` copy it. Keep both invariants when touching them.
- `pkg/service` tests start an embedded nats-server, so no external NATS is needed for
  `/snap/go/current/bin/go test ./...`.
