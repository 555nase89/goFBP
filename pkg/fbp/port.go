package fbp

import (
	"context"
	"log/slog"
)

// receiver is anything a Packet can be delivered to: an Inport's channel, or
// an Outport that forwards to its own targets (used for Network ports).
type receiver interface {
	receive(ctx context.Context, p Packet) error
}

// Inport owns one buffered channel. All Connections leading to it write into
// this channel. It is never closed.
type Inport struct {
	node string
	ch   chan Packet
	runs *runs
}

// receive blocks while the buffer is full, until there is space again or ctx
// is done. The Packet counts for its Run before it enters the channel, so the
// receiving Brick cannot finish it first; the sender still holds its own
// Packet, so taking it back on failure never ends the Run.
func (in *Inport) receive(ctx context.Context, p Packet) error {
	in.runs.add(p.RunID)
	select {
	case in.ch <- p:
		return nil
	case <-ctx.Done():
		in.runs.done(p.RunID)
		return ctx.Err()
	}
}

// Outport keeps the list of receivers it is connected to and sends a copy of
// each Packet to every one of them, one after another (Fan-out).
type Outport struct {
	node    string
	name    string
	targets []receiver
	logger  *slog.Logger
	obs     *observer // set only on the Outports of the top-level Network
}

// receive takes ownership of p: its Data is shared with no one else, because
// Send and Emit copy the data and Error packets are freshly marshalled. So
// the last target gets p itself and only the others need a copy.
func (out *Outport) receive(ctx context.Context, p Packet) error {
	if out.obs != nil {
		out.obs.event(Event{Kind: Output, RunID: p.RunID, PacketID: p.id, Port: out.name, Data: p.Data})
	}
	if len(out.targets) == 0 {
		if out.name == Error {
			out.logger.Warn("fbp: error packet on unconnected outport",
				"node", out.node, "outport", out.name, "run_id", p.RunID, "data", string(p.Data))
		}
		return nil
	}
	for i, t := range out.targets {
		q := p
		if i < len(out.targets)-1 {
			q = p.clone()
		}
		if err := t.receive(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// OutputFunc receives every Packet leaving an Outport of the top-level
// Network. It is called synchronously from the goroutine of the sending Brick,
// possibly from several goroutines at once, so shared state must be guarded.
// While it runs, that Brick stands still; if it blocks, backpressure stops the
// whole Network up to Instance.Send. It must therefore return quickly and must
// never wait for the caller of Send, or both wait for each other forever.
type OutputFunc func(ctx context.Context, outport string, p Packet)

type outputReceiver struct {
	outport string
	fn      OutputFunc
	runs    *runs
}

func (o outputReceiver) receive(ctx context.Context, p Packet) error {
	o.runs.output(p.RunID)
	o.fn(ctx, o.outport, p)
	return nil
}
