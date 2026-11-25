package fbp

import (
	"encoding/json"
	"sync/atomic"
	"time"
)

// EventKind says at which station of a Run an Event happened.
type EventKind int

const (
	Sent      EventKind = iota // a Packet entered the Network via Instance.Send
	Received                   // a Brick took a Packet from its Inport
	Processed                  // the Brick's ProcessFunc returned
	Emitted                    // a Brick sent a new Packet through an Outport
	Output                     // a Packet left an Outport of the top-level Network
)

func (k EventKind) String() string {
	switch k {
	case Sent:
		return "Sent"
	case Received:
		return "Received"
	case Processed:
		return "Processed"
	case Emitted:
		return "Emitted"
	case Output:
		return "Output"
	}
	return "EventKind(?)"
}

// Event describes one step of a Packet through an Instance.
type Event struct {
	Kind  EventKind
	Time  time.Time
	RunID string
	// PacketID identifies the Packet within the Instance. Send and every Emit
	// create a new Packet; the copies of a Fan-out keep its ID.
	PacketID uint64
	// ParentID is set for Emitted only: the Packet the Brick was processing.
	ParentID uint64
	// Network is the name of the top-level Network of the Instance.
	Network string
	// Node is the node path within the top-level Network, e.g. "shout/upper".
	// It is empty for Sent and Output.
	Node string
	// Port is the Network's Inport for Sent, "Inport" for Received, the
	// Outport for Emitted and Output, and empty for Processed.
	Port string
	// Copy is the number of the Brick copy (see Brick.Concurrency), 0-based.
	Copy int
	// Data is the Packet data; empty for Processed. It is only valid during
	// the call of the ObserverFunc and must not be modified; copy it to keep it.
	Data json.RawMessage
	// Err is set for Processed only: the error or panic of the ProcessFunc.
	Err error
}

// ObserverFunc receives every Event of an Instance. It is called synchronously
// from the goroutine that causes the Event, possibly from several goroutines
// at once, so shared state must be guarded. Like OutputFunc it must return
// quickly: while it runs, the Brick or Send that caused the Event stands
// still. Events of different Bricks may arrive out of order; sort by Time.
type ObserverFunc func(Event)

// WithObserver sets the function receiving the Events of the Instance from
// its start on. Without an Observer no Events and no Packet IDs are created.
// If given more than once, the last one wins. See Instance.SetObserver to
// change it while the Instance runs.
func WithObserver(fn ObserverFunc) Option {
	return func(c *config) { c.observer = fn }
}

// SetObserver sets or replaces the function receiving Events while the
// Instance runs; nil switches observing off. Packets already in flight when
// observing is switched on have Packet ID 0, and so have the parents of the
// Packets they lead to; Runs sent afterwards are complete. After switching
// off, a call that is already running still finishes, and an Event that
// another goroutine is just reporting may still arrive.
func (i *Instance) SetObserver(fn ObserverFunc) {
	i.obs.set(fn)
}

// observer is shared by all parts of one Instance. It always exists; whether
// Events are reported is decided at each station by loading fn.
type observer struct {
	fn      atomic.Pointer[ObserverFunc]
	network string
	ids     atomic.Uint64
}

func (o *observer) set(fn ObserverFunc) {
	if fn == nil {
		o.fn.Store(nil)
		return
	}
	o.fn.Store(&fn)
}

// enabled reports whether an ObserverFunc is set right now.
func (o *observer) enabled() bool {
	return o.fn.Load() != nil
}

func (o *observer) newID() uint64 {
	return o.ids.Add(1)
}

// event reports e if an ObserverFunc is set.
func (o *observer) event(e Event) {
	fn := o.fn.Load()
	if fn == nil {
		return
	}
	e.Time = time.Now()
	e.Network = o.network
	(*fn)(e)
}
