package fbp

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
)

// Default Outports of every Brick and Network.
const (
	Success = "Success"
	Error   = "Error"
)

// In is the name of the default Inport of every Network and of the only
// Inport of a Brick.
const In = "In"

// Emitter sends data through an Outport of the Brick that is currently
// processing a Packet. The Run ID of the incoming Packet is kept. Data that
// is not valid JSON is not sent; Emit returns an error wrapping
// ErrInvalidJSON instead. The data is copied, so the Brick may reuse or
// change it once Emit returns. Emit blocks while the target buffers are full,
// until ctx is done or the Instance stops; then it returns ErrStopped.
// An Emitter is valid only while the ProcessFunc it was handed to runs; it
// may be used from goroutines the ProcessFunc waits for, but afterwards Emit
// returns an error wrapping ErrEmitAfterProcess.
type Emitter interface {
	Emit(ctx context.Context, outport string, data json.RawMessage) error
}

// EmitAs marshals v to JSON and sends it through outport of out. An error
// from json.Marshal is returned without sending anything.
func EmitAs(ctx context.Context, out Emitter, outport string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return out.Emit(ctx, outport, data)
}

// ProcessFunc is the business logic of a Brick. It is called once per
// incoming Packet. A returned error (or a panic) is sent to the Error Outport
// as ErrorData. It holds no state of its own; configuration is captured when
// the Brick is built. It must return normally or panic: runtime.Goexit (e.g.
// via testing.T.FailNow) ends the goroutine of that Brick copy without an
// Error Packet, the copy processes no further Packets, and the Run never
// finishes.
type ProcessFunc func(ctx context.Context, p Packet, out Emitter) error

// Brick is a reusable unit of business logic with one Inport and at least the
// Outports Success and Error. A Brick is a definition only; the runtime state
// is created per Instance when a Network starts.
type Brick struct {
	Name        string
	Description string
	// Outports lists additional Outports; Success and Error always exist.
	Outports []string
	// Concurrency is the number of copies processing packets in parallel
	// within one Instance. Values below 1 mean 1.
	Concurrency int
	Process     ProcessFunc
}

// Node is something that can be added to a Network: a *Brick or a nested
// *Network.
type Node interface {
	inportNames() []string
	outportNames() []string
}

func (b *Brick) inportNames() []string {
	return []string{In}
}

func (b *Brick) outportNames() []string {
	return withDefaults(b.Outports)
}

func (b *Brick) validate() error {
	if b.Name == "" {
		return errors.New("fbp: brick has no name")
	}
	if b.Process == nil {
		return errors.New("fbp: brick " + b.Name + " has no Process function")
	}
	if slices.Contains(b.Outports, "") {
		return errors.New("fbp: brick " + b.Name + " has an outport without name")
	}
	return nil
}

// withDefaults returns Success, Error and the extra names without duplicates.
func withDefaults(extra []string) []string {
	return withNames([]string{Success, Error}, extra)
}

// withNames returns names followed by the extra names without duplicates.
func withNames(names, extra []string) []string {
	names = slices.Clone(names)
	for _, e := range extra {
		if !slices.Contains(names, e) {
			names = append(names, e)
		}
	}
	return names
}
