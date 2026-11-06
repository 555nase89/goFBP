package fbp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// ErrStopped is returned by Send and Emit once the Instance has been stopped.
var ErrStopped = errors.New("fbp: instance stopped")

// ErrInvalidJSON is returned by Send and Emit for Packet data that is not
// valid JSON.
var ErrInvalidJSON = errors.New("fbp: packet data is not valid JSON")

// DefaultBufferSize is the capacity of every Inport channel unless
// WithBufferSize is given.
const DefaultBufferSize = 10

// Option configures an Instance.
type Option func(*config)

type config struct {
	bufferSize int
	logger     *slog.Logger
	output     OutputFunc
	observer   ObserverFunc
	finished   RunFinishedFunc
	runs       *runs // created by Start, nil without WithRunFinished
}

// WithBufferSize sets the capacity of every Inport channel.
func WithBufferSize(n int) Option {
	return func(c *config) { c.bufferSize = max(n, 0) }
}

// WithLogger sets the logger, e.g. for error packets on unconnected Outports.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithOutput sets the function receiving all Packets leaving the Network's
// Outports. Without it those Packets are dropped (Error packets are logged).
// If given more than once, the last one wins. See OutputFunc for how it is
// called.
func WithOutput(fn OutputFunc) Option {
	return func(c *config) { c.output = fn }
}

// RunFinishedFunc is called once a Run is finished: no Packet of it is
// buffered in an Inport or being processed by a Brick any more. outputs is the
// number of Packets of the Run that left the Network's Outports; every call of
// the OutputFunc for the Run happened before. It is called synchronously from
// the goroutine of the last Brick of the Run (or from Send) and must return
// quickly, like OutputFunc.
type RunFinishedFunc func(runID string, outputs int)

// WithRunFinished sets the function called when a Run is finished. Without it
// the Instance does not track Runs. A Run whose Packets are lost on Stop
// never finishes. Packets sent with the Run ID of a Run that is still going
// on count for that Run, so it finishes only once.
func WithRunFinished(fn RunFinishedFunc) Option {
	return func(c *config) { c.finished = fn }
}

// Instance is one running copy of a Network.
type Instance struct {
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	entries map[string]*Outport // the Network's Inports by name
	obs     *observer
	runs    *runs
}

// Start validates the Network and starts an Instance of it. The Instance
// stops when ctx is done or Stop is called.
func (n *Network) Start(ctx context.Context, opts ...Option) (*Instance, error) {
	if err := n.Validate(); err != nil {
		return nil, err
	}
	cfg := config{bufferSize: DefaultBufferSize, logger: slog.Default()}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.finished != nil {
		cfg.runs = &runs{inFlight: map[string]*runState{}, finished: cfg.finished}
	}

	obs := &observer{network: n.Name}
	obs.set(cfg.observer)

	top := n.newPorts(n.Name, &cfg)
	for name, o := range top.outports {
		if cfg.output != nil {
			o.targets = []receiver{outputReceiver{name, cfg.output, cfg.runs}}
		}
		o.obs = obs
	}
	var bricks []*brickNode
	n.build("", top, &cfg, &bricks)

	ctx, cancel := context.WithCancel(ctx)
	inst := &Instance{ctx: ctx, cancel: cancel, entries: map[string]*Outport{}, obs: obs, runs: cfg.runs}
	for name, in := range top.inports {
		inst.entries[name] = in.(*Outport)
	}
	for _, bn := range bricks {
		bn.obs = obs
		for c := range max(bn.brick.Concurrency, 1) {
			inst.wg.Go(func() { bn.run(ctx, c) })
		}
	}
	return inst, nil
}

// runNode is the runtime view of a Node: where Packets for it are delivered
// and the Outports they leave through.
type runNode struct {
	inports  map[string]receiver
	outports map[string]*Outport
}

// newPorts creates the runtime ports of a Network occurrence. A Network's
// ports have no channels of their own: its Inport forwards to the Inports of
// the connected inner Nodes, and each Outport forwards to the targets in the
// enclosing Network. Nesting thus adds no extra hop.
func (n *Network) newPorts(path string, cfg *config) runNode {
	rn := runNode{inports: map[string]receiver{}, outports: map[string]*Outport{}}
	for _, name := range n.inportNames() {
		rn.inports[name] = &Outport{node: path, name: name, logger: cfg.logger}
	}
	for _, name := range n.outportNames() {
		rn.outports[name] = &Outport{node: path, name: name, logger: cfg.logger}
	}
	return rn
}

// build creates the runtime Nodes of n, wires them to the ports self and
// appends every Brick Node to bricks. prefix is the node path of n within the
// top-level Network, e.g. "outer/inner/".
func (n *Network) build(prefix string, self runNode, cfg *config, bricks *[]*brickNode) {
	nodes := map[string]runNode{}
	for _, name := range n.order {
		path := prefix + name
		switch node := n.nodes[name].(type) {
		case *Brick:
			bn := &brickNode{
				name:     path,
				brick:    node,
				in:       &Inport{node: path, ch: make(chan Packet, cfg.bufferSize), runs: cfg.runs},
				outports: map[string]*Outport{},
			}
			for _, op := range node.outportNames() {
				bn.outports[op] = &Outport{node: path, name: op, logger: cfg.logger}
			}
			*bricks = append(*bricks, bn)
			nodes[name] = runNode{inports: map[string]receiver{In: bn.in}, outports: bn.outports}
		case *Network:
			rn := node.newPorts(path, cfg)
			node.build(path+"/", rn, cfg, bricks)
			nodes[name] = rn
		}
	}
	for _, c := range n.conns {
		out := nodes[c.from].outports[c.outport]
		out.targets = append(out.targets, nodes[c.to].inports[c.inport])
	}
	for _, c := range n.outConns {
		out := nodes[c.from].outports[c.outport]
		out.targets = append(out.targets, self.outports[c.netOutport])
	}
	for _, c := range n.inConns {
		entry := self.inports[c.netInport].(*Outport)
		entry.targets = append(entry.targets, nodes[c.to].inports[c.inport])
	}
}

// Send delivers p to the Network's Inport In and returns its Run ID. A Packet
// without Run ID gets a new one. Send blocks while the Inport buffers are
// full, until ctx is done or the Instance stops. Data that is not valid JSON
// is rejected with ErrInvalidJSON. The data is copied, so the caller may
// reuse it once Send returns.
func (i *Instance) Send(ctx context.Context, p Packet) (string, error) {
	return i.SendTo(ctx, In, p)
}

// SendTo delivers p to the Network's Inport inport like Send.
func (i *Instance) SendTo(ctx context.Context, inport string, p Packet) (string, error) {
	if i.ctx.Err() != nil {
		return "", ErrStopped
	}
	entry, ok := i.entries[inport]
	if !ok {
		return "", fmt.Errorf("fbp: network has no inport %q", inport)
	}
	if !json.Valid(p.Data) {
		return "", ErrInvalidJSON
	}
	p = p.clone()
	if p.RunID == "" {
		p.RunID = NewRunID()
	}
	if i.obs.enabled() {
		p.id = i.obs.newID()
		i.obs.event(Event{Kind: Sent, RunID: p.RunID, PacketID: p.id, Port: inport, Data: p.Data})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(i.ctx, cancel)()
	// Send counts as a Packet of the Run until the Fan-out of the Network's
	// Inport is done, so a first target finishing early cannot end the Run.
	i.runs.add(p.RunID)
	err := entry.receive(ctx, p)
	i.runs.done(p.RunID)
	// A Packet that got into a buffer while the Instance was stopping is
	// lost, so report it as not delivered.
	if i.ctx.Err() != nil {
		return "", ErrStopped
	}
	if err != nil {
		return "", err
	}
	return p.RunID, nil
}

// SendAs marshals v to JSON and sends it like Send, with a new Run ID. An
// error from json.Marshal is returned without sending anything.
func (i *Instance) SendAs(ctx context.Context, v any) (string, error) {
	return i.SendAsTo(ctx, In, v)
}

// SendAsTo marshals v to JSON and sends it like SendTo, with a new Run ID.
func (i *Instance) SendAsTo(ctx context.Context, inport string, v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return i.SendTo(ctx, inport, Packet{Data: data})
}

// Stop stops all Bricks immediately and waits until their goroutines have
// returned. Packets still waiting in Inport buffers are lost.
func (i *Instance) Stop() {
	i.cancel()
	i.wg.Wait()
}

// brickNode is the runtime state of one Brick Node within an Instance.
type brickNode struct {
	name     string
	brick    *Brick
	in       *Inport
	outports map[string]*Outport
	obs      *observer
}

// event reports e for this Node if the Instance is observed.
func (n *brickNode) event(e Event) {
	e.Node = n.name
	n.obs.event(e)
}

// emit sends p, which must not share its data, through an Outport and
// reports it as a new Packet with parent as its origin.
func (n *brickNode) emit(ctx context.Context, out *Outport, p Packet, parent uint64, cp int) error {
	if n.obs.enabled() {
		p.id = n.obs.newID()
		n.event(Event{Kind: Emitted, RunID: p.RunID, PacketID: p.id, ParentID: parent, Port: out.name, Copy: cp, Data: p.Data})
	}
	return out.receive(ctx, p)
}

func (n *brickNode) run(ctx context.Context, cp int) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-n.in.ch:
			if ctx.Err() != nil {
				return
			}
			n.event(Event{Kind: Received, RunID: p.RunID, PacketID: p.id, Port: "Inport", Copy: cp, Data: p.Data})
			n.process(ctx, p, cp)
			n.in.runs.done(p.RunID)
		}
	}
}

func (n *brickNode) process(ctx context.Context, p Packet, cp int) {
	err := n.call(ctx, p, cp)
	n.event(Event{Kind: Processed, RunID: p.RunID, PacketID: p.id, Copy: cp, Err: err})
	if err == nil || ctx.Err() != nil {
		return
	}
	data, _ := json.Marshal(ErrorData{Node: n.name, Brick: n.brick.Name, Message: err.Error(), Input: p.Data})
	_ = n.emit(ctx, n.outports[Error], Packet{Data: data, RunID: p.RunID}, p.id, cp)
}

func (n *brickNode) call(ctx context.Context, p Packet, cp int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return n.brick.Process(ctx, p, emitter{node: n, stop: ctx, runID: p.RunID, parent: p.id, copy: cp})
}

type emitter struct {
	node   *brickNode
	stop   context.Context // context of the Instance
	runID  string
	parent uint64 // Packet ID of the Packet being processed
	copy   int
}

func (e emitter) Emit(ctx context.Context, outport string, data json.RawMessage) error {
	out, ok := e.node.outports[outport]
	if !ok {
		return fmt.Errorf("fbp: node %q has no outport %q", e.node.name, outport)
	}
	if !json.Valid(data) {
		return fmt.Errorf("fbp: node %q, outport %q: %w", e.node.name, outport, ErrInvalidJSON)
	}
	// Like Send, Emit must not outlive the Instance, whatever ctx the Brick
	// passes; otherwise Stop would wait for it forever.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(e.stop, cancel)()
	err := e.node.emit(ctx, out, Packet{Data: data, RunID: e.runID}.clone(), e.parent, e.copy)
	if e.stop.Err() != nil {
		return ErrStopped
	}
	return err
}
