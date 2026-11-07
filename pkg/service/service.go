// Package service makes a Network reachable via nats.io. A Service wraps
// exactly one Instance of a Network and subscribes to one subject per Network
// Inport. Several Services of the same Network share one queue group, so
// each Packet is processed by only one of them.
// Observing is switched on and off for all of them at once via the
// observe subject; the Events are then published on the trace subject.
// Every Service answers the NATS micro discovery requests $SRV.PING,
// $SRV.INFO and $SRV.STATS, so `nats micro ls` lists the running Networks.
//
// Packets can be lost on the way through a Service:
//   - When the Network is slower than the incoming messages, they queue up in
//     the NATS client. Beyond the pending limits (see WithPendingLimits) the
//     client drops further messages (slow consumer). The queue group does not
//     redeliver them to another Service. Drops are logged.
//   - When the connection is down and the NATS client's reconnect buffer is
//     full, publishing a result fails. This is logged.
//   - On Stop, messages not yet passed to the Network are discarded.
//   - When the ctx given to Start is done, the Instance stops and the Service
//     unsubscribes at once, so the queue group stops sending it messages.
//     Messages that arrive in between are discarded.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go"
)

// RunIDHeader is the NATS header carrying a Packet's Run ID. Like all NATS
// headers it is case-sensitive: a header named e.g. "fbp-run-id" is not
// recognised and the Packet gets a new Run ID. The Run ID becomes the last
// token of the Outport subject, so it must be a valid subject token of at
// most MaxRunIDLength bytes; otherwise the Packet is rejected on the Error
// subject under a new Run ID. A message the Service rejects for another
// reason gets a new Run ID too while its Run is going on, so it cannot add
// an Error Packet to or announce the end of a Run it does not belong to.
const RunIDHeader = "Fbp-Run-Id"

// MaxRunIDLength is the longest Run ID, in bytes, a Service accepts from the
// RunIDHeader. A UUID from fbp.NewRunID has 36.
const MaxRunIDLength = 64

// maxLoggedData is how many bytes of a rejected message's data are logged.
const maxLoggedData = 64

// Default pending limits of each Inport subscription. They are far below the
// NATS client defaults (500,000 messages / 64 MB), so an overloaded Service
// starts dropping and logging early instead of hoarding messages that other
// Services of the queue group could process.
const (
	DefaultPendingMsgs  = 10_000
	DefaultPendingBytes = 64 * 1024 * 1024
)

// Option configures a Service.
type Option func(*config)

type config struct {
	logger       *slog.Logger
	pendingMsgs  int
	pendingBytes int
	version      string
	netOpts      []fbp.Option
	logLevel     slog.Level
	logHistory   int
}

// WithLogger sets the logger of the Service and its Instance. The Service
// wraps its handler: every record still goes to l, and records at or above
// the level set with WithLogLevel are also published on the Log subject (see
// Service.Logger).
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithLogLevel sets the lowest level of the log records published on the
// Log subject. The default is slog.LevelInfo.
func WithLogLevel(l slog.Level) Option {
	return func(c *config) { c.logLevel = l }
}

// WithLogHistory sets how many log records the Service keeps for the Log
// history subject; 0 keeps none. The default is DefaultLogHistory.
func WithLogHistory(n int) Option {
	return func(c *config) { c.logHistory = n }
}

// WithPendingLimits sets how many messages and bytes may wait in the NATS
// client for each Network Inport before further messages are dropped. A
// value of -1 means no limit.
func WithPendingLimits(msgs, bytes int) Option {
	return func(c *config) { c.pendingMsgs, c.pendingBytes = msgs, bytes }
}

// WithVersion sets the SemVer version the Service reports on $SRV. The
// default is DefaultVersion.
func WithVersion(v string) Option {
	return func(c *config) { c.version = v }
}

// WithNetworkOptions passes options on to fbp.Network.Start. fbp.WithLogger
// and fbp.WithOutput are set by the Service and must not be given here.
func WithNetworkOptions(opts ...fbp.Option) Option {
	return func(c *config) { c.netOpts = append(c.netOpts, opts...) }
}

// Service receives Packets on the subjects of the Network's Inports and
// publishes every Packet leaving a Network Outport on that Outport's subject.
type Service struct {
	nc      *nats.Conn
	base    string
	logger  *slog.Logger
	inst    *fbp.Instance
	inports []*inport // fbp.In first
	obsSub  *nats.Subscription
	logSub  *nats.Subscription // Log history requests
	id      string             // identifies this Service in published Events and on $SRV
	stats   counters
	disc    *discovery

	stopCtxWatch func() bool // stops the context.AfterFunc set by Start
	unsubOnce    sync.Once
	unsubErr     error
}

// Subject returns the base subject fbp.<packagepath>.<networkname>. Every
// "/" in packagePath separates a subject token and every "." inside a path
// element becomes "_", e.g. "github.com/acme/flows" -> "github_com.acme.flows".
// packagePath must not contain "_", otherwise e.g. "github_com/acme" and
// "github.com/acme" would get the same subject and share one queue group.
func Subject(packagePath, networkName string) (string, error) {
	if strings.Contains(packagePath, "_") {
		return "", fmt.Errorf("service: package path %q must not contain \"_\"", packagePath)
	}
	var tokens []string
	for _, elem := range strings.Split(packagePath, "/") {
		tokens = append(tokens, strings.ReplaceAll(elem, ".", "_"))
	}
	tokens = append(tokens, networkName)
	for _, t := range tokens {
		if !validToken(t) {
			return "", fmt.Errorf("service: invalid subject token %q in %q/%q", t, packagePath, networkName)
		}
	}
	return "fbp." + strings.Join(tokens, "."), nil
}

// validToken reports whether t can be used as one NATS subject token.
func validToken(t string) bool {
	return t != "" && !strings.ContainsAny(t, " \t\r\n*>.")
}

// InportSubject returns the subject of the Network's Inport fbp.In.
func InportSubject(base string) string { return base + ".ip.in" }

// InportSubjectFor returns the subject of the Network's Inport inport:
// <base>.ip.in for fbp.In, <base>.ip.<inport> for any other Inport.
func InportSubjectFor(base, inport string) string {
	if inport == fbp.In {
		return InportSubject(base)
	}
	return base + ".ip." + inport
}

// OutportSubject returns the subject on which a Packet of the given Run ID
// leaves a Network Outport. A runID of "*" subscribes to all Runs.
func OutportSubject(base, outport, runID string) string {
	return base + ".op." + outport + "." + runID
}

// ObserveSubject returns the subject that switches observing on (message
// data `true`) or off (`false`) in every Service of the Network.
func ObserveSubject(base string) string { return base + ".observe" }

// DoneSubject returns the subject on which the Service announces that the
// Run runID is finished, with DoneData as message data.
func DoneSubject(base, runID string) string { return base + ".done." + runID }

// DoneData is the message data on the Done subject. Outputs is the number of
// Packets the Run published on Outport subjects before; a client that has
// received fewer of them must wait for the rest.
type DoneData struct {
	Outputs int `json:"outputs"`
}

// TraceSubject returns the subject on which observed Events are published as
// EventData.
func TraceSubject(base string) string { return base + ".trace" }

// EventData is the JSON form of an fbp.Event published on the Trace subject.
// Packet IDs are counted per Instance, so Events of different Services can
// only be told apart by Service and joined by RunID.
type EventData struct {
	Service  string          `json:"service"`
	Kind     string          `json:"kind"`
	Time     time.Time       `json:"time"`
	RunID    string          `json:"run_id"`
	PacketID uint64          `json:"packet_id,omitempty"`
	ParentID uint64          `json:"parent_id,omitempty"`
	Network  string          `json:"network"`
	Node     string          `json:"node,omitempty"`
	Port     string          `json:"port,omitempty"`
	Copy     int             `json:"copy"`
	Data     json.RawMessage `json:"data,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// Start starts an Instance of net and subscribes to the subjects of its
// Inports. packagePath is the Go package path of the Network, e.g.
// "gofbp/examples/text". Every Inport and Outport name of net must be a valid
// subject token as well. An extra Inport must not be named "in", the subject
// token of fbp.In, nor "observe", the name of the Observe endpoint on $SRV. Observing is off unless switched on by a message on the
// Observe subject or by fbp.WithObserver in WithNetworkOptions. When Start
// returns, the NATS server knows all subscriptions of the Service. When ctx is
// done, the Service unsubscribes from all subjects, also from $SRV; Stop must
// still be called.
func Start(ctx context.Context, nc *nats.Conn, packagePath string, net *fbp.Network, opts ...Option) (*Service, error) {
	base, err := Subject(packagePath, net.Name)
	if err != nil {
		return nil, err
	}
	for _, name := range net.Inports {
		if !validToken(name) || name == InportEndpoint || name == ObserveEndpoint {
			return nil, fmt.Errorf("service: inport %q of network %q is not a valid subject token or is reserved", name, net.Name)
		}
	}
	for _, name := range net.Outports {
		if !validToken(name) {
			return nil, fmt.Errorf("service: outport %q of network %q is not a valid subject token", name, net.Name)
		}
	}
	cfg := config{logger: slog.Default(), pendingMsgs: DefaultPendingMsgs, pendingBytes: DefaultPendingBytes,
		version: DefaultVersion, logLevel: slog.LevelInfo, logHistory: DefaultLogHistory}
	for _, o := range opts {
		o(&cfg)
	}
	s := &Service{nc: nc, base: base, id: fbp.NewRunID()}
	// The ID exists before the logger, so the records of Start carry it too.
	sink := &logSink{nc: nc, subject: LogSubject(base, s.id), service: s.id, level: cfg.logLevel, size: cfg.logHistory}
	s.logger = slog.New(&logHandler{inner: innerHandler(cfg.logger), sink: sink})
	s.stats.outports = map[string]int{}
	s.stats.inports = map[string]*inportCounters{}
	for _, name := range append([]string{fbp.In}, net.Inports...) {
		if s.stats.inports[name] == nil {
			s.inports = append(s.inports, &inport{name: name})
			s.stats.inports[name] = &inportCounters{}
		}
	}

	netOpts := append(cfg.netOpts, fbp.WithLogger(s.logger), fbp.WithOutput(s.publish), fbp.WithRunFinished(s.finish))
	if s.inst, err = net.Start(ctx, netOpts...); err != nil {
		return nil, err
	}
	// Not a queue subscription: every Service of the Network has to switch.
	s.obsSub, err = nc.Subscribe(ObserveSubject(base), s.handleObserve)
	if err == nil {
		s.logSub, err = nc.Subscribe(LogHistorySubject(base, s.id), sink.handleHistory)
	}
	for _, in := range s.inports {
		if err == nil {
			in.sub, err = nc.QueueSubscribe(InportSubjectFor(base, in.name), base, func(msg *nats.Msg) { s.handle(ctx, in, msg) })
		}
		if err == nil {
			err = in.sub.SetPendingLimits(cfg.pendingMsgs, cfg.pendingBytes)
		}
	}
	if err == nil {
		outports := []string{fbp.Success, fbp.Error}
		for _, name := range net.Outports {
			if !slices.Contains(outports, name) {
				outports = append(outports, name)
			}
		}
		inports := make([]string, len(s.inports))
		for i, in := range s.inports {
			inports[i] = in.name
		}
		err = s.startDiscovery(net.Name, packagePath, inports, outports, cfg.version)
	}
	if err == nil {
		// Once Start returns, the server must know the subscriptions;
		// otherwise a message sent on another connection right away is lost.
		err = nc.Flush()
	}
	if err != nil {
		if s.obsSub != nil {
			s.obsSub.Unsubscribe()
		}
		if s.logSub != nil {
			s.logSub.Unsubscribe()
		}
		for _, in := range s.inports {
			if in.sub != nil {
				in.sub.Unsubscribe()
			}
		}
		if s.disc != nil {
			s.disc.stop()
		}
		s.inst.Stop()
		return nil, err
	}
	// The Instance stops with ctx; without unsubscribing, the Service would
	// keep taking messages from the queue group and discard them.
	s.stopCtxWatch = context.AfterFunc(ctx, func() {
		if err := s.unsubscribe(); err != nil {
			s.logger.Warn("service: unsubscribe failed", "base", s.base, "error", err)
		}
		s.logger.Info("service: context done, unsubscribed", "base", s.base)
	})
	return s, nil
}

// SetObserver sets or replaces the function receiving the Events of the
// Service's Instance; nil switches observing off. See fbp.Instance.SetObserver.
// A later message on the Observe subject replaces fn. Events reach fn only,
// so $SRV.STATS reports observing as off.
func (s *Service) SetObserver(fn fbp.ObserverFunc) {
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	s.stats.observing = false
	s.inst.SetObserver(fn)
}

// handleObserve switches observing on or off. Message data `true` makes the
// Service publish every Event on the Trace subject, `false` stops it.
func (s *Service) handleObserve(msg *nats.Msg) {
	var on bool
	err := json.Unmarshal(msg.Data, &on)
	s.stats.mu.Lock()
	s.stats.observes++
	if err != nil {
		s.stats.obsErrors++
		s.stats.obsLastErr = "invalid observe message, expected true or false"
	}
	if err != nil {
		s.stats.mu.Unlock()
		// Every Service of the Network logs this, so keep the record small.
		s.logger.Warn("service: invalid observe message, expected true or false",
			"subject", msg.Subject, "data", truncate(string(msg.Data), maxLoggedData), "size", len(msg.Data))
		return
	}
	// Switched under the lock, so the reported state matches the Observer
	// even when two observe messages race.
	s.stats.observing = on
	if on {
		s.inst.SetObserver(s.publishEvent)
	} else {
		s.inst.SetObserver(nil)
	}
	s.stats.mu.Unlock()
	s.logger.Info("service: observing switched", "subject", msg.Subject, "on", on)
}

// publishEvent is the fbp.ObserverFunc set by handleObserve. Like publish it
// never waits for handle.
func (s *Service) publishEvent(e fbp.Event) {
	ed := EventData{
		Service: s.id, Kind: e.Kind.String(), Time: e.Time, RunID: e.RunID,
		PacketID: e.PacketID, ParentID: e.ParentID, Network: e.Network,
		Node: e.Node, Port: e.Port, Copy: e.Copy, Data: e.Data,
	}
	if e.Err != nil {
		ed.Error = e.Err.Error()
	}
	msg := nats.NewMsg(TraceSubject(s.base))
	msg.Data, _ = json.Marshal(ed)
	msg.Header.Set(RunIDHeader, e.RunID)
	if err := s.nc.PublishMsg(msg); err != nil {
		s.logger.Error("service: publish failed, event lost", "subject", msg.Subject, "run_id", e.RunID, "error", err)
	}
}

// truncate returns s cut to at most n bytes, marked with "…" when cut. It
// does not split a UTF-8 character.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

// inport is the subscription of one Network Inport.
type inport struct {
	name    string // name of the Network Inport
	sub     *nats.Subscription
	dropped int // last value of sub.Dropped() that was logged; used by handle only
}

// Logger returns the logger of the Service: records go to the logger given
// with WithLogger and, from the level set with WithLogLevel, also to the Log
// subject. Use it in main and in Bricks, e.g. slog.SetDefault(s.Logger()),
// so their records reach gofbpSRVLS and other readers of the Log subject.
func (s *Service) Logger() *slog.Logger { return s.logger }

// Base returns the base subject of the Service.
func (s *Service) Base() string { return s.base }

// ID returns the ID of the Service, reported on $SRV and as
// EventData.Service.
func (s *Service) ID() string { return s.id }

// handle passes a message to the Network Inport in. It blocks while the
// Instance's Inport buffers are full; NATS then queues further messages up
// to the pending limits. NATS calls handle from one goroutine per
// subscription, possibly before Start has assigned in.sub, so handle uses
// msg.Sub instead.
func (s *Service) handle(ctx context.Context, in *inport, msg *nats.Msg) {
	s.logDropped(in, msg.Sub)
	s.stats.mu.Lock()
	s.stats.inports[in.name].received++
	s.stats.mu.Unlock()
	runID := msg.Header.Get(RunIDHeader)
	if runID != "" && (len(runID) > MaxRunIDLength || !validToken(runID)) {
		reason := fmt.Sprintf("run ID %q is not a valid subject token of at most %d bytes", truncate(runID, MaxRunIDLength), MaxRunIDLength)
		s.reject(ctx, in.name, reason, fbp.NewRunID())
		return
	}
	if !json.Valid(msg.Data) {
		// A Run going on is not touched: its Done message would come too
		// early, and a caller counting its Packets would stop short.
		if runID == "" || s.inst.RunGoingOn(runID) {
			runID = fbp.NewRunID()
		}
		s.reject(ctx, in.name, "packet data is not valid JSON", runID)
		return
	}
	_, err := s.inst.SendTo(ctx, in.name, fbp.Packet{Data: msg.Data, RunID: runID})
	if err != nil && !errors.Is(err, fbp.ErrStopped) {
		s.stats.inError(in.name, "packet not delivered: "+err.Error())
		s.logger.Warn("service: packet not delivered", "subject", msg.Subject, "run_id", runID, "error", err)
	}
}

// reject counts an error of the Network Inport inport and publishes it as an
// Error Packet with the given Run ID, without passing the message to the
// Instance.
func (s *Service) reject(ctx context.Context, inport, reason, runID string) {
	s.stats.inError(inport, reason)
	data, _ := json.Marshal(fbp.ErrorData{Message: "service: " + reason})
	s.publish(ctx, fbp.Error, fbp.Packet{Data: data, RunID: runID})
	s.finish(runID, 1)
}

// logDropped logs messages the NATS client dropped from sub, the
// subscription of in, since the last call because the pending limits were
// exceeded.
func (s *Service) logDropped(in *inport, sub *nats.Subscription) {
	n, err := sub.Dropped()
	if err != nil || n <= in.dropped {
		return
	}
	s.logger.Warn("service: slow consumer, messages dropped",
		"subject", sub.Subject, "dropped", n-in.dropped, "dropped_total", n)
	in.dropped = n
}

// publish is the fbp.OutputFunc of the Instance. nc.PublishMsg only waits
// while the client writes a full outbound buffer to the socket; it never
// waits for handle, so it cannot deadlock with Send.
func (s *Service) publish(_ context.Context, outport string, p fbp.Packet) {
	msg := nats.NewMsg(OutportSubject(s.base, outport, p.RunID))
	msg.Data = p.Data
	if p.RunID != "" {
		msg.Header.Set(RunIDHeader, p.RunID)
	}
	s.stats.mu.Lock()
	s.stats.outports[outport]++
	s.stats.mu.Unlock()
	if err := s.nc.PublishMsg(msg); err != nil {
		s.logger.Error("service: publish failed, packet lost", "subject", msg.Subject, "run_id", p.RunID, "error", err)
	}
}

// finish is the fbp.RunFinishedFunc of the Instance. It publishes on the same
// connection as publish, after all Outputs of the Run.
func (s *Service) finish(runID string, outputs int) {
	msg := nats.NewMsg(DoneSubject(s.base, runID))
	msg.Data, _ = json.Marshal(DoneData{Outputs: outputs})
	msg.Header.Set(RunIDHeader, runID)
	if err := s.nc.PublishMsg(msg); err != nil {
		s.logger.Error("service: publish failed, run end lost", "subject", msg.Subject, "run_id", runID, "error", err)
	}
}

// Stop unsubscribes, also from $SRV, stops the Instance and flushes published Packets.
// Messages not yet passed to the Network and Packets not yet processed are
// lost.
func (s *Service) Stop() error {
	s.stopCtxWatch()
	err := s.unsubscribe()
	s.inst.Stop()
	if ferr := s.nc.Flush(); ferr != nil && !errors.Is(ferr, nats.ErrConnectionClosed) {
		err = errors.Join(err, ferr)
	}
	return err
}

// unsubscribe removes all subscriptions of the Service, also from $SRV. It is
// called by Stop and when the ctx given to Start is done, whichever comes
// first; later calls return the result of the first one.
func (s *Service) unsubscribe() error {
	s.unsubOnce.Do(func() {
		errs := []error{s.obsSub.Unsubscribe(), s.logSub.Unsubscribe(), s.disc.stop()}
		for _, in := range s.inports {
			errs = append(errs, in.sub.Unsubscribe())
		}
		s.unsubErr = errors.Join(errs...)
	})
	return s.unsubErr
}
