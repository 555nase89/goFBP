package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Defaults of JetStreamConfig.
const (
	DefaultAckWait    = 30 * time.Second
	DefaultMaxDeliver = 5
	DefaultPrefetch   = 10
)

// deadLetterTimeout is how long a Service waits for the server to confirm
// that it stored a message on the Dead letter subject.
const deadLetterTimeout = 10 * time.Second

// Headers of a message on the Dead letter subject, in addition to the headers
// of the original message.
const (
	// ErrorHeader carries the fbp.ErrorData of the failed Run as JSON,
	// without its input (that is the message data).
	ErrorHeader = "Fbp-Error"
	// SubjectHeader, StreamHeader, SequenceHeader and DeliveredHeader say
	// where the message came from and how often it was delivered.
	SubjectHeader   = "Fbp-Subject"
	StreamHeader    = "Fbp-Stream"
	SequenceHeader  = "Fbp-Sequence"
	DeliveredHeader = "Fbp-Delivered"
)

// maxErrorHeader is the longest ErrorHeader value, in bytes.
const maxErrorHeader = 1024

// ErrorPolicy says what a Service with WithJetStream does with a message
// whose Run ended with a Packet on the Network's Error Outport.
type ErrorPolicy int

const (
	// ErrorAck acknowledges the message like any other; the Error Packet on
	// the Error subject is the only trace of the failure. The default.
	ErrorAck ErrorPolicy = iota
	// ErrorNak gives the message back, so JetStream delivers it again after
	// NakDelay. On its last delivery (MaxDeliver of the consumer) it is put
	// on the Dead letter subject and terminated instead.
	ErrorNak
	// ErrorTerm puts the message on the Dead letter subject and terminates
	// it at once, so it is not delivered again.
	ErrorTerm
)

// InportSource says where a Service with WithJetStream takes the messages for
// one Network Inport from. The zero value is the default: the stream of
// JetStreamConfig.Stream, filtered on the Inport subject.
type InportSource struct {
	// Stream is the name of the stream the consumer of the Inport reads.
	// Empty means JetStreamConfig.Stream. Start creates only that stream; a
	// different stream named here must exist, e.g. the event stream of an
	// application, which a Service only reads.
	Stream string
	// FilterSubject selects the messages of the stream the Inport gets, e.g.
	// "events.order.>". Empty means the Inport subject,
	// InportSubjectFor(base, inport). A consumer that exists must have the
	// same filter subject.
	FilterSubject string
}

// JetStreamConfig configures a Service that takes the messages for its
// Network Inports from JetStream streams instead of a core NATS queue group.
// See WithJetStream.
type JetStreamConfig struct {
	// Stream is the name of the stream that stores the messages sent to the
	// Network Inports. Empty means StreamName(base). If the stream does not
	// exist and an Inport reads it, Start creates it with work queue
	// retention, file storage, one replica and the subjects <base>.ip.>. An
	// existing stream is used as it is; it must store the subjects of all
	// Network Inports that read it.
	Stream string
	// Inports sets, by Network Inport name, a stream and filter subject
	// other than the default for that Inport. Inports not listed read the
	// default.
	Inports map[string]InportSource
	// AckWait is how long JetStream waits for the acknowledgement of a
	// message before it delivers the message again. While a Run is going
	// on, the Service tells JetStream every AckWait/3 that its messages are
	// still in progress. Default DefaultAckWait. Used only when Start
	// creates the consumer.
	AckWait time.Duration
	// MaxDeliver is how often JetStream delivers a message at most, e.g. one
	// that keeps crashing the Service; -1 means no limit. Default
	// DefaultMaxDeliver. Used only when Start creates the consumer.
	MaxDeliver int
	// Prefetch is how many messages the Service fetches ahead per Network
	// Inport. They wait in the NATS client, where nobody reports them in
	// progress, so Prefetch times the time a Packet needs must stay well
	// below AckWait. Default DefaultPrefetch.
	Prefetch int
	// OnError says what becomes of a message whose Run ended with a Packet
	// on the Network's Error Outport. Default ErrorAck.
	OnError ErrorPolicy
	// NakDelay is how long JetStream waits before it delivers a message
	// again that was given back with ErrorNak. 0 delivers it at once.
	NakDelay time.Duration
	// DeadLetter is the subject on which a message that failed for good is
	// published before it is terminated (ErrorNak on the last delivery,
	// ErrorTerm, and rejected messages unless OnError is ErrorAck). A stream
	// must store it. Empty: such messages are only logged and terminated.
	DeadLetter string
	// Envelope makes the Service pass an Envelope as Packet data instead of
	// the message data, so the Network also gets the subject, stream
	// sequence and headers of the message.
	Envelope bool
}

// Envelope is the Packet data a Service with JetStreamConfig.Envelope passes
// to the Network: the message data together with where it is stored.
type Envelope struct {
	Subject   string              `json:"subject"`
	Stream    string              `json:"stream,omitempty"`
	Sequence  uint64              `json:"sequence,omitempty"`  // stream sequence
	Time      time.Time           `json:"time,omitzero"`       // when the stream stored the message
	Delivered uint64              `json:"delivered,omitempty"` // 1 on the first delivery
	Header    map[string][]string `json:"header,omitempty"`
	Data      json.RawMessage     `json:"data"`
}

// WithJetStream makes the Service take the messages for its Network Inports
// from JetStream streams, one durable pull consumer per Inport
// (ConsumerName) shared by all Services of the Network, instead of a core
// NATS queue group. A message is acknowledged once its Run is finished, so
// a message whose Run did not finish, e.g. because the Service stopped or
// crashed, is delivered again, possibly to another Service. A Run can thus
// be processed more than once, and its Packets and Done message published
// more than once. WithPendingLimits does not apply.
func WithJetStream(c JetStreamConfig) Option {
	return func(cfg *config) { cfg.js = &c }
}

// StreamName returns the name of the stream a Service with WithJetStream
// uses unless JetStreamConfig.Stream is set, e.g.
// "fbp_gofbp_examples_text_pipeline_bf241440", like ServiceName.
func StreamName(base string) string { return ServiceName(base) }

// ConsumerName returns the name of the durable consumer that the Services of
// the Network with the base subject base share for the Network Inport
// inport, e.g. "fbp_gofbp_examples_text_pipeline_ip_in_<hash>".
func ConsumerName(base, inport string) string { return ServiceName(InportSubjectFor(base, inport)) }

// startJetStream binds every Network Inport to its consumer and starts one
// goroutine per Inport that passes the messages to the Network, plus one
// that reports the messages in progress.
func (s *Service) startJetStream(ctx context.Context, c JetStreamConfig) error {
	if c.Stream == "" {
		c.Stream = StreamName(s.base)
	}
	if c.AckWait <= 0 {
		c.AckWait = DefaultAckWait
	}
	if c.MaxDeliver == 0 {
		c.MaxDeliver = DefaultMaxDeliver
	}
	if c.Prefetch <= 0 {
		c.Prefetch = DefaultPrefetch
	}
	if c.OnError < ErrorAck || c.OnError > ErrorTerm {
		return fmt.Errorf("service: unknown error policy %d", c.OnError)
	}
	for name := range c.Inports {
		if !slices.ContainsFunc(s.inports, func(in *inport) bool { return in.name == name }) {
			return fmt.Errorf("service: JetStreamConfig.Inports names %q, which is not an Inport of the Network", name)
		}
	}
	js, err := jetstream.New(s.nc)
	if err != nil {
		return err
	}
	if c.DeadLetter != "" {
		if _, err := js.StreamNameBySubject(ctx, c.DeadLetter); err != nil {
			return fmt.Errorf("service: no stream stores the dead letter subject %q: %w", c.DeadLetter, err)
		}
	}
	s.js, s.jsc = js, &c
	s.acks = &acks{msgs: map[jetstream.Msg]struct{}{}, runs: map[string][]jetstream.Msg{},
		entering: map[string]int{}, errs: map[string]json.RawMessage{}}
	s.maxDeliver = map[string]int{}
	s.progressStop = make(chan struct{})
	ackWait := time.Duration(0)
	for _, in := range s.inports {
		src := c.Inports[in.name]
		in.stream = src.Stream
		if in.stream == "" {
			in.stream = c.Stream
		}
		if src.FilterSubject != "" {
			in.subject = src.FilterSubject
		}
		if in.stream == c.Stream {
			if err := ensureStream(ctx, js, c.Stream, s.base); err != nil {
				return err
			}
			s.stream = c.Stream
		} else if _, err := js.Stream(ctx, in.stream); err != nil {
			return fmt.Errorf("service: stream %q of inport %q: %w", in.stream, in.name, err)
		}
		in.consumer = ConsumerName(s.base, in.name)
		cons, err := ensureConsumer(ctx, js, in.stream, jetstream.ConsumerConfig{
			Durable:       in.consumer,
			Description:   "goFBP Inport " + in.name + " of " + s.base,
			FilterSubject: in.subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       c.AckWait,
			MaxDeliver:    c.MaxDeliver,
		})
		if err != nil {
			return err
		}
		info := cons.CachedInfo()
		if src.FilterSubject != "" && info.Config.FilterSubject != src.FilterSubject {
			return fmt.Errorf("service: consumer %q exists with filter subject %q, not %q",
				in.consumer, info.Config.FilterSubject, src.FilterSubject)
		}
		s.maxDeliver[in.consumer] = info.Config.MaxDeliver
		// An existing consumer keeps its own AckWait.
		if w := info.Config.AckWait; w > 0 && (ackWait == 0 || w < ackWait) {
			ackWait = w
		}
		if in.msgs, err = cons.Messages(jetstream.PullMaxMessages(c.Prefetch)); err != nil {
			return err
		}
		s.pulls.Go(func() { s.pull(ctx, in) })
	}
	if ackWait == 0 {
		ackWait = DefaultAckWait
	}
	go s.progress(ackWait/3, s.progressStop)
	return nil
}

// ensureStream creates the stream name for the Inport subjects of base
// unless it exists.
func ensureStream(ctx context.Context, js jetstream.JetStream, name, base string) error {
	_, err := js.Stream(ctx, name)
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        name,
		Description: "goFBP Inports of " + base,
		Subjects:    []string{base + ".ip.>"},
		Retention:   jetstream.WorkQueuePolicy,
		Storage:     jetstream.FileStorage,
	})
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		// Another Service created it meanwhile, with other settings.
		err = nil
	}
	return err
}

// ensureConsumer returns the consumer cfg.Durable of stream and creates it
// with cfg unless it exists. An existing consumer is not changed.
func ensureConsumer(ctx context.Context, js jetstream.JetStream, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	cons, err := js.Consumer(ctx, stream, cfg.Durable)
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return cons, err
	}
	cons, err = js.CreateConsumer(ctx, stream, cfg)
	if errors.Is(err, jetstream.ErrConsumerExists) {
		// Another Service created it meanwhile, with other settings.
		return js.Consumer(ctx, stream, cfg.Durable)
	}
	return cons, err
}

// pull passes the messages of the consumer of in to the Network until the
// iterator is drained.
func (s *Service) pull(ctx context.Context, in *inport) {
	for {
		msg, err := in.msgs.Next()
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			return
		}
		if err != nil {
			// e.g. missed heartbeats while the connection is down; the
			// iterator keeps pulling.
			s.logger.Warn("service: jetstream pull failed", "consumer", in.consumer, "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		s.handleJetStream(ctx, in, msg)
	}
}

// handleJetStream passes msg to the Network Inport in. A rejected message is
// terminated (or put on the Dead letter subject), so it is not delivered
// again; one the Instance did not take is given back (Nak); one in the
// Network is settled when its Run is finished.
func (s *Service) handleJetStream(ctx context.Context, in *inport, msg jetstream.Msg) {
	md, _ := msg.Metadata()
	if md != nil && md.NumDelivered > 1 {
		s.stats.mu.Lock()
		s.stats.inports[in.name].redelivered++
		s.stats.mu.Unlock()
	}
	// The Run ID must be known before the Packet enters the Network: its
	// Run may finish before sent, and its Error Packets must still be
	// matched with msg. Without one, the Service creates it instead of
	// the Instance; msg itself is not changed.
	header := msg.Headers()
	runID := header.Get(RunIDHeader)
	if runID == "" {
		runID = fbp.NewRunID()
		header = maps.Clone(header)
		if header == nil {
			header = nats.Header{}
		}
		header.Set(RunIDHeader, runID)
	}
	data := msg.Data()
	if s.jsc.Envelope && json.Valid(data) {
		data = envelope(msg, md)
	}
	s.acks.add(msg, runID)
	_, res := s.receive(ctx, in, msg.Subject(), header, data)
	switch res {
	case rejected:
		if s.acks.remove(msg, runID) {
			if s.jsc.OnError == ErrorAck {
				s.settle(msg, "term", msg.Term)
			} else {
				data, _ := json.Marshal(fbp.ErrorData{Message: "service: message rejected (invalid Run ID or JSON)"})
				s.deadLetter(msg, runID, data)
			}
		}
	case notDelivered:
		if s.acks.remove(msg, runID) {
			s.settle(msg, "nak", msg.Nak)
		}
	case delivered:
		if done, errData := s.acks.sent(msg, runID, s.inst.RunGoingOn); done {
			s.settleRun(runID, []jetstream.Msg{msg}, errData)
		}
	}
}

// envelope returns the Envelope of msg as JSON. msg.Data() must be valid
// JSON; md may be nil.
func envelope(msg jetstream.Msg, md *jetstream.MsgMetadata) json.RawMessage {
	e := Envelope{Subject: msg.Subject(), Header: msg.Headers(), Data: msg.Data()}
	if md != nil {
		e.Stream, e.Sequence, e.Time, e.Delivered = md.Stream, md.Sequence.Stream, md.Timestamp, md.NumDelivered
	}
	data, _ := json.Marshal(e)
	return data
}

// output is the fbp.OutputFunc of the Instance with WithJetStream: it notes
// a Packet on the Error Outport for the settlement of its Run and publishes
// p like publish.
func (s *Service) output(ctx context.Context, outport string, p fbp.Packet) {
	if outport == fbp.Error {
		s.acks.failed(p.RunID, p.Data)
	}
	s.publish(ctx, outport, p)
}

// settleRun settles the messages ms of the finished Run runID: it
// acknowledges them, or, if the Run ended with the Error Packet errData,
// handles them as JetStreamConfig.OnError says. It does nothing without
// WithJetStream.
func (s *Service) settleRun(runID string, ms []jetstream.Msg, errData json.RawMessage) {
	for _, m := range ms {
		switch {
		case errData == nil || s.jsc.OnError == ErrorAck:
			s.settle(m, "ack", m.Ack)
		case s.jsc.OnError == ErrorNak && !s.lastDelivery(m):
			s.logger.Warn("service: run failed, message given back", "subject", m.Subject(),
				"run_id", runID, "error", errorText(errData))
			s.settle(m, "nak", func() error { return m.NakWithDelay(s.jsc.NakDelay) })
		default:
			s.deadLetter(m, runID, errData)
		}
	}
}

// lastDelivery reports whether JetStream will not deliver m again, because
// it was delivered MaxDeliver times.
func (s *Service) lastDelivery(m jetstream.Msg) bool {
	md, err := m.Metadata()
	if err != nil {
		return false
	}
	max := s.maxDeliver[md.Consumer]
	return max > 0 && md.NumDelivered >= uint64(max)
}

// deadLetter puts m on the Dead letter subject and terminates it once the
// server confirmed that it stored it. Without a Dead letter subject, or if
// publishing fails, m is only logged: terminated in the first case, given
// back in the second.
func (s *Service) deadLetter(m jetstream.Msg, runID string, errData json.RawMessage) {
	s.logger.Error("service: run failed for good", "subject", m.Subject(), "run_id", runID,
		"dead_letter", s.jsc.DeadLetter, "error", errorText(errData))
	term := func() error { return m.TermWithReason("goFBP: run failed") }
	if s.jsc.DeadLetter == "" {
		s.settle(m, "term", term)
		return
	}
	dl := nats.NewMsg(s.jsc.DeadLetter)
	dl.Data = m.Data()
	for k, v := range m.Headers() {
		dl.Header[k] = slices.Clone(v)
	}
	dl.Header.Set(ErrorHeader, errorHeader(errData))
	dl.Header.Set(SubjectHeader, m.Subject())
	if md, err := m.Metadata(); err == nil {
		dl.Header.Set(StreamHeader, md.Stream)
		dl.Header.Set(SequenceHeader, strconv.FormatUint(md.Sequence.Stream, 10))
		dl.Header.Set(DeliveredHeader, strconv.FormatUint(md.NumDelivered, 10))
	}
	f, err := s.js.PublishMsgAsync(dl)
	if err != nil {
		s.logger.Error("service: dead letter publish failed, message given back", "subject", m.Subject(), "run_id", runID, "error", err)
		s.settle(m, "nak", m.Nak)
		return
	}
	// Not waited for here: deadLetter is called from finish, which must
	// return quickly. release waits for these goroutines.
	s.deadLetters.Go(func() {
		select {
		case <-f.Ok():
			s.settle(m, "term", term)
		case err := <-f.Err():
			s.logger.Error("service: dead letter not stored, message given back", "subject", m.Subject(), "run_id", runID, "error", err)
			s.settle(m, "nak", m.Nak)
		case <-time.After(deadLetterTimeout):
			s.logger.Error("service: dead letter not confirmed in time, message given back", "subject", m.Subject(), "run_id", runID)
			s.settle(m, "nak", m.Nak)
		}
	})
}

// errorHeader returns errData as a single-line JSON header value: an
// fbp.ErrorData without its input (that is the message data), anything else
// compacted and cut to maxErrorHeader bytes.
func errorHeader(errData json.RawMessage) string {
	var ed fbp.ErrorData
	if err := json.Unmarshal(errData, &ed); err == nil && ed.Message != "" {
		ed.Input = nil
		data, _ := json.Marshal(ed)
		return truncate(string(data), maxErrorHeader)
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, errData); err != nil {
		return ""
	}
	return truncate(buf.String(), maxErrorHeader)
}

// errorText returns the message of an fbp.ErrorData, or errData itself, for
// log records.
func errorText(errData json.RawMessage) string {
	var ed fbp.ErrorData
	if err := json.Unmarshal(errData, &ed); err == nil && ed.Message != "" {
		return ed.Message
	}
	return truncate(string(errData), maxLoggedData)
}

// settle sends one acknowledgement of kind to JetStream. Like publish it
// does not wait for the server.
func (s *Service) settle(msg jetstream.Msg, kind string, fn func() error) {
	if err := fn(); err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
		s.logger.Warn("service: jetstream "+kind+" failed, message may be delivered again",
			"subject", msg.Subject(), "run_id", msg.Headers().Get(RunIDHeader), "error", err)
	}
}

// progress tells JetStream every interval that the messages not yet
// acknowledged are still in progress, so it does not deliver them again
// while their Runs are going on.
func (s *Service) progress(interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(max(interval, 10*time.Millisecond))
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		var failed int
		var last error
		for _, m := range s.acks.pending() {
			if err := m.InProgress(); err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
				failed, last = failed+1, err
			}
		}
		if failed > 0 {
			s.logger.Warn("service: jetstream in-progress failed, messages may be delivered again", "messages", failed, "error", last)
		}
	}
}

// release waits until the pull goroutines have returned and gives every
// message not yet acknowledged back to JetStream (Nak), so another Service
// gets it at once instead of after AckWait. It then waits for the messages
// on their way to the Dead letter subject. The Inport iterators must be
// drained before. It does nothing without WithJetStream; later calls wait
// for the first one.
func (s *Service) release() {
	s.releaseOnce.Do(func() {
		if s.acks == nil {
			return
		}
		close(s.progressStop)
		s.pulls.Wait()
		for _, m := range s.acks.takeAll() {
			s.settle(m, "nak", m.Nak)
		}
		s.deadLetters.Wait()
	})
}

// acks keeps the JetStream messages of a Service that are not yet
// acknowledged. A message delivered to the Network waits for the end of the
// Run it entered with. A nil *acks keeps nothing.
type acks struct {
	mu   sync.Mutex
	msgs map[jetstream.Msg]struct{} // all messages not yet acknowledged
	runs map[string][]jetstream.Msg // messages in the Network by Run ID
	// entering counts by Run ID the messages between add and sent/remove:
	// their Packet may be in the Network already, or about to enter it.
	entering map[string]int
	// errs holds by Run ID the data of the first Error Packet of a Run,
	// until the messages of that Run are settled.
	errs map[string]json.RawMessage
}

// add keeps m, which is about to enter the Network with runID.
func (a *acks) add(m jetstream.Msg, runID string) {
	a.mu.Lock()
	a.msgs[m] = struct{}{}
	a.entering[runID]++
	a.mu.Unlock()
}

// remove reports whether m, which did not enter the Network with runID, was
// still kept and forgets it.
func (a *acks) remove(m jetstream.Msg, runID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.left(runID)
	_, ok := a.msgs[m]
	delete(a.msgs, m)
	return ok
}

// sent records that m entered the Network with runID. If the Run is already
// finished, sent forgets m and reports true with the Run's Error Packet, if
// any: the caller settles it. goingOn is called under the lock that finished
// takes after the Instance has removed the Run, so a Run cannot finish
// unnoticed in between.
func (a *acks) sent(m jetstream.Msg, runID string, goingOn func(string) bool) (bool, json.RawMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	defer a.left(runID)
	if _, ok := a.msgs[m]; !ok {
		return false, nil // given back by release meanwhile
	}
	if goingOn(runID) {
		a.runs[runID] = append(a.runs[runID], m)
		return false, nil
	}
	delete(a.msgs, m)
	return true, a.errs[runID]
}

// left stops counting one entering message of runID and forgets the Error
// Packet of the Run once no message of it is left to settle. a.mu must be
// held.
func (a *acks) left(runID string) {
	if a.entering[runID]--; a.entering[runID] <= 0 {
		delete(a.entering, runID)
		if len(a.runs[runID]) == 0 {
			delete(a.errs, runID)
		}
	}
}

// failed notes the data of an Error Packet of the Run runID; only the first
// one is kept.
func (a *acks) failed(runID string, data json.RawMessage) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.errs[runID]; !ok {
		a.errs[runID] = data
	}
}

// finished forgets and returns the messages waiting for the Run runID, with
// the Run's Error Packet, if any. The Error Packet is kept for a message of
// the Run that is still entering; sent returns it then.
func (a *acks) finished(runID string) ([]jetstream.Msg, json.RawMessage) {
	if a == nil {
		return nil, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ms := a.runs[runID]
	delete(a.runs, runID)
	for _, m := range ms {
		delete(a.msgs, m)
	}
	errData := a.errs[runID]
	if a.entering[runID] == 0 {
		delete(a.errs, runID)
	}
	return ms, errData
}

// pending returns all messages not yet acknowledged.
func (a *acks) pending() []jetstream.Msg {
	a.mu.Lock()
	defer a.mu.Unlock()
	ms := make([]jetstream.Msg, 0, len(a.msgs))
	for m := range a.msgs {
		ms = append(ms, m)
	}
	return ms
}

// takeAll forgets and returns all messages not yet acknowledged.
func (a *acks) takeAll() []jetstream.Msg {
	a.mu.Lock()
	defer a.mu.Unlock()
	ms := make([]jetstream.Msg, 0, len(a.msgs))
	for m := range a.msgs {
		ms = append(ms, m)
	}
	a.msgs = map[jetstream.Msg]struct{}{}
	a.runs = map[string][]jetstream.Msg{}
	a.entering = map[string]int{}
	a.errs = map[string]json.RawMessage{}
	return ms
}
