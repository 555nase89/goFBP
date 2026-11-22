package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Defaults of JetStreamConfig.
const (
	DefaultAckWait    = 30 * time.Second
	DefaultMaxDeliver = 5
	DefaultPrefetch   = 10
)

// JetStreamConfig configures a Service that takes the messages for its
// Network Inports from a JetStream stream instead of a core NATS queue group.
// See WithJetStream.
type JetStreamConfig struct {
	// Stream is the name of the stream that stores the messages sent to the
	// Network Inports. Empty means StreamName(base). If the stream does not
	// exist, Start creates it with work queue retention, file storage, one
	// replica and the subjects <base>.ip.>. An existing stream is used as it
	// is; it must store the subjects of all Network Inports.
	Stream string
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
}

// WithJetStream makes the Service take the messages for its Network Inports
// from the JetStream stream c.Stream, one durable pull consumer per Inport
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
	js, err := jetstream.New(s.nc)
	if err != nil {
		return err
	}
	if err := ensureStream(ctx, js, c.Stream, s.base); err != nil {
		return err
	}
	s.stream = c.Stream
	s.acks = &acks{msgs: map[jetstream.Msg]struct{}{}, runs: map[string][]jetstream.Msg{}}
	s.progressStop = make(chan struct{})
	ackWait := time.Duration(0)
	for _, in := range s.inports {
		in.consumer = ConsumerName(s.base, in.name)
		cons, err := ensureConsumer(ctx, js, c.Stream, jetstream.ConsumerConfig{
			Durable:       in.consumer,
			Description:   "goFBP Inport " + in.name + " of " + s.base,
			FilterSubject: InportSubjectFor(s.base, in.name),
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       c.AckWait,
			MaxDeliver:    c.MaxDeliver,
		})
		if err != nil {
			return err
		}
		// An existing consumer keeps its own AckWait.
		if w := cons.CachedInfo().Config.AckWait; w > 0 && (ackWait == 0 || w < ackWait) {
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
// terminated, so it is not delivered again; one the Instance did not take is
// given back (Nak); one in the Network is acknowledged when its Run is
// finished.
func (s *Service) handleJetStream(ctx context.Context, in *inport, msg jetstream.Msg) {
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 1 {
		s.stats.mu.Lock()
		s.stats.inports[in.name].redelivered++
		s.stats.mu.Unlock()
	}
	s.acks.add(msg)
	runID, res := s.receive(ctx, in, msg.Subject(), msg.Headers(), msg.Data())
	switch res {
	case rejected:
		if s.acks.remove(msg) {
			s.settle(msg, "term", msg.Term)
		}
	case notDelivered:
		if s.acks.remove(msg) {
			s.settle(msg, "nak", msg.Nak)
		}
	case delivered:
		if s.acks.sent(msg, runID, s.inst.RunGoingOn) {
			s.settle(msg, "ack", msg.Ack)
		}
	}
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
// gets it at once instead of after AckWait. The Inport iterators must be
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
	})
}

// acks keeps the JetStream messages of a Service that are not yet
// acknowledged. A message delivered to the Network waits for the end of the
// Run it entered with. A nil *acks keeps nothing.
type acks struct {
	mu   sync.Mutex
	msgs map[jetstream.Msg]struct{} // all messages not yet acknowledged
	runs map[string][]jetstream.Msg // messages in the Network by Run ID
}

func (a *acks) add(m jetstream.Msg) {
	a.mu.Lock()
	a.msgs[m] = struct{}{}
	a.mu.Unlock()
}

// remove reports whether m was still kept and forgets it.
func (a *acks) remove(m jetstream.Msg) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.msgs[m]
	delete(a.msgs, m)
	return ok
}

// sent records that m entered the Network with runID. If the Run is already
// finished, sent forgets m and reports true: the caller acknowledges it.
// goingOn is called under the lock that finished takes after the Instance
// has removed the Run, so a Run cannot finish unnoticed in between.
func (a *acks) sent(m jetstream.Msg, runID string, goingOn func(string) bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.msgs[m]; !ok {
		return false // given back by release meanwhile
	}
	if goingOn(runID) {
		a.runs[runID] = append(a.runs[runID], m)
		return false
	}
	delete(a.msgs, m)
	return true
}

// finished forgets and returns the messages waiting for the Run runID.
func (a *acks) finished(runID string) []jetstream.Msg {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ms := a.runs[runID]
	delete(a.runs, runID)
	for _, m := range ms {
		delete(a.msgs, m)
	}
	return ms
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
	return ms
}
