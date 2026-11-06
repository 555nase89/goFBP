package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// DefaultLogHistory is how many log records a Service keeps for the Log
// history subject unless set with WithLogHistory.
const DefaultLogHistory = 500

// LogSubject returns the subject on which the Service with ID id publishes
// its log records as LogData. LogSubject(base, "*") matches all Services of
// the Network.
func LogSubject(base, id string) string { return base + ".log." + id }

// LogHistorySubject returns the subject on which the Service with ID id
// answers a request with its recent log records, a JSON array of LogData,
// oldest first.
func LogHistorySubject(base, id string) string { return base + ".loghistory." + id }

// LogData is the JSON form of a log record published on the Log subject.
type LogData struct {
	Service string `json:"service"`
	// Seq counts the published records of the Service from 1, so a reader
	// can join the Log history and the Log subject without gaps or
	// duplicates.
	Seq     uint64         `json:"seq"`
	Time    time.Time      `json:"time"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Attrs   map[string]any `json:"attrs,omitempty"`
}

// innerHandler returns the handler a Service passes its records on to. slog's
// built-in default handler writes through the log package; once
// slog.SetDefault(s.Logger()) has redirected the log package to slog, it
// would call back into itself and deadlock. It is therefore replaced by a
// text handler writing to stderr.
func innerHandler(l *slog.Logger) slog.Handler {
	h := l.Handler()
	if fmt.Sprintf("%T", h) == "*slog.defaultHandler" {
		return slog.NewTextHandler(os.Stderr, nil)
	}
	return h
}

// logSink publishes the log records of one Service and keeps the most
// recent ones. It is shared by all logHandlers derived from one another.
type logSink struct {
	nc      *nats.Conn
	subject string
	service string
	level   slog.Level

	mu     sync.Mutex
	seq    uint64
	recent []LogData // ring, oldest at next once full
	next   int
	size   int
}

func (k *logSink) add(d LogData) LogData {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.seq++
	d.Seq = k.seq
	if k.size <= 0 {
		return d
	}
	if len(k.recent) < k.size {
		k.recent = append(k.recent, d)
	} else {
		k.recent[k.next] = d
		k.next = (k.next + 1) % k.size
	}
	return d
}

// history returns the recent log records, oldest first.
func (k *logSink) history() []LogData {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append(append([]LogData{}, k.recent[k.next:]...), k.recent[:k.next]...)
}

// handleHistory answers a request on the Log history subject.
func (k *logSink) handleHistory(msg *nats.Msg) {
	data, _ := json.Marshal(k.history())
	// Not logged on failure: logging here would publish again.
	msg.Respond(data)
}

// logHandler passes every record on to the handler of the logger given with
// WithLogger and publishes records at or above the sink's level on the Log
// subject. Attributes and groups added with WithAttrs and WithGroup are
// flattened into LogData.Attrs as "group.key".
type logHandler struct {
	inner  slog.Handler
	sink   *logSink
	attrs  map[string]any // from WithAttrs, keys already prefixed
	prefix string         // groups from WithGroup, e.g. "req."
}

func (h *logHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.sink.level || h.inner.Enabled(ctx, l)
}

func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	var err error
	if h.inner.Enabled(ctx, r.Level) {
		err = h.inner.Handle(ctx, r)
	}
	if r.Level < h.sink.level {
		return err
	}
	attrs := make(map[string]any, len(h.attrs)+r.NumAttrs())
	for k, v := range h.attrs {
		attrs[k] = v
	}
	r.Attrs(func(a slog.Attr) bool {
		addAttr(attrs, h.prefix, a)
		return true
	})
	if len(attrs) == 0 {
		attrs = nil
	}
	// An attribute value JSON cannot encode: keep its text instead. This must
	// happen before add, because the Log history shares attrs from then on.
	if _, merr := json.Marshal(attrs); merr != nil {
		for k, v := range attrs {
			attrs[k] = fmt.Sprint(v)
		}
	}
	d := h.sink.add(LogData{Service: h.sink.service, Time: r.Time, Level: r.Level.String(), Message: r.Message, Attrs: attrs})
	data, _ := json.Marshal(d)
	// Errors are not logged: logging here would publish again.
	h.sink.nc.Publish(h.sink.subject, data)
	return err
}

func (h *logHandler) WithAttrs(as []slog.Attr) slog.Handler {
	attrs := make(map[string]any, len(h.attrs)+len(as))
	for k, v := range h.attrs {
		attrs[k] = v
	}
	for _, a := range as {
		addAttr(attrs, h.prefix, a)
	}
	return &logHandler{inner: h.inner.WithAttrs(as), sink: h.sink, attrs: attrs, prefix: h.prefix}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &logHandler{inner: h.inner.WithGroup(name), sink: h.sink, attrs: h.attrs, prefix: h.prefix + name + "."}
}

// addAttr adds a to attrs under prefix+key; groups add their members.
func addAttr(attrs map[string]any, prefix string, a slog.Attr) {
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		p := prefix
		if a.Key != "" {
			p += a.Key + "."
		}
		for _, ga := range v.Group() {
			addAttr(attrs, p, ga)
		}
		return
	}
	if a.Key == "" {
		return
	}
	attrs[prefix+a.Key] = attrValue(v)
}

// attrValue returns the JSON form of a resolved, non-group value.
func attrValue(v slog.Value) any {
	switch v.Kind() {
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindTime:
		return v.Time()
	case slog.KindAny:
		if err, ok := v.Any().(error); ok {
			return err.Error()
		}
		return v.Any()
	default:
		return v.Any()
	}
}
