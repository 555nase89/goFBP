package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// DefaultVersion is the version a Service reports on $SRV unless set with
// WithVersion.
const DefaultVersion = "0.0.0"

// Endpoint names reported in $SRV.INFO and $SRV.STATS. An extra Network
// Inport is reported under its own name.
const (
	InportEndpoint  = "in"
	ObserveEndpoint = "observe"
)

// ServiceName returns the name a Service reports on $SRV for base, e.g.
// "fbp.gofbp.examples.text.pipeline" ->
// "fbp_gofbp_examples_text_pipeline_<hash>". Micro names may only contain
// letters, digits, "-" and "_", so every other character becomes "_". That
// alone is ambiguous ("fbp.a_b.c" and "fbp.a.b_c" would both become
// "fbp_a_b_c"), so the first 8 hex digits of the SHA-256 of base are
// appended.
func ServiceName(base string) string {
	sum := sha256.Sum256([]byte(base))
	return strings.Map(func(r rune) rune {
		if r == '-' || r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			return r
		}
		return '_'
	}, base) + "_" + hex.EncodeToString(sum[:4])
}

// InportStats is the Data of an Inport endpoint in $SRV.STATS.
type InportStats struct {
	// Dropped counts messages the NATS client dropped because the pending
	// limits were exceeded.
	Dropped int `json:"dropped"`
	// Outports counts the Packets published per Network Outport, for all
	// Inports together. It is reported on the endpoint of fbp.In only.
	Outports map[string]int `json:"outports,omitempty"`
}

// ObserveStats is the Data of the Observe endpoint in $SRV.STATS.
type ObserveStats struct {
	// Observing is true while the Service publishes the Events of its
	// Instance on the Trace subject.
	Observing bool `json:"observing"`
}

// counters holds the statistics reported on $SRV.STATS. handle, publish and
// handleObserve run in different goroutines than the STATS responder.
type counters struct {
	mu         sync.Mutex
	inports    map[string]*inportCounters // by Network Inport
	observes   int
	obsErrors  int
	obsLastErr string
	observing  bool // Events are published on the Trace subject
	outports   map[string]int
}

type inportCounters struct {
	received  int
	errors    int
	lastError string
}

func (c *counters) inError(inport, err string) {
	c.mu.Lock()
	in := c.inports[inport]
	in.errors++
	in.lastError = err
	c.mu.Unlock()
}

// discovery answers the NATS micro discovery protocol ($SRV.PING, $SRV.INFO,
// $SRV.STATS) for one Service, so tools like `nats micro ls` find it. It
// does not use micro.AddService: micro stops the whole service on any
// asynchronous subscription error, including a slow consumer, and replaces
// the connection's error and closed handlers. A Service has to keep running
// and only drop messages instead.
type discovery struct {
	s           *Service
	name        string
	version     string
	description string
	metadata    map[string]string
	started     time.Time
	subs        []*nats.Subscription
}

// startDiscovery subscribes to the $SRV control subjects for all services,
// for this service name and for this Service's ID. It must be called after
// the Inport subscriptions and s.obsSub are set.
func (s *Service) startDiscovery(net, packagePath string, inports, outports []string, version string) error {
	d := &discovery{
		s:           s,
		name:        ServiceName(s.base),
		version:     version,
		description: fmt.Sprintf("FBP network %s of package %s", net, packagePath),
		metadata: map[string]string{
			"package":  packagePath,
			"network":  net,
			"base":     s.base,
			"inports":  strings.Join(inports, ","),
			"outports": strings.Join(outports, ","),
			"trace":    TraceSubject(s.base),
			"log":      LogSubject(s.base, s.id),
		},
		started: time.Now().UTC(),
	}
	s.disc = d
	for _, verb := range []micro.Verb{micro.PingVerb, micro.InfoVerb, micro.StatsVerb} {
		for _, ids := range [][2]string{{"", ""}, {d.name, ""}, {d.name, s.id}} {
			subject, err := micro.ControlSubject(verb, ids[0], ids[1])
			if err != nil {
				return err
			}
			sub, err := s.nc.Subscribe(subject, d.handler(verb))
			if err != nil {
				return err
			}
			d.subs = append(d.subs, sub)
		}
	}
	return nil
}

// stop unsubscribes from all control subjects.
func (d *discovery) stop() error {
	var err error
	for _, sub := range d.subs {
		err = errors.Join(err, sub.Unsubscribe())
	}
	return err
}

func (d *discovery) handler(verb micro.Verb) nats.MsgHandler {
	return func(msg *nats.Msg) {
		var resp any
		switch verb {
		case micro.PingVerb:
			resp = micro.Ping{ServiceIdentity: d.identity(), Type: micro.PingResponseType}
		case micro.InfoVerb:
			resp = d.info()
		case micro.StatsVerb:
			resp = d.stats()
		}
		data, _ := json.Marshal(resp)
		if err := msg.Respond(data); err != nil {
			d.s.logger.Warn("service: discovery response failed", "subject", msg.Subject, "error", err)
		}
	}
}

func (d *discovery) identity() micro.ServiceIdentity {
	return micro.ServiceIdentity{Name: d.name, ID: d.s.id, Version: d.version, Metadata: d.metadata}
}

func (d *discovery) info() micro.Info {
	return micro.Info{
		ServiceIdentity: d.identity(),
		Type:            micro.InfoResponseType,
		Description:     d.description,
		Endpoints: append(d.inportInfos(), micro.EndpointInfo{
			Name: ObserveEndpoint, Subject: ObserveSubject(d.s.base),
			Metadata: map[string]string{"format": "true|false"},
		}),
	}
}

func (d *discovery) inportInfos() []micro.EndpointInfo {
	var infos []micro.EndpointInfo
	for _, in := range d.s.inports {
		infos = append(infos, micro.EndpointInfo{
			Name: endpointName(in.name), Subject: InportSubjectFor(d.s.base, in.name), QueueGroup: d.s.base,
			Metadata: map[string]string{"format": "json", "run_id_header": RunIDHeader, "inport": in.name},
		})
	}
	return infos
}

// endpointName returns the $SRV endpoint name of a Network Inport.
func endpointName(inport string) string {
	if inport == fbp.In {
		return InportEndpoint
	}
	return inport
}

func (d *discovery) stats() micro.Stats {
	c := &d.s.stats
	c.mu.Lock()
	defer c.mu.Unlock()
	var endpoints []*micro.EndpointStats
	for _, in := range d.s.inports {
		var st InportStats
		if in.name == fbp.In {
			st.Outports = make(map[string]int, len(c.outports))
			for name, n := range c.outports {
				st.Outports[name] = n
			}
		}
		st.Dropped, _ = in.sub.Dropped()
		data, _ := json.Marshal(st)
		ic := c.inports[in.name]
		endpoints = append(endpoints, &micro.EndpointStats{
			Name: endpointName(in.name), Subject: InportSubjectFor(d.s.base, in.name), QueueGroup: d.s.base,
			NumRequests: ic.received, NumErrors: ic.errors, LastError: ic.lastError,
			Data: data,
		})
	}
	obs, _ := json.Marshal(ObserveStats{Observing: c.observing})
	return micro.Stats{
		ServiceIdentity: d.identity(),
		Type:            micro.StatsResponseType,
		Started:         d.started,
		Endpoints: append(endpoints, &micro.EndpointStats{
			Name: ObserveEndpoint, Subject: ObserveSubject(d.s.base),
			NumRequests: c.observes, NumErrors: c.obsErrors, LastError: c.obsLastErr,
			Data: obs,
		}),
	}
}
