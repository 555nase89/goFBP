package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

func TestServiceName(t *testing.T) {
	for base, want := range map[string]string{
		"fbp.gofbp.examples.text.pipeline": "fbp_gofbp_examples_text_pipeline_bf241440",
		"fbp.a-b.c~d":                      "fbp_a-b_c_d_0685cba7",
		// Equal without the hash.
		"fbp.a_b.c": "fbp_a_b_c_8bd831ea",
		"fbp.a.b_c": "fbp_a_b_c_00bb189e",
	} {
		if got := ServiceName(base); got != want {
			t.Errorf("ServiceName(%q) = %q, want %q", base, got, want)
		}
	}
}

// request sends a request to subject and decodes the reply into v.
func request(t *testing.T, nc *nats.Conn, subject string, v any) {
	t.Helper()
	msg, err := nc.Request(subject, nil, timeout)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(msg.Data, v); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryInfo(t *testing.T) {
	nc := connect(t)
	net := upperNetwork(t)
	net.Outports = []string{"Extra"}
	s, err := Start(context.Background(), nc, "gofbp/test", net, WithVersion("1.2.3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	for _, subject := range []string{"$SRV.INFO", "$SRV.INFO.fbp_gofbp_test_upper_366cd53d", "$SRV.INFO.fbp_gofbp_test_upper_366cd53d." + s.ID()} {
		var info micro.Info
		request(t, nc, subject, &info)
		if info.Type != micro.InfoResponseType || info.Name != "fbp_gofbp_test_upper_366cd53d" || info.ID != s.ID() || info.Version != "1.2.3" {
			t.Fatalf("%s: got %+v", subject, info)
		}
		md := info.Metadata
		if md["package"] != "gofbp/test" || md["network"] != "upper" || md["base"] != s.Base() ||
			md["outports"] != "Success,Error,Extra" || md["trace"] != TraceSubject(s.Base()) {
			t.Fatalf("%s: metadata %v", subject, md)
		}
		if len(info.Endpoints) != 2 ||
			info.Endpoints[0].Name != InportEndpoint || info.Endpoints[0].Subject != InportSubject(s.Base()) || info.Endpoints[0].QueueGroup != s.Base() ||
			info.Endpoints[1].Name != ObserveEndpoint || info.Endpoints[1].Subject != ObserveSubject(s.Base()) || info.Endpoints[1].QueueGroup != "" {
			t.Fatalf("%s: endpoints %+v", subject, info.Endpoints)
		}
	}
}

func TestDiscoveryPingAllInstances(t *testing.T) {
	nc := connect(t)
	net := upperNetwork(t)
	ids := map[string]bool{}
	for range 2 {
		s, err := Start(context.Background(), nc, "gofbp/test", net)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Stop() })
		ids[s.ID()] = true
	}

	inbox := nc.NewRespInbox()
	replies, _ := nc.SubscribeSync(inbox)
	if err := nc.PublishRequest("$SRV.PING.fbp_gofbp_test_upper_366cd53d", inbox, nil); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		var ping micro.Ping
		if err := json.Unmarshal(nextMsg(t, replies).Data, &ping); err != nil {
			t.Fatal(err)
		}
		if ping.Type != micro.PingResponseType || ping.Version != DefaultVersion || !ids[ping.ID] {
			t.Fatalf("got %+v", ping)
		}
		delete(ids, ping.ID)
	}

	// A stopped Service no longer answers.
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	s.Stop()
	if _, err := nc.Request("$SRV.PING.fbp_gofbp_test_upper_366cd53d."+s.ID(), nil, 100*time.Millisecond); err == nil {
		t.Fatal("stopped service answered")
	}
}

func TestDiscoveryStats(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	success, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Success, "*"))
	failure, _ := nc.SubscribeSync(OutportSubject(s.Base(), fbp.Error, "*"))
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{`"a"`, `"b"`, `42`, `not json`} {
		if err := nc.Publish(InportSubject(s.Base()), []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	nextMsg(t, success)
	nextMsg(t, success)
	nextMsg(t, failure)
	nextMsg(t, failure)
	if err := nc.Publish(ObserveSubject(s.Base()), []byte(`maybe`)); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	var stats micro.Stats
	request(t, nc, "$SRV.STATS.fbp_gofbp_test_upper_366cd53d."+s.ID(), &stats)
	if stats.Type != micro.StatsResponseType || stats.Started.IsZero() || len(stats.Endpoints) != 2 {
		t.Fatalf("got %+v", stats)
	}
	in, obs := stats.Endpoints[0], stats.Endpoints[1]
	// Only invalid JSON is an Inport error; the Brick error of 42 is a
	// Packet on the Error Outport.
	if in.Name != InportEndpoint || in.NumRequests != 4 || in.NumErrors != 1 || in.LastError == "" {
		t.Fatalf("inport %+v", in)
	}
	var data InportStats
	if err := json.Unmarshal(in.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Dropped != 0 || data.Outports[fbp.Success] != 2 || data.Outports[fbp.Error] != 2 {
		t.Fatalf("inport data %+v", data)
	}
	if obs.Name != ObserveEndpoint || obs.NumRequests != 1 || obs.NumErrors != 1 {
		t.Fatalf("observe %+v", obs)
	}
}

func TestDiscoveryStatsObserving(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	observing := func() bool {
		t.Helper()
		var stats micro.Stats
		request(t, nc, "$SRV.STATS.fbp_gofbp_test_upper_366cd53d."+s.ID(), &stats)
		var data ObserveStats
		if err := json.Unmarshal(stats.Endpoints[len(stats.Endpoints)-1].Data, &data); err != nil {
			t.Fatal(err)
		}
		return data.Observing
	}
	switchTo := func(data string) {
		t.Helper()
		if err := nc.Publish(ObserveSubject(s.Base()), []byte(data)); err != nil {
			t.Fatal(err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if observing() {
		t.Fatal("observing after Start")
	}
	switchTo(`true`)
	if !observing() {
		t.Fatal("not observing after true")
	}
	switchTo(`maybe`)
	if !observing() {
		t.Fatal("invalid message switched observing off")
	}
	s.SetObserver(func(fbp.Event) {})
	if observing() {
		t.Fatal("observing after SetObserver")
	}
	switchTo(`true`)
	switchTo(`false`)
	if observing() {
		t.Fatal("observing after false")
	}
}
