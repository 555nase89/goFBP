package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go/micro"
)

// twoInportNetwork is upperNetwork with an extra Inport Echo that sends its
// Packets straight to Success.
func twoInportNetwork(t *testing.T) *fbp.Network {
	t.Helper()
	n := upperNetwork(t)
	n.Inports = []string{"Echo"}
	echo := &fbp.Brick{Name: "Echo", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		return out.Emit(ctx, fbp.Success, p.Data)
	}}
	for _, err := range []error{
		n.Add("echo", echo),
		n.ConnectInport("Echo", "echo"),
		n.ConnectOut("echo", fbp.Success, fbp.Success),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	return n
}

func TestInportSubjectFor(t *testing.T) {
	if got := InportSubjectFor("fbp.a.n", fbp.In); got != "fbp.a.n.ip.in" {
		t.Fatalf("In: got %s", got)
	}
	if got := InportSubjectFor("fbp.a.n", "Echo"); got != "fbp.a.n.ip.Echo" {
		t.Fatalf("Echo: got %s", got)
	}
}

func TestCallToInport(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", twoInportNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for inport, want := range map[string]string{fbp.In: `"A"`, "Echo": `"a"`} {
		outport, p, err := CallTo(ctx, nc, s.Base(), inport, []byte(`"a"`))
		if err != nil {
			t.Fatal(err)
		}
		if outport != fbp.Success || string(p.Data) != want {
			t.Fatalf("%s: got %s %s, want %s", inport, outport, p.Data, want)
		}
	}
	var got []string
	err = CallAllTo(ctx, nc, s.Base(), "Echo", []byte(`1`), func(_ string, p fbp.Packet) { got = append(got, string(p.Data)) })
	if err != nil || len(got) != 1 || got[0] != `1` {
		t.Fatalf("CallAllTo: got %v %v", got, err)
	}
}

func TestDiscoveryReportsEveryInport(t *testing.T) {
	nc := connect(t)
	s, err := Start(context.Background(), nc, "gofbp/test", twoInportNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, _, err := CallTo(ctx, nc, s.Base(), "Echo", []byte(`1`)); err != nil {
		t.Fatal(err)
	}

	var info micro.Info
	request(t, nc, "$SRV.INFO.fbp_gofbp_test_upper_366cd53d."+s.ID(), &info)
	if info.Metadata["inports"] != "In,Echo" || len(info.Endpoints) != 3 ||
		info.Endpoints[0].Name != InportEndpoint || info.Endpoints[0].Subject != InportSubject(s.Base()) ||
		info.Endpoints[1].Name != "Echo" || info.Endpoints[1].Subject != InportSubjectFor(s.Base(), "Echo") || info.Endpoints[1].QueueGroup != s.Base() ||
		info.Endpoints[2].Name != ObserveEndpoint {
		t.Fatalf("got %v %+v", info.Metadata, info.Endpoints)
	}

	var stats micro.Stats
	request(t, nc, "$SRV.STATS.fbp_gofbp_test_upper_366cd53d."+s.ID(), &stats)
	if len(stats.Endpoints) != 3 || stats.Endpoints[0].NumRequests != 0 || stats.Endpoints[1].NumRequests != 1 {
		t.Fatalf("got %+v", stats.Endpoints)
	}
	var in, echo InportStats
	if err := json.Unmarshal(stats.Endpoints[0].Data, &in); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(stats.Endpoints[1].Data, &echo); err != nil {
		t.Fatal(err)
	}
	if in.Outports[fbp.Success] != 1 || echo.Outports != nil {
		t.Fatalf("got in %+v, echo %+v", in, echo)
	}
}

func TestStartRejectsInvalidInport(t *testing.T) {
	nc := connect(t)
	for _, name := range []string{"a.b", "in", "observe"} {
		net := upperNetwork(t)
		net.Inports = []string{name}
		if err := net.ConnectInport(name, "upper"); err != nil {
			t.Fatal(err)
		}
		if _, err := Start(context.Background(), nc, "gofbp/test", net); err == nil {
			t.Fatalf("inport %q accepted", name)
		}
	}
}
