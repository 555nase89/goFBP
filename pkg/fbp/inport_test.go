package fbp

import (
	"context"
	"strings"
	"testing"
)

// twoInports routes its Inport In to Upper and its Inport Loud to Exclaim.
func twoInports(t *testing.T) *Network {
	t.Helper()
	n := &Network{Name: "two", Inports: []string{"Loud"}}
	must(t, n.Add("upper", upper()))
	must(t, n.Add("exclaim", exclaim()))
	must(t, n.ConnectIn("upper"))
	must(t, n.ConnectInport("Loud", "exclaim"))
	must(t, n.ConnectOut("upper", Success, Success))
	must(t, n.ConnectOut("exclaim", Success, Success))
	return n
}

func TestSendToInport(t *testing.T) {
	log := newRunLog()
	fn, ch := collector()
	inst := start(t, twoInports(t), WithOutput(fn), WithRunFinished(log.runFinished))

	_, err := inst.Send(context.Background(), Packet{Data: str("a")})
	must(t, err)
	if o := next(t, ch); string(o.p.Data) != `"A"` {
		t.Fatalf("In: got %s", o.p.Data)
	}
	runID, err := inst.SendTo(context.Background(), "Loud", Packet{Data: str("a")})
	must(t, err)
	if o := next(t, ch); string(o.p.Data) != `"a!"` || o.p.RunID != runID {
		t.Fatalf("Loud: got %s %s", o.p.Data, o.p.RunID)
	}
	// Both Runs finish; their order is not fixed.
	if r1, r2 := log.next(t), log.next(t); (r1.runID != runID && r2.runID != runID) || r1.outputs != 1 || r2.outputs != 1 {
		t.Fatalf("got %+v %+v", r1, r2)
	}

	_, err = inst.SendAsTo(context.Background(), "Loud", "b")
	must(t, err)
	if o := next(t, ch); string(o.p.Data) != `"b!"` {
		t.Fatalf("SendAsTo: got %s", o.p.Data)
	}

	if _, err := inst.SendTo(context.Background(), "Nope", Packet{Data: str("a")}); err == nil {
		t.Fatal("unknown inport accepted")
	}
}

func TestEveryInportMustBeConnected(t *testing.T) {
	n := &Network{Name: "half", Inports: []string{"Loud"}}
	must(t, n.Add("upper", upper()))
	must(t, n.ConnectIn("upper"))
	if err := n.Validate(); err == nil || !strings.Contains(err.Error(), `"Loud"`) {
		t.Fatalf("got %v, want error for unconnected inport Loud", err)
	}

	unnamed := &Network{Name: "unnamed", Inports: []string{""}}
	must(t, unnamed.Add("upper", upper()))
	must(t, unnamed.ConnectIn("upper"))
	if err := unnamed.Validate(); err == nil {
		t.Fatal("inport without name accepted")
	}
}

func TestInportConnectErrors(t *testing.T) {
	n := &Network{Name: "n", Inports: []string{"Loud"}}
	must(t, n.Add("upper", upper()))
	must(t, n.Add("exclaim", exclaim()))
	must(t, n.Add("two", twoInports(t)))
	tests := map[string]error{
		"unknown net inport":      n.ConnectInport("Nope", "upper"),
		"brick has only In":       n.ConnectInportTo("Loud", "upper", "Loud"),
		"unknown nested inport":   n.ConnectTo("upper", Success, "two", "Nope"),
		"connect to brick inport": n.ConnectTo("upper", Success, "exclaim", "Loud"),
	}
	for name, err := range tests {
		if err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	must(t, n.ConnectInport("Loud", "upper"))
	if err := n.ConnectInport("Loud", "upper"); err == nil {
		t.Error("duplicate inport connection accepted")
	}
}

func TestNestedNetworkInport(t *testing.T) {
	outer := &Network{Name: "outer", Inports: []string{"Loud"}}
	must(t, outer.Add("two", twoInports(t)))
	must(t, outer.Add("upper", upper()))
	must(t, outer.ConnectIn("two"))
	// Loud -> upper -> two.Loud: "a" becomes "A!".
	must(t, outer.ConnectInport("Loud", "upper"))
	must(t, outer.ConnectTo("upper", Success, "two", "Loud"))
	must(t, outer.ConnectOut("two", Success, Success))

	obs, evs := observed()
	fn, ch := collector()
	inst := start(t, outer, WithOutput(fn))

	_, err := inst.Send(context.Background(), Packet{Data: str("a")})
	must(t, err)
	if o := next(t, ch); string(o.p.Data) != `"A"` {
		t.Fatalf("In: got %s", o.p.Data)
	}

	inst.SetObserver(obs)
	_, err = inst.SendTo(context.Background(), "Loud", Packet{Data: str("a"), RunID: "r"})
	must(t, err)
	if o := next(t, ch); string(o.p.Data) != `"A!"` {
		t.Fatalf("Loud: got %s", o.p.Data)
	}
	if got := events(t, evs, 8); !strings.HasPrefix(got[len(got)-1], `Sent r outer/.Loud `) {
		t.Fatalf("Sent event: got %v", got)
	}
}
