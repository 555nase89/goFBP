package fbp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

// observed returns an ObserverFunc that forwards every Event to a channel.
func observed() (ObserverFunc, chan Event) {
	ch := make(chan Event, 100)
	return func(e Event) { ch <- e }, ch
}

// events waits for n Events and returns them in a stable, readable form,
// sorted, because Events of different Bricks may arrive in any order.
func events(t *testing.T, ch <-chan Event, n int) []string {
	t.Helper()
	var got []string
	for range n {
		select {
		case e := <-ch:
			got = append(got, format(e))
		case <-time.After(timeout):
			t.Fatalf("timeout after %d of %d events: %q", len(got), n, got)
		}
	}
	select {
	case e := <-ch:
		t.Fatalf("unexpected extra event %q", format(e))
	case <-time.After(50 * time.Millisecond):
	}
	slices.Sort(got)
	return got
}

func format(e Event) string {
	s := fmt.Sprintf("%s %s %s/%s.%s #%d", e.Kind, e.RunID, e.Network, e.Node, e.Port, e.PacketID)
	if e.ParentID != 0 {
		s += fmt.Sprintf("<#%d", e.ParentID)
	}
	if e.Data != nil {
		s += " " + string(e.Data)
	}
	if e.Err != nil {
		s += " err=" + e.Err.Error()
	}
	return s
}

func TestObserverFollowsPacketsThroughNestedNetwork(t *testing.T) {
	outer := &Network{Name: "outer"}
	must(t, outer.Add("loud", shout(t)))
	must(t, outer.ConnectIn("loud"))
	must(t, outer.ConnectOut("loud", Success, Success))

	fn, ch := observed()
	inst := start(t, outer, WithObserver(fn))
	_, err := inst.Send(context.Background(), Packet{Data: str("hi"), RunID: "r"})
	must(t, err)

	want := []string{
		`Emitted r outer/loud/exclaim.Success #3<#2 "HI!"`,
		`Emitted r outer/loud/upper.Success #2<#1 "HI"`,
		`Output r outer/.Success #3 "HI!"`,
		`Processed r outer/loud/exclaim. #2`,
		`Processed r outer/loud/upper. #1`,
		`Received r outer/loud/exclaim.Inport #2 "HI"`,
		`Received r outer/loud/upper.Inport #1 "hi"`,
		`Sent r outer/.In #1 "hi"`,
	}
	if got := events(t, ch, len(want)); !slices.Equal(got, want) {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestObserverFanOutKeepsPacketID(t *testing.T) {
	pass := &Brick{Name: "Pass", Process: func(ctx context.Context, p Packet, out Emitter) error {
		return out.Emit(ctx, Success, p.Data)
	}}
	n := &Network{Name: "fanout"}
	must(t, n.Add("source", pass))
	must(t, n.Add("left", pass))
	must(t, n.Add("right", pass))
	must(t, n.ConnectIn("source"))
	must(t, n.Connect("source", Success, "left"))
	must(t, n.Connect("source", Success, "right"))

	fn, ch := observed()
	inst := start(t, n, WithObserver(fn))
	_, err := inst.Send(context.Background(), Packet{Data: str("x"), RunID: "r"})
	must(t, err)

	// The unconnected Outports of left and right get no Output Event, so
	// their Emitted Events are the last trace of those Packets.
	got := events(t, ch, 10)
	for _, w := range []string{
		`Emitted r fanout/source.Success #2<#1 "x"`,
		`Received r fanout/left.Inport #2 "x"`,
		`Received r fanout/right.Inport #2 "x"`,
	} {
		if !slices.Contains(got, w) {
			t.Fatalf("missing %q in\n%q", w, got)
		}
	}
}

func TestObserverReportsErrors(t *testing.T) {
	fail := stringBrick("Fail", func(string) (string, error) { return "", errors.New("boom") })
	n := &Network{Name: "errors"}
	must(t, n.Add("fail", fail))
	must(t, n.ConnectIn("fail"))
	must(t, n.ConnectOut("fail", Error, Error))

	fn, ch := observed()
	inst := start(t, n, WithObserver(fn))
	_, err := inst.Send(context.Background(), Packet{Data: str("x"), RunID: "r"})
	must(t, err)

	errData := `{"node":"fail","brick":"Fail","message":"boom","input":"x"}`
	want := []string{
		`Emitted r errors/fail.Error #2<#1 ` + errData,
		`Output r errors/.Error #2 ` + errData,
		`Processed r errors/fail. #1 err=boom`,
		`Received r errors/fail.Inport #1 "x"`,
		`Sent r errors/.In #1 "x"`,
	}
	if got := events(t, ch, len(want)); !slices.Equal(got, want) {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestObserverReportsCopy(t *testing.T) {
	const copies = 3
	release := make(chan struct{})
	block := &Brick{Name: "Block", Concurrency: copies, Process: func(context.Context, Packet, Emitter) error {
		<-release
		return nil
	}}
	n := &Network{Name: "copies"}
	must(t, n.Add("block", block))
	must(t, n.ConnectIn("block"))

	fn, ch := observed()
	inst := start(t, n, WithObserver(fn))
	defer close(release)
	for range copies {
		_, err := inst.Send(context.Background(), Packet{Data: null})
		must(t, err)
	}

	// All copies hold one Packet each, so every copy number shows up once.
	seen := map[int]bool{}
	for len(seen) < copies {
		select {
		case e := <-ch:
			if e.Kind == Received {
				if seen[e.Copy] || e.Copy < 0 || e.Copy >= copies {
					t.Fatalf("unexpected copy %d, seen %v", e.Copy, seen)
				}
				seen[e.Copy] = true
			}
		case <-time.After(timeout):
			t.Fatalf("timeout, seen copies %v", seen)
		}
	}
}

func TestSetObserverWhileRunning(t *testing.T) {
	n := &Network{Name: "switch"}
	must(t, n.Add("upper", upper()))
	must(t, n.ConnectIn("upper"))
	must(t, n.ConnectOut("upper", Success, Success))

	out, results := collector()
	inst := start(t, n, WithOutput(out))
	send := func(data string) {
		t.Helper()
		_, err := inst.Send(context.Background(), Packet{Data: str(data), RunID: "r"})
		must(t, err)
		next(t, results)
	}
	fn, ch := observed()

	send("off")
	inst.SetObserver(fn)
	send("on")
	// Packet IDs are only counted while observed, so they start at 1.
	want := []string{
		`Emitted r switch/upper.Success #2<#1 "ON"`,
		`Output r switch/.Success #2 "ON"`,
		`Processed r switch/upper. #1`,
		`Received r switch/upper.Inport #1 "on"`,
		`Sent r switch/.In #1 "on"`,
	}
	if got := events(t, ch, len(want)); !slices.Equal(got, want) {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}

	inst.SetObserver(nil)
	send("off again")
	events(t, ch, 0)
}
