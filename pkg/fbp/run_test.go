package fbp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// runLog records Outputs and finished Runs in the order they happen.
type runLog struct {
	mu       sync.Mutex
	outputs  map[string]int // Outputs seen per Run
	finished chan finishedRun
}

type finishedRun struct {
	runID           string
	outputs, actual int // reported by RunFinishedFunc, counted by OutputFunc
}

func newRunLog() *runLog {
	return &runLog{outputs: map[string]int{}, finished: make(chan finishedRun, 1000)}
}

func (l *runLog) output(_ context.Context, _ string, p Packet) {
	l.mu.Lock()
	l.outputs[p.RunID]++
	l.mu.Unlock()
}

func (l *runLog) runFinished(runID string, outputs int) {
	l.mu.Lock()
	actual := l.outputs[runID]
	l.mu.Unlock()
	l.finished <- finishedRun{runID, outputs, actual}
}

func (l *runLog) next(t *testing.T) finishedRun {
	t.Helper()
	select {
	case f := <-l.finished:
		return f
	case <-time.After(timeout):
		t.Fatal("timeout waiting for finished Run")
		return finishedRun{}
	}
}

// split emits every element of a JSON array of strings as its own Packet.
func split() *Brick {
	return &Brick{Name: "Split", Process: func(ctx context.Context, p Packet, out Emitter) error {
		items, err := DataAs[[]string](p)
		if err != nil {
			return err
		}
		for _, s := range items {
			if err := EmitAs(ctx, out, Success, s); err != nil {
				return err
			}
		}
		return nil
	}}
}

// runNetwork: Inport -> split -> shout (nested, Concurrency 3) -> Success,
// and Inport -> count -> Count. Errors of split and shout go to Error.
func runNetwork(t *testing.T) *Network {
	inner := shout(t)
	for _, name := range []string{"upper", "exclaim"} {
		inner.nodes[name].(*Brick).Concurrency = 3
	}
	count := &Brick{Name: "Count", Process: func(ctx context.Context, p Packet, out Emitter) error {
		var items []json.RawMessage
		if err := json.Unmarshal(p.Data, &items); err != nil {
			return err
		}
		return EmitAs(ctx, out, Success, len(items))
	}}
	n := &Network{Name: "runs", Outports: []string{"Count"}}
	must(t, n.Add("split", split()))
	must(t, n.Add("shout", inner))
	must(t, n.Add("count", count))
	must(t, n.ConnectIn("split"))
	must(t, n.ConnectIn("count"))
	must(t, n.Connect("split", Success, "shout"))
	must(t, n.ConnectOut("shout", Success, Success))
	must(t, n.ConnectOut("count", Success, "Count"))
	must(t, n.ConnectOut("split", Error, Error))
	must(t, n.ConnectOut("shout", Error, Error))
	return n
}

func TestRunFinishedAfterAllOutputs(t *testing.T) {
	l := newRunLog()
	inst := start(t, runNetwork(t), WithOutput(l.output), WithRunFinished(l.runFinished))

	// Many Runs at once, with 0..9 Packets each through the nested Network.
	want := map[string]int{}
	for i := range 50 {
		items := make([]string, i%10)
		for j := range items {
			items[j] = fmt.Sprint("x", j)
		}
		runID, err := inst.SendAs(context.Background(), items)
		must(t, err)
		want[runID] = len(items) + 1 // plus the Count Packet
	}
	for range len(want) {
		f := l.next(t)
		if f.outputs != want[f.runID] || f.actual != want[f.runID] {
			t.Errorf("run %s: reported %d, seen %d outputs before finish, want %d", f.runID, f.outputs, f.actual, want[f.runID])
		}
		delete(want, f.runID)
	}
	if len(want) != 0 {
		t.Fatalf("not finished: %v", want)
	}
	select {
	case f := <-l.finished:
		t.Fatalf("finished twice: %+v", f)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRunFinishedOnError(t *testing.T) {
	l := newRunLog()
	inst := start(t, runNetwork(t), WithOutput(l.output), WithRunFinished(l.runFinished))

	// split fails on a non-array; count fails too, but its Error Outport is
	// not connected, so only one Error Packet leaves the Network.
	runID, err := inst.Send(context.Background(), Packet{Data: json.RawMessage(`42`)})
	must(t, err)
	if f := l.next(t); f.runID != runID || f.outputs != 1 || f.actual != 1 {
		t.Fatalf("got %+v, want run %s with 1 output", f, runID)
	}
}

func TestRunFinishedWithoutOutput(t *testing.T) {
	n := &Network{Name: "silent"}
	must(t, n.Add("upper", upper()))
	must(t, n.ConnectIn("upper"))
	l := newRunLog()
	inst := start(t, n, WithRunFinished(l.runFinished))

	runID, err := inst.Send(context.Background(), Packet{Data: str("a")})
	must(t, err)
	if f := l.next(t); f.runID != runID || f.outputs != 0 {
		t.Fatalf("got %+v, want run %s with 0 outputs", f, runID)
	}
}
