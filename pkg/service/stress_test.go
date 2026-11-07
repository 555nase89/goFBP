package service

import (
	"context"
	"flag"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go"
)

var (
	stressCalls   = flag.Int("stress.calls", 4000, "number of Calls in TestCallStress")
	stressWorkers = flag.Int("stress.workers", 16, "number of goroutines calling at once in TestCallStress")
)

// runSet collects Run IDs seen at one station of a Call.
type runSet struct {
	mu  sync.Mutex
	ids map[string]bool
}

func newRunSet() *runSet { return &runSet{ids: map[string]bool{}} }

func (r *runSet) add(id string) {
	r.mu.Lock()
	r.ids[id] = true
	r.mu.Unlock()
}

func (r *runSet) has(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ids[id]
}

func (r *runSet) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

// TestCallStress runs many Calls from many goroutines over one client
// connection and checks that every Call gets its result. When one does not,
// it reports where its Run got lost: before the Network (never Sent), in the
// Network (Sent, no Output), on the way back (Output, not seen on NATS) or in
// Call itself (seen on NATS, not returned). Larger runs:
//
//	go test ./pkg/service -run TestCallStress -stress.calls 100000 -stress.workers 64
func TestCallStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}
	nc := connect(t)
	client, err := nats.Connect(nc.ConnectedUrl())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	monitor, err := nats.Connect(nc.ConnectedUrl())
	if err != nil {
		t.Fatal(err)
	}
	defer monitor.Close()

	s, err := Start(context.Background(), nc, "gofbp/stress", upperNetwork(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	sent, output, onNATS, returned := newRunSet(), newRunSet(), newRunSet(), newRunSet()
	s.SetObserver(func(e fbp.Event) {
		switch e.Kind {
		case fbp.Sent:
			sent.add(e.RunID)
		case fbp.Output:
			output.add(e.RunID)
		}
	})
	sub, err := monitor.Subscribe(OutportSubject(s.Base(), "*", "*"), func(msg *nats.Msg) {
		onNATS.add(msg.Header.Get(RunIDHeader))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.SetPendingLimits(-1, -1); err != nil {
		t.Fatal(err)
	}
	if err := monitor.Flush(); err != nil {
		t.Fatal(err)
	}

	var (
		mu     sync.Mutex
		failed []string
		wg     sync.WaitGroup
	)
	calls := make(chan struct{}, *stressCalls)
	for range *stressCalls {
		calls <- struct{}{}
	}
	close(calls)
	start := time.Now()
	for range *stressWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range calls {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				runID, p, err := Call(ctx, client, s.Base(), []byte(`"hello"`))
				cancel()
				if err == nil && string(p.Data) == `"HELLO"` {
					returned.add(runID)
					continue
				}
				mu.Lock()
				if err != nil {
					failed = append(failed, err.Error())
				} else {
					failed = append(failed, "run "+runID+": got "+string(p.Data))
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("%d Calls, %d goroutines: %v, %v per Call", *stressCalls, *stressWorkers, elapsed, elapsed/time.Duration(*stressCalls))
	if len(failed) == 0 {
		return
	}

	// Call does not return the Run ID of a failed Call, so the stations are
	// compared as sets: every Run that was Sent but not returned is a lost one.
	time.Sleep(100 * time.Millisecond) // let the monitor catch up
	var lost []string
	sent.mu.Lock()
	for id := range sent.ids {
		if returned.has(id) {
			continue
		}
		switch {
		case !output.has(id):
			lost = append(lost, id+": Sent, no Output (lost in the Network)")
		case !onNATS.has(id):
			lost = append(lost, id+": Output, not on NATS (lost on the way back)")
		default:
			lost = append(lost, id+": on NATS, not returned by Call (lost in Call)")
		}
	}
	sent.mu.Unlock()
	s.stats.mu.Lock()
	received := s.stats.inports[fbp.In].received
	s.stats.mu.Unlock()
	dropped, _ := s.inports[0].sub.Dropped()
	t.Errorf("%d of %d Calls failed: %s", len(failed), *stressCalls, strings.Join(failed[:min(len(failed), 5)], "; "))
	t.Errorf("Service received %d messages, dropped %d; %d Runs Sent, %d Output, %d on NATS, %d returned",
		received, dropped, sent.len(), output.len(), onNATS.len(), returned.len())
	if never := len(failed) - len(lost); never > 0 {
		t.Errorf("%d Runs never reached the Network (lost before Send)", never)
	}
	for _, l := range lost[:min(len(lost), 20)] {
		t.Error(l)
	}
}
