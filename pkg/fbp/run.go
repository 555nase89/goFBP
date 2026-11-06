package fbp

import "sync"

// runs tracks the Runs of an Instance to tell when one is finished. Each
// Packet of a Run counts while it is buffered in an Inport or being processed
// by a Brick; Send counts while it delivers. A Brick emits before its own
// Packet stops counting, so the count of a Run going on never drops to 0
// early. A nil *runs tracks nothing.
type runs struct {
	mu       sync.Mutex
	inFlight map[string]*runState
	finished RunFinishedFunc
}

type runState struct {
	packets int // Packets of the Run counting right now
	outputs int // Packets of the Run that left the Network's Outports
}

func (r *runs) add(runID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.inFlight[runID]
	if st == nil {
		st = &runState{}
		r.inFlight[runID] = st
	}
	st.packets++
}

// output counts a Packet leaving the Network. It is only called while a
// Packet of the Run counts, so the Run is known.
func (r *runs) output(runID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if st := r.inFlight[runID]; st != nil {
		st.outputs++
	}
}

// done stops counting one Packet and calls finished when it was the last one.
func (r *runs) done(runID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	st := r.inFlight[runID]
	if st == nil {
		r.mu.Unlock()
		return
	}
	st.packets--
	if st.packets > 0 {
		r.mu.Unlock()
		return
	}
	delete(r.inFlight, runID)
	r.mu.Unlock()
	r.finished(runID, st.outputs)
}
