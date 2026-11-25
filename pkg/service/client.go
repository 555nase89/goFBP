package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/555nase89/goFBP/pkg/fbp"

	"github.com/nats-io/nats.go"
)

// Call sends data as one Packet to the Inport fbp.In of the Network with the
// base subject base and waits for the first Packet published for that Run on any
// Outport. It returns the name of that Outport and the Packet. Data that is
// not valid JSON is not sent; Call returns fbp.ErrInvalidJSON.
//
// Call creates the Run ID itself and subscribes to the Outport subjects of
// the Run before sending, so no result can be missed. Networks that publish
// several Packets per Run (Fan-out to Network Outports, extra Outports)
// deliver only the first one here; use the same steps with your own
// subscription to collect all of them.
//
// Call returns ctx.Err() when ctx is done before a result arrives, e.g.
// because no Service of the Network is running.
func Call(ctx context.Context, nc *nats.Conn, base string, data []byte) (string, fbp.Packet, error) {
	return CallTo(ctx, nc, base, fbp.In, data)
}

// CallTo is Call for the Network Inport inport.
func CallTo(ctx context.Context, nc *nats.Conn, base, inport string, data []byte) (string, fbp.Packet, error) {
	if !json.Valid(data) {
		return "", fbp.Packet{}, fbp.ErrInvalidJSON
	}
	runID := fbp.NewRunID()
	// Only the first result counts; later ones are dropped. SubscribeSync
	// would allocate a channel of Options.SubChanLen (64k) entries per Call.
	first := make(chan *nats.Msg, 1)
	sub, err := nc.Subscribe(OutportSubject(base, "*", runID), func(msg *nats.Msg) {
		select {
		case first <- msg:
		default:
		}
	})
	if err != nil {
		return "", fbp.Packet{}, err
	}
	defer sub.Unsubscribe()
	// The server must know the subscription before the Service can publish.
	if err := nc.Flush(); err != nil {
		return "", fbp.Packet{}, err
	}

	msg := nats.NewMsg(InportSubjectFor(base, inport))
	msg.Data = data
	msg.Header.Set(RunIDHeader, runID)
	if err := nc.PublishMsg(msg); err != nil {
		return "", fbp.Packet{}, err
	}

	var res *nats.Msg
	select {
	case <-ctx.Done():
		return "", fbp.Packet{}, ctx.Err()
	case res = <-first:
	}
	outport := strings.TrimSuffix(strings.TrimPrefix(res.Subject, base+".op."), "."+runID)
	return outport, fbp.Packet{Data: res.Data, RunID: runID}, nil
}

// CallAll sends data like Call and calls fn for every Packet the Run
// publishes on any Outport, in the order they arrive. It returns once the
// Service has announced the end of the Run on the Done subject and all
// Packets counted there have arrived.
//
// CallAll returns ctx.Err() when ctx is done before, e.g. because no Service
// of the Network is running or it does not publish on the Done subject.
func CallAll(ctx context.Context, nc *nats.Conn, base string, data []byte, fn func(outport string, p fbp.Packet)) error {
	return CallAllTo(ctx, nc, base, fbp.In, data, fn)
}

// CallAllTo is CallAll for the Network Inport inport.
func CallAllTo(ctx context.Context, nc *nats.Conn, base, inport string, data []byte, fn func(outport string, p fbp.Packet)) error {
	if !json.Valid(data) {
		return fbp.ErrInvalidJSON
	}
	runID := fbp.NewRunID()
	// Both subscriptions feed one channel; the handlers block while the
	// caller is busy, which NATS buffers up to the pending limits. Once
	// CallAllTo returns, nobody reads msgs any more: a message arriving late
	// is dropped instead of blocking the handler until ctx is done (forever
	// for a ctx without deadline).
	msgs := make(chan *nats.Msg)
	returned := make(chan struct{})
	defer close(returned)
	handler := func(msg *nats.Msg) {
		select {
		case msgs <- msg:
		case <-ctx.Done():
		case <-returned:
		}
	}
	outSub, err := nc.Subscribe(OutportSubject(base, "*", runID), handler)
	if err != nil {
		return err
	}
	defer outSub.Unsubscribe()
	doneSub, err := nc.Subscribe(DoneSubject(base, runID), handler)
	if err != nil {
		return err
	}
	defer doneSub.Unsubscribe()
	// The server must know the subscriptions before the Service can publish.
	if err := nc.Flush(); err != nil {
		return err
	}

	msg := nats.NewMsg(InportSubjectFor(base, inport))
	msg.Data = data
	msg.Header.Set(RunIDHeader, runID)
	if err := nc.PublishMsg(msg); err != nil {
		return err
	}

	// The Done message may overtake Packets of the other subscription, so
	// count until both have arrived.
	received, outputs := 0, -1
	for outputs < 0 || received < outputs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-msgs:
			if res.Sub == doneSub {
				var d DoneData
				if err := json.Unmarshal(res.Data, &d); err != nil {
					return fmt.Errorf("service: invalid done message: %w", err)
				}
				outputs = d.Outputs
				continue
			}
			outport := strings.TrimSuffix(strings.TrimPrefix(res.Subject, base+".op."), "."+runID)
			fn(outport, fbp.Packet{Data: res.Data, RunID: runID})
			received++
		}
	}
	return nil
}
