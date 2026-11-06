package crash

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"
)

func TestStats(t *testing.T) {
	n, err := Stats()
	if err != nil {
		t.Fatal(err)
	}
	type output struct {
		outport string
		packet  fbp.Packet
	}
	ch := make(chan output, 10)
	inst, err := n.Start(context.Background(), fbp.WithOutput(func(_ context.Context, outport string, p fbp.Packet) {
		ch <- output{outport, p}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Stop()

	tests := []struct {
		in      string
		outport string
		data    string // Success data, or the start of the ErrorData message
		node    string // node of the ErrorData
	}{
		{`[1,2,3]`, fbp.Success, `2`, ""},
		{`[4,-1]`, fbp.Error, "negative number -1", "check"},
		{`[]`, fbp.Error, "panic: runtime error: integer divide by zero", "average"},
	}
	for _, tt := range tests {
		if _, err := inst.Send(context.Background(), fbp.Packet{Data: json.RawMessage(tt.in)}); err != nil {
			t.Fatal(err)
		}
		var o output
		select {
		case o = <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("input %s: timeout", tt.in)
		}
		if o.outport != tt.outport {
			t.Errorf("input %s: got outport %s %s", tt.in, o.outport, o.packet.Data)
			continue
		}
		if o.outport == fbp.Success {
			if string(o.packet.Data) != tt.data {
				t.Errorf("input %s: got %s", tt.in, o.packet.Data)
			}
			continue
		}
		ed, err := fbp.DataAs[fbp.ErrorData](o.packet)
		if err != nil {
			t.Fatal(err)
		}
		if ed.Node != tt.node || !strings.HasPrefix(ed.Message, tt.data) {
			t.Errorf("input %s: got %+v", tt.in, ed)
		}
	}
}
