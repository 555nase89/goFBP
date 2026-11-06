package text

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/555nase89/goFBP/pkg/fbp"
)

func TestPipeline(t *testing.T) {
	n, err := Pipeline()
	if err != nil {
		t.Fatal(err)
	}
	type output struct {
		outport string
		data    string
	}
	ch := make(chan output, 10)
	inst, err := n.Start(context.Background(), fbp.WithOutput(func(_ context.Context, outport string, p fbp.Packet) {
		ch <- output{outport, string(p.Data)}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Stop()

	tests := []struct {
		in      string
		outport string
		data    string
	}{
		{`"  hello "`, fbp.Success, `"HELLO!"`},
		{`"   "`, Empty, `""`},
		{`42`, fbp.Error, ""},
	}
	for _, tt := range tests {
		if _, err := inst.Send(context.Background(), fbp.Packet{Data: json.RawMessage(tt.in)}); err != nil {
			t.Fatal(err)
		}
		select {
		case o := <-ch:
			if o.outport != tt.outport || (tt.data != "" && o.data != tt.data) {
				t.Errorf("input %s: got %s %s", tt.in, o.outport, o.data)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("input %s: timeout", tt.in)
		}
	}
}
