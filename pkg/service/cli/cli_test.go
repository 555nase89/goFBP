package cli

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/555nase89/goFBP/examples/text"
	"github.com/555nase89/goFBP/pkg/fbp"
	"github.com/555nase89/goFBP/pkg/service"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const timeout = 2 * time.Second

func connect(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(timeout) {
		t.Fatal("nats server not ready")
	}
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestRun(t *testing.T) {
	nc := connect(t)
	net, err := text.Pipeline()
	if err != nil {
		t.Fatal(err)
	}
	s, err := service.Start(context.Background(), nc, text.PackagePath, net)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	base, err := ParseNetwork(text.PackagePath + "/" + net.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ in, outport, line string }{
		{`"  hello "`, fbp.Success, `Success "HELLO!"`},
		{`"   "`, text.Empty, `Empty ""`},
		{`42`, fbp.Error, `Error {"node":"trim","brick":"Trim","message":"input is not a JSON string","input":42}`},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		var out strings.Builder
		outports, err := Run(ctx, nc, base, fbp.In, []byte(c.in), &out, false)
		cancel()
		if err != nil {
			t.Fatalf("input %s: %v", c.in, err)
		}
		if !slices.Equal(outports, []string{c.outport}) || out.String() != c.line+"\n" {
			t.Errorf("input %s: got %q / %q, want %q / %q", c.in, outports, out.String(), c.outport, c.line)
		}
	}
}

// fanOut sends its input to Success and, as a second Packet, to Copy.
func fanOut(t *testing.T) *fbp.Network {
	t.Helper()
	b := &fbp.Brick{Name: "FanOut", Outports: []string{"Copy"}, Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		if err := out.Emit(ctx, fbp.Success, p.Data); err != nil {
			return err
		}
		return out.Emit(ctx, "Copy", p.Data)
	}}
	n := &fbp.Network{Name: "fanout", Outports: []string{"Copy"}}
	if err := errors.Join(
		n.Add("fanout", b),
		n.ConnectIn("fanout"),
		n.ConnectOut("fanout", fbp.Success, fbp.Success),
		n.ConnectOut("fanout", "Copy", "Copy"),
	); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRunAllPackets(t *testing.T) {
	nc := connect(t)
	s, err := service.Start(context.Background(), nc, "gofbp/test", fanOut(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	for _, c := range []struct {
		first bool
		want  int
	}{{false, 2}, {true, 1}} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		var out strings.Builder
		outports, err := Run(ctx, nc, s.Base(), fbp.In, []byte(`1`), &out, c.first)
		cancel()
		if err != nil {
			t.Fatalf("first=%v: %v", c.first, err)
		}
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		if len(outports) != c.want || len(lines) != c.want {
			t.Errorf("first=%v: got %q / %q, want %d Packets", c.first, outports, lines, c.want)
		}
		for _, l := range lines {
			if l != "Success 1" && l != "Copy 1" {
				t.Errorf("first=%v: unexpected line %q", c.first, l)
			}
		}
	}
}

func TestRunJSON(t *testing.T) {
	nc := connect(t)
	net, err := text.Pipeline()
	if err != nil {
		t.Fatal(err)
	}
	s, err := service.Start(context.Background(), nc, text.PackagePath, net)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	for name, c := range map[string]struct{ in, outport, out string }{
		"success": {`"  hello "`, fbp.Success, `{"Success":["HELLO!"]}`},
		"empty":   {`"   "`, text.Empty, `{"Empty":[""]}`},
		"error":   {`42`, fbp.Error, `{"Error":[{"node":"trim","brick":"Trim","message":"input is not a JSON string","input":42}]}`},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		var out strings.Builder
		outports, err := RunJSON(ctx, nc, s.Base(), fbp.In, []byte(c.in), &out, false)
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(outports, []string{c.outport}) || out.String() != c.out+"\n" {
			t.Errorf("%s: got %q / %q, want %q / %q", name, outports, out.String(), c.outport, c.out)
		}
	}
}

func TestRunJSONAllPackets(t *testing.T) {
	nc := connect(t)
	s, err := service.Start(context.Background(), nc, "gofbp/test", fanOut(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	for name, c := range map[string]struct {
		first bool
		want  int
	}{"all": {false, 2}, "first": {true, 1}} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		var out strings.Builder
		outports, err := RunJSON(ctx, nc, s.Base(), fbp.In, []byte(`{"n":1}`), &out, c.first)
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var results map[string][]json.RawMessage
		if err := json.Unmarshal([]byte(out.String()), &results); err != nil {
			t.Fatalf("%s: output %q is no JSON object: %v", name, out.String(), err)
		}
		n := 0
		for outport, list := range results {
			if outport != fbp.Success && outport != "Copy" {
				t.Errorf("%s: unexpected Outport %q", name, outport)
			}
			for _, d := range list {
				if string(d) != `{"n":1}` {
					t.Errorf("%s: unexpected data %s at %s", name, d, outport)
				}
			}
			n += len(list)
		}
		if len(outports) != c.want || n != c.want {
			t.Errorf("%s: got %q / %d Packets in %q, want %d", name, outports, n, out.String(), c.want)
		}
	}
}

// TestRunJSONSameOutport checks that several Packets on one Outport end up in
// one list, in the order they arrived.
func TestRunJSONSameOutport(t *testing.T) {
	nc := connect(t)
	b := &fbp.Brick{Name: "Split", Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
		for _, d := range []string{`1`, `2`, `3`} {
			if err := out.Emit(ctx, fbp.Success, json.RawMessage(d)); err != nil {
				return err
			}
		}
		return nil
	}}
	n := &fbp.Network{Name: "split"}
	if err := errors.Join(
		n.Add("split", b),
		n.ConnectIn("split"),
		n.ConnectOut("split", fbp.Success, fbp.Success),
	); err != nil {
		t.Fatal(err)
	}
	s, err := service.Start(context.Background(), nc, "gofbp/test", n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out strings.Builder
	if _, err := RunJSON(ctx, nc, s.Base(), fbp.In, []byte(`0`), &out, false); err != nil {
		t.Fatal(err)
	}
	if want := `{"Success":[1,2,3]}` + "\n"; out.String() != want {
		t.Errorf("got %q, want %q", out.String(), want)
	}
}

func TestRunJSONWithoutService(t *testing.T) {
	nc := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var out strings.Builder
	_, err := RunJSON(ctx, nc, "fbp.gofbp.test.none", fbp.In, []byte(`"x"`), &out, false)
	if err == nil || !strings.Contains(err.Error(), "no result within timeout") {
		t.Fatalf("got %v, want timeout error", err)
	}
	if out.String() != "{}\n" {
		t.Errorf("got output %q, want an empty object", out.String())
	}
}

func TestRunWithoutService(t *testing.T) {
	nc := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := Run(ctx, nc, "fbp.gofbp.test.none", fbp.In, []byte(`"x"`), &strings.Builder{}, false)
	if err == nil || !strings.Contains(err.Error(), "no result within timeout") {
		t.Fatalf("got %v, want timeout error", err)
	}
}

func TestParseNetwork(t *testing.T) {
	base, err := ParseNetwork("gofbp/examples/text/pipeline")
	if err != nil || base != "fbp.gofbp.examples.text.pipeline" {
		t.Errorf("got %q, %v", base, err)
	}
	for _, spec := range []string{"", "pipeline", "/pipeline", "gofbp/", "go_fbp/pipeline"} {
		if _, err := ParseNetwork(spec); err == nil {
			t.Errorf("%q: expected error", spec)
		}
	}
}

func TestReadInput(t *testing.T) {
	for _, c := range []struct {
		input, stdin string
		asString     bool
		want         string
		err          error
	}{
		{`{"a":1}`, "", false, `{"a":1}`, nil},
		{"", "[1,2]\n", false, `[1,2]`, nil},
		{"-", "hello\n", true, `"hello"`, nil},
		{"  hi ", "", true, `"  hi "`, nil},
		{"not json", "", false, "", fbp.ErrInvalidJSON},
	} {
		got, err := ReadInput(c.input, strings.NewReader(c.stdin), c.asString)
		if !errors.Is(err, c.err) || string(got) != c.want {
			t.Errorf("%q/%q: got %q, %v; want %q, %v", c.input, c.stdin, got, err, c.want, c.err)
		}
	}
}
