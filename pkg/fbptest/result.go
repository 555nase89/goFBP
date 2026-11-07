// Package fbptest helps to test Bricks and Networks of package fbp: call a
// Brick without a Network (CallBrick), send a Packet into an Instance and wait
// until its Run is finished (Start, Instance.Run), and compare the Packets that
// left through each Outport (Result.Want, Result.WantError).
// Terms follow GLOSSARY.md in the project root.
package fbptest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/555nase89/goFBP/pkg/fbp"
)

// Outputs lists the expected Packet data (JSON text) per Outport.
type Outputs map[string][]string

// Result holds the Packets of one Run (or one Brick call) by the Outport they
// were sent through, in the order they were sent.
type Result struct {
	RunID   string
	Outputs map[string][]json.RawMessage
}

// Want reports an error unless the Packets of r are exactly want: the same
// Outports with the same data. Data is compared as JSON, so white space and
// the order of object keys do not matter. The order of the Packets within an
// Outport does not matter either, because Fan-out and Concurrency may change
// it; check r.Outputs directly to test the order. An empty want means no
// Packet left at all.
func (r Result) Want(t testing.TB, want Outputs) {
	t.Helper()
	for _, port := range sortedKeys(r.Outputs, want) {
		got, err := canonicalAll(r.Outputs[port])
		if err != nil {
			t.Errorf("run %s, outport %s: %v", r.RunID, port, err)
			continue
		}
		exp, err := canonicalStrings(want[port])
		if err != nil {
			t.Errorf("run %s, outport %s: want: %v", r.RunID, port, err)
			continue
		}
		if !slices.Equal(sorted(got), sorted(exp)) {
			t.Errorf("run %s, outport %s: got %v, want %v", r.RunID, port, got, exp)
		}
	}
}

// WantError reports an error unless exactly one Packet left, through the
// Error Outport, carrying ErrorData whose Message contains message. node is
// the expected node path (e.g. "shout/upper"); an empty node is not checked.
func (r Result) WantError(t testing.TB, node, message string) {
	t.Helper()
	errs := r.Outputs[fbp.Error]
	if len(errs) != 1 || len(r.Outputs) != 1 {
		t.Errorf("run %s: want one Error Packet, got %s", r.RunID, r)
		return
	}
	var ed fbp.ErrorData
	if err := json.Unmarshal(errs[0], &ed); err != nil {
		t.Errorf("run %s: Error Packet is no ErrorData: %v", r.RunID, err)
		return
	}
	if (node != "" && ed.Node != node) || !strings.Contains(ed.Message, message) {
		t.Errorf("run %s: got error at node %q: %q, want node %q, message containing %q",
			r.RunID, ed.Node, ed.Message, node, message)
	}
}

// String lists the Packets of r by Outport, for failure messages.
func (r Result) String() string {
	if len(r.Outputs) == 0 {
		return "no Packets"
	}
	var b strings.Builder
	for i, port := range slices.Sorted(maps.Keys(r.Outputs)) {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s: %s", port, bytes.Join(asBytes(r.Outputs[port]), []byte(" ")))
	}
	return b.String()
}

// canonical returns data as compact JSON with sorted object keys. Numbers keep
// their literal text, so large integers are not rounded.
func canonical(data []byte) (string, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return "", fmt.Errorf("invalid JSON %q: %w", data, err)
	}
	if d.More() {
		return "", fmt.Errorf("invalid JSON %q: more than one value", data)
	}
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return "", err
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func canonicalAll(data []json.RawMessage) ([]string, error) {
	out := make([]string, len(data))
	for i, d := range data {
		c, err := canonical(d)
		if err != nil {
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

func canonicalStrings(data []string) ([]string, error) {
	return canonicalAll(asRaw(data))
}

func asRaw(data []string) []json.RawMessage {
	out := make([]json.RawMessage, len(data))
	for i, d := range data {
		out[i] = json.RawMessage(d)
	}
	return out
}

func asBytes(data []json.RawMessage) [][]byte {
	out := make([][]byte, len(data))
	for i, d := range data {
		out[i] = d
	}
	return out
}

func sorted(s []string) []string {
	return slices.Sorted(slices.Values(s))
}

// sortedKeys returns the Outports named in got or want.
func sortedKeys(got map[string][]json.RawMessage, want Outputs) []string {
	keys := slices.Collect(maps.Keys(got))
	for k := range want {
		if !slices.Contains(keys, k) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}
