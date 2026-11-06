// Package cli is the logic of the command fbpcli: it sends one input to the
// Inport of a Network that runs as a Service and prints every Packet of the
// Run together with the name of its Outport.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/555nase89/goFBP/pkg/fbp"
	"github.com/555nase89/goFBP/pkg/service"

	"github.com/nats-io/nats.go"
)

// ParseNetwork turns spec, "<packagepath>/<networkname>" such as
// "gofbp/examples/text/pipeline", into the base subject of the Network.
func ParseNetwork(spec string) (string, error) {
	i := strings.LastIndex(spec, "/")
	if i <= 0 || i == len(spec)-1 {
		return "", fmt.Errorf("cli: network %q must be <packagepath>/<networkname>", spec)
	}
	return service.Subject(spec[:i], spec[i+1:])
}

// ReadInput returns the Packet data for input. An empty input or "-" reads
// all of stdin, without the trailing newline. With asString the text is sent
// as a JSON string; otherwise it must be valid JSON (fbp.ErrInvalidJSON).
func ReadInput(input string, stdin io.Reader, asString bool) ([]byte, error) {
	data := []byte(input)
	if input == "" || input == "-" {
		var err error
		if data, err = io.ReadAll(stdin); err != nil {
			return nil, err
		}
		data = bytes.TrimRight(data, "\r\n")
	}
	if asString {
		return json.Marshal(string(data))
	}
	if !json.Valid(data) {
		return nil, fbp.ErrInvalidJSON
	}
	return data, nil
}

// Run sends data to the Inport inport of the Network with the base subject
// base and writes every
// Packet of the Run as one line "<Outport> <data>" to w, until the Service
// announces the end of the Run. With first it stops after the first Packet.
// It returns the Outports of the Packets in the order they arrived.
func Run(ctx context.Context, nc *nats.Conn, base, inport string, data []byte, w io.Writer, first bool) ([]string, error) {
	var outports []string
	var werr error
	write := func(outport string, p fbp.Packet) {
		outports = append(outports, outport)
		if werr == nil {
			_, werr = fmt.Fprintf(w, "%s %s\n", outport, p.Data)
		}
	}
	var err error
	if first {
		var outport string
		var p fbp.Packet
		if outport, p, err = service.CallTo(ctx, nc, base, inport, data); err == nil {
			write(outport, p)
		}
	} else {
		err = service.CallAllTo(ctx, nc, base, inport, data, write)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if len(outports) > 0 {
			return outports, fmt.Errorf("cli: end of the Run not announced within timeout (does the Service publish on %s?)", service.DoneSubject(base, "<runid>"))
		}
		return nil, fmt.Errorf("cli: no result within timeout (is a Service of %s running?)", base)
	}
	if err != nil {
		return outports, err
	}
	return outports, werr
}
