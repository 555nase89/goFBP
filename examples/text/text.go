// Package text is an example: Bricks working on JSON strings and a Network
// that nests another Network.
package text

import (
	"context"
	"errors"
	"strings"

	"github.com/555nase89/goFBP/pkg/fbp"
)

// PackagePath is the package path used for the Service subjects.
const PackagePath = "gofbp/examples/text"

// Empty is the Outport of Trim for strings that are empty after trimming.
const Empty = "Empty"

// stringBrick builds a Brick that transforms a JSON string with fn.
func stringBrick(name, description string, fn func(string) (string, error)) *fbp.Brick {
	return &fbp.Brick{
		Name:        name,
		Description: description,
		Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
			s, err := fbp.DataAs[string](p)
			if err != nil {
				return errors.New("input is not a JSON string")
			}
			r, err := fn(s)
			if err != nil {
				return err
			}
			return fbp.EmitAs(ctx, out, fbp.Success, r)
		},
	}
}

// Upper converts the string to upper case.
func Upper() *fbp.Brick {
	return stringBrick("Upper", "Converts a string to upper case",
		func(s string) (string, error) { return strings.ToUpper(s), nil })
}

// Suffix appends suffix to the string.
func Suffix(suffix string) *fbp.Brick {
	return stringBrick("Suffix", "Appends "+suffix+" to a string",
		func(s string) (string, error) { return s + suffix, nil })
}

// Trim removes surrounding white space and sends empty results to Empty.
func Trim() *fbp.Brick {
	return &fbp.Brick{
		Name:        "Trim",
		Description: "Removes surrounding white space; empty strings go to Empty",
		Outports:    []string{Empty},
		Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
			s, err := fbp.DataAs[string](p)
			if err != nil {
				return errors.New("input is not a JSON string")
			}
			s = strings.TrimSpace(s)
			if s == "" {
				return fbp.EmitAs(ctx, out, Empty, s)
			}
			return fbp.EmitAs(ctx, out, fbp.Success, s)
		},
	}
}

// Shout is a Network: Upper -> Suffix("!").
func Shout() (*fbp.Network, error) {
	n := &fbp.Network{Name: "shout", Description: "Upper case plus exclamation mark"}
	return n, errors.Join(
		n.Add("upper", Upper()),
		n.Add("suffix", Suffix("!")),
		n.ConnectIn("upper"),
		n.Connect("upper", fbp.Success, "suffix"),
		n.ConnectOut("suffix", fbp.Success, fbp.Success),
		n.ConnectOut("upper", fbp.Error, fbp.Error),
		n.ConnectOut("suffix", fbp.Error, fbp.Error),
	)
}

// Pipeline is a Network: Trim -> Shout (nested). Empty input leaves through
// the Outport Empty.
func Pipeline() (*fbp.Network, error) {
	shout, err := Shout()
	if err != nil {
		return nil, err
	}
	n := &fbp.Network{
		Name:        "pipeline",
		Description: "Trims a string and shouts it",
		Outports:    []string{Empty},
	}
	return n, errors.Join(
		n.Add("trim", Trim()),
		n.Add("shout", shout),
		n.ConnectIn("trim"),
		n.Connect("trim", fbp.Success, "shout"),
		n.ConnectOut("trim", Empty, Empty),
		n.ConnectOut("trim", fbp.Error, fbp.Error),
		n.ConnectOut("shout", fbp.Success, fbp.Success),
		n.ConnectOut("shout", fbp.Error, fbp.Error),
	)
}
