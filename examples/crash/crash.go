// Package crash is an example of a Network that runs into errors: one Brick
// rejects bad input with an error, another one has a bug and panics on empty
// input. Both end up as ErrorData on the Error Outport of the Network.
package crash

import (
	"context"
	"errors"
	"fmt"

	"github.com/555nase89/goFBP/pkg/fbp"
)

// Check passes a JSON array of integers on unchanged and fails with an
// error if one of them is negative.
func Check() *fbp.Brick {
	return &fbp.Brick{
		Name:        "Check",
		Description: "Rejects arrays containing negative numbers",
		Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
			nums, err := fbp.DataAs[[]int](p)
			if err != nil {
				return errors.New("input is not a JSON array of integers")
			}
			for i, n := range nums {
				if n < 0 {
					return fmt.Errorf("negative number %d at index %d", n, i)
				}
			}
			return out.Emit(ctx, fbp.Success, p.Data)
		},
	}
}

// Average sends the integer average of a JSON array of integers. It has a
// deliberate bug: it divides by the length without checking for an empty
// array, so [] panics with "integer divide by zero".
func Average() *fbp.Brick {
	return &fbp.Brick{
		Name:        "Average",
		Description: "Integer average of an array (panics on empty arrays)",
		Process: func(ctx context.Context, p fbp.Packet, out fbp.Emitter) error {
			nums, err := fbp.DataAs[[]int](p)
			if err != nil {
				return errors.New("input is not a JSON array of integers")
			}
			sum := 0
			for _, n := range nums {
				sum += n
			}
			return fbp.EmitAs(ctx, out, fbp.Success, sum/len(nums))
		},
	}
}

// Stats is a Network: Check -> Average. Errors of both Bricks leave through
// the Error Outport.
func Stats() (*fbp.Network, error) {
	n := &fbp.Network{Name: "stats", Description: "Average of non-negative integers"}
	return n, errors.Join(
		n.Add("check", Check()),
		n.Add("average", Average()),
		n.ConnectIn("check"),
		n.Connect("check", fbp.Success, "average"),
		n.ConnectOut("average", fbp.Success, fbp.Success),
		n.ConnectOut("check", fbp.Error, fbp.Error),
		n.ConnectOut("average", fbp.Error, fbp.Error),
	)
}
