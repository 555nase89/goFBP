package crash

import (
	"testing"

	"github.com/555nase89/goFBP/pkg/fbp"
	"github.com/555nase89/goFBP/pkg/fbptest"
)

func TestStats(t *testing.T) {
	n, err := Stats()
	if err != nil {
		t.Fatal(err)
	}
	inst := fbptest.Start(t, n)

	inst.Run(t, `[1,2,3]`).Want(t, fbptest.Outputs{fbp.Success: {`2`}})
	inst.Run(t, `[4,-1]`).WantError(t, "check", "negative number -1")
	inst.Run(t, `[]`).WantError(t, "average", "panic: runtime error: integer divide by zero")
}
