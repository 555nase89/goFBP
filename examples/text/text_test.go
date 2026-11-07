package text

import (
	"testing"

	"github.com/555nase89/goFBP/pkg/fbp"
	"github.com/555nase89/goFBP/pkg/fbptest"
)

func TestTrim(t *testing.T) {
	fbptest.CallBrick(t, Trim(), `"  hello "`).Want(t, fbptest.Outputs{fbp.Success: {`"hello"`}})
	fbptest.CallBrick(t, Trim(), `"   "`).Want(t, fbptest.Outputs{Empty: {`""`}})
	fbptest.CallBrick(t, Trim(), `42`).WantError(t, "", "not a JSON string")
}

func TestPipeline(t *testing.T) {
	n, err := Pipeline()
	if err != nil {
		t.Fatal(err)
	}
	inst := fbptest.Start(t, n)

	tests := []struct {
		in   string
		want fbptest.Outputs
	}{
		{`"  hello "`, fbptest.Outputs{fbp.Success: {`"HELLO!"`}}},
		{`"   "`, fbptest.Outputs{Empty: {`""`}}},
	}
	for _, tt := range tests {
		inst.Run(t, tt.in).Want(t, tt.want)
	}
	inst.Run(t, `42`).WantError(t, "trim", "not a JSON string")
}
