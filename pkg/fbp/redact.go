package fbp

import (
	"encoding/json"
	"slices"
)

// RedactFunc returns Packet data with sensitive values hidden. It must not
// modify data; it may return it unchanged. See Brick.Redact.
type RedactFunc func(data json.RawMessage) json.RawMessage

// redactedValue replaces the value of a field hidden by RedactFields.
const redactedValue = `"[redacted]"`

// RedactFields returns a RedactFunc that replaces the values of the given
// top-level fields of a JSON object with "[redacted]", so an Event still
// shows that the field was there. Data that is no JSON object, or has none
// of the fields, is returned unchanged. Nested fields are not looked at.
func RedactFields(fields ...string) RedactFunc {
	fields = slices.Clone(fields)
	return func(data json.RawMessage) json.RawMessage {
		var obj map[string]json.RawMessage
		if json.Unmarshal(data, &obj) != nil || obj == nil {
			return data
		}
		changed := false
		for _, f := range fields {
			if _, ok := obj[f]; ok {
				obj[f] = json.RawMessage(redactedValue)
				changed = true
			}
		}
		if !changed {
			return data
		}
		out, err := json.Marshal(obj)
		if err != nil {
			return nil
		}
		return out
	}
}

// redaction is the chain of RedactFuncs that applies to a Packet: those of
// the Bricks that emitted it or one of its ancestors within the Run. A nil
// *redaction hides nothing. Packets share the chain; it is never changed.
type redaction struct {
	brick *Brick // identifies the RedactFunc, so a chain holds it only once
	fn    RedactFunc
	next  *redaction
}

// with returns r extended by the Redact of b. Without one, or if b is
// already in r, r itself is returned.
func (r *redaction) with(b *Brick, fn RedactFunc) *redaction {
	if fn == nil {
		return r
	}
	for c := r; c != nil; c = c.next {
		if c.brick == b {
			return r
		}
	}
	return &redaction{brick: b, fn: fn, next: r}
}

// apply returns data with every RedactFunc of r applied. It fails closed: if
// a RedactFunc panics or returns invalid JSON, nil is returned.
func (r *redaction) apply(data json.RawMessage) (out json.RawMessage) {
	if r == nil {
		return data
	}
	defer func() {
		if recover() != nil {
			out = nil
		}
	}()
	for c := r; c != nil; c = c.next {
		data = c.fn(data)
		if !json.Valid(data) {
			return nil
		}
	}
	return data
}
