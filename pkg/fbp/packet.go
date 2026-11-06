// Package fbp is the core of goFBP: Bricks, Networks, Ports and the
// in-process runtime that executes a Network as an Instance using Go channels.
// Terms follow GLOSSARY.md in the project root.
package fbp

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
)

// Packet is a single unit of data travelling along a Connection.
type Packet struct {
	Data  json.RawMessage
	RunID string
	id    uint64 // Packet ID, set only while the Instance is observed
}

// clone returns a copy whose Data does not share memory with p.
func (p Packet) clone() Packet {
	p.Data = slices.Clone(p.Data)
	return p
}

// DataAs unmarshals the Data of p into a value of type T.
func DataAs[T any](p Packet) (T, error) {
	var v T
	err := json.Unmarshal(p.Data, &v)
	return v, err
}

// ErrorData is the payload a Brick sends through its Error Outport when its
// ProcessFunc returns an error or panics.
type ErrorData struct {
	Node    string          `json:"node"`
	Brick   string          `json:"brick"`
	Message string          `json:"message"`
	Input   json.RawMessage `json:"input,omitempty"`
}

// NewRunID returns a new Run ID, a random UUID (version 4).
func NewRunID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
