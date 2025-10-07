package domain

import "time"

// Block represents a stored data block in the system.
// It mirrors the protobuf Block but uses native Go types where appropriate.
type Block struct {
	ID        string
	Data      []byte
	Timestamp time.Time
}
