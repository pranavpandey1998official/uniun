// message definitions for internal server communication

package domain

// InboundMessage represents a message received from a client.
type InboundMessage struct {
	ClientID string
	Type     string
	Payload  []byte
}

// OutboundMessage represents a message destined for a specific client.
type OutboundMessage struct {
	ClientID string
	Type     string
	Payload  []byte
}

// CloseMessage represents a message to close a connection.
type CloseMessage struct {
	ClientID string
}
