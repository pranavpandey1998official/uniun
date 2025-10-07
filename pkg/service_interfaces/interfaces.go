package serviceInterfaces

import (
	"errors"
	"uniun/pkg/domain"
)

// MessageProvider exposes how to receive inbound messages from the connection layer.
type MessageProvider interface {
	Subscribe() <-chan *domain.InboundMessage
}

// MessageSender allows sending outbound messages to a client.
type MessageSender interface {
	SendMessage(msg *domain.OutboundMessage) error
}

type ConnectionService interface {
	MessageProvider
	MessageSender
	RegisterConnection(conn any) error
	Start()
	Stop()
}

type MessageRoutingService interface {
	Start()
	Stop()
}

type SendMessageService interface {
	// Public methods
	Start()
	Stop()
	EnqueueMessage(msg *domain.OutboundMessage) error
}

// ErrBlockNotFound is returned when a block with a given ID is not found in the database.
var ErrBlockNotFound = errors.New("block not found")

type DatabaseService interface {
	Start() error
	Stop()
	StoreBlock(block *domain.Block) error
	FetchBlock(id string) (*domain.Block, error)
}

// RequestBlockService is the service for requesting blocks.
type RequestBlockService interface {
	Start()
	Stop()
	EnqueueRequest(msg *domain.InboundMessage) error
}
