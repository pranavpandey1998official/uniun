package serviceInterfaces

import (
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
	GetRequestBlock() (*domain.InboundMessage, bool)
	GetPublishBlock() (*domain.InboundMessage, bool)
	GetInterestedChains() (*domain.InboundMessage, bool)
}

type SendMessageService interface {
	// Public methods
	Start()
	Stop()
	EnqueueMessage(msg *domain.OutboundMessage) error
}
