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
	GetSendMessageChannel() chan<- *domain.OutboundMessage
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
	GetRequestBlockChannel() <-chan *domain.InboundMessage
	GetPublishBlockChannel() <-chan *domain.InboundMessage
	GetInterestedChainsChannel() <-chan *domain.InboundMessage
	GetWatchBlockChannel() <-chan *domain.InboundMessage
}

// this service is not used anymore send through connection service
// type SendMessageService interface {
// 	// Public methods
// 	Start()
// 	Stop()
// 	EnqueueMessage(msg *domain.OutboundMessage) error
// }

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
}

type PublishBlockService interface {
	Start()
	Stop()
	GetFilteredBlockChannel() <-chan *domain.InboundMessage
}

type InterestedChainsService interface {
	Start()
	Stop()
}

type WatchBlockService interface {
	Start()
	Stop()
}
