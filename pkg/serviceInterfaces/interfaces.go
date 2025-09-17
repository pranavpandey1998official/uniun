package service_interfaces

import (
	"uniun/pkg/domain"

	"github.com/gorilla/websocket"
)

// --- Role Interfaces ---

// MessageProvider defines the capability of providing a stream of inbound messages.
type MessageProvider interface {
	GetMessageQueue() <-chan *domain.InboundMessage
}

// MessageSender defines the capability of sending a message to a specific client.
type MessageSender interface {
	SendMessage(msg *domain.OutboundMessage) error
}

// --- Full Service Interfaces ---

// ConnectionService is the primary interface for managing client connections.
type ConnectionService interface {
	MessageProvider // It can provide messages
	MessageSender   // It can send messages

	Start()
	Stop()
	RegisterClient(conn *websocket.Conn)
}

// ProcessingService defines the interface for our example business logic service.
type ProcessingService interface {
	Start()
	Stop()
}

// NotificationService defines the interface for a service responsible for sending notifications.
type NotificationService interface {
	SendWelcomeMessage(clientID string) error
}
