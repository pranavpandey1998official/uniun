package service_interfaces

type ConnectionService interface {
	Start()
	Stop()
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
