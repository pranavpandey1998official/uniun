package notificationservice

import (
	"fmt"
	"uniun/pkg/domain"
	service_interfaces "uniun/pkg/serviceInterfaces"
)

type service struct {
	sender service_interfaces.MessageSender
}

// NewService is the constructor.
func NewService(sender service_interfaces.MessageSender) service_interfaces.NotificationService {
	return &service{
		sender: sender,
	}
}

func (s *service) SendWelcomeMessage(clientID string) error {
	fmt.Printf("[NotificationService] Sending welcome message to client %s\n", clientID)
	payload := fmt.Sprintf("Welcome, Client %s! Your connection is confirmed.", clientID)
	outboundMsg := &domain.OutboundMessage{
		ClientID: clientID,
		Type:     "server.welcome",
		Payload:  []byte(payload),
	}
	return s.sender.SendMessage(outboundMsg)
}
