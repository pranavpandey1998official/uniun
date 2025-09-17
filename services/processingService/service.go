package processingservice

import (
	"fmt"
	"sync"
	service_interfaces "uniun/pkg/serviceInterfaces"
)

type service struct {
	provider     service_interfaces.MessageProvider
	notifier     service_interfaces.NotificationService
	stopChan     chan struct{}
	wg           sync.WaitGroup
	knownClients map[string]bool
}

// NewService is the constructor.
func NewService(p service_interfaces.MessageProvider, n service_interfaces.NotificationService) service_interfaces.ProcessingService {
	return &service{
		provider:     p,
		notifier:     n,
		stopChan:     make(chan struct{}),
		knownClients: make(map[string]bool),
	}
}

func (s *service) Start() {
	s.wg.Add(1)
	go s.processMessages()
	fmt.Println("[ProcessingService] Started.")
}

func (s *service) Stop() {
	close(s.stopChan)
	s.wg.Wait()
	fmt.Println("[ProcessingService] Stopped.")
}

func (s *service) processMessages() {
	defer s.wg.Done()
	messageQueue := s.provider.GetMessageQueue()

	for {
		select {
		case msg, ok := <-messageQueue:
			if !ok {
				return
			}
			fmt.Printf(
				"[ProcessingService] Processing message from ClientID: %s\n", msg.ClientID,
			)
			if !s.knownClients[msg.ClientID] {
				s.knownClients[msg.ClientID] = true
				if err := s.notifier.SendWelcomeMessage(msg.ClientID); err != nil {
					fmt.Printf("[ProcessingService] Error sending welcome: %v\n", err)
				}
			}
		case <-s.stopChan:
			return
		}
	}
}
