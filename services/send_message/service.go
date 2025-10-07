package sendmessage

import (
	"fmt"
	"log"
	"sync"
	"uniun/pkg/domain"
	service_interfaces "uniun/pkg/service_interfaces"
	connectionservice "uniun/services/connection_service"
)

// sendMessageService is the concrete implementation following singleton pattern
type sendMessageService struct {
	// Dependencies
	connService service_interfaces.ConnectionService

	// Internal channels
	messageQueue chan *domain.OutboundMessage

	// Control channels
	stop chan struct{}

	// State management
	mu     sync.RWMutex
	active bool
}

var (
	once             sync.Once
	singletonService *sendMessageService
)

// GetService returns the singleton instance of SendMessageService
func GetService() service_interfaces.SendMessageService {
	once.Do(func() {
		singletonService = &sendMessageService{
			connService:  connectionservice.GetService(),
			messageQueue: make(chan *domain.OutboundMessage, 1000), // Buffered channel for high throughput
			stop:         make(chan struct{}),
			active:       false,
		}
	})
	return singletonService
}

// Start begins the message processing loop
func (s *sendMessageService) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active {
		log.Println("SendMessageService is already running")
		return
	}

	s.active = true
	log.Println("[SendMessageService] started")

	// Start the message processing goroutine
	go s.processMessages()
}

// Stop gracefully shuts down the service
func (s *sendMessageService) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.active {
		log.Println("SendMessageService is already stopped")
		return
	}

	log.Println("SendMessageService stopping...")
	close(s.stop)
	s.active = false
	log.Println("SendMessageService stopped")
}

// EnqueueMessage provides an abstraction to push messages into the internal queue.
func (s *sendMessageService) EnqueueMessage(msg *domain.OutboundMessage) error {
	if msg == nil {
		return fmt.Errorf("received nil message")
	}

	// Lock for checking if the service is running
	s.mu.RLock()
	active := s.active
	s.mu.RUnlock()
	if !active {
		return fmt.Errorf("SendMessageService is not running")
	}

	// Validate before enqueuing
	if err := s.validateMessage(msg); err != nil {
		return fmt.Errorf("message validation failed: %w", err)
	}

	// Non-blocking enqueue with backpressure signal
	select {
	case s.messageQueue <- msg:
		return nil
	default:
		return fmt.Errorf("message queue is full")
	}
}

// processMessages is the main message processing loop
func (s *sendMessageService) processMessages() {
	log.Println("[SendMessageService] message processor started")

	for {
		select {
		case msg := <-s.messageQueue:
			if err := s.handleMessage(msg); err != nil {
				log.Printf("Error handling message: %v", err)
			}
		case <-s.stop:
			log.Println("SendMessageService message processor stopped")
			return
		}
	}
}

// handleMessage processes a single message
func (s *sendMessageService) handleMessage(msg *domain.OutboundMessage) error {
	if msg == nil {
		return fmt.Errorf("received nil message")
	}

	// Validate message
	if err := s.validateMessage(msg); err != nil {
		return fmt.Errorf("message validation failed: %w", err)
	}

	// Send directly to connection service
	if err := s.connService.SendMessage(msg); err != nil {
		return fmt.Errorf("failed to send message to client: %w", err)
	}

	log.Printf("Message sent successfully to client %s", msg.ClientID)
	return nil
}

// validateMessage validates the outbound message
func (s *sendMessageService) validateMessage(msg *domain.OutboundMessage) error {
	if msg.ClientID == "" {
		return fmt.Errorf("client ID cannot be empty")
	}

	if msg.Type == "" {
		return fmt.Errorf("message type cannot be empty")
	}

	if len(msg.Payload) == 0 {
		return fmt.Errorf("message payload cannot be empty")
	}

	return nil
}
