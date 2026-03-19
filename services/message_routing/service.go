package messagerouting

import (
	"fmt"
	"sync"
	"uniun/pkg/domain"
	service_interfaces "uniun/pkg/service_interfaces"
	connectionservice "uniun/services/connection_service"
)

var (
	once             sync.Once
	singletonService *messageRoutingService
)

// messageRoutingService is the private concrete implementation.
type messageRoutingService struct {
	connService service_interfaces.ConnectionService

	reqBlockCh         chan *domain.InboundMessage
	publishBlockCh     chan *domain.InboundMessage
	interestedChainsCh chan *domain.InboundMessage
	watchBlockCh       chan *domain.InboundMessage

	stop chan struct{}
}

// GetInstance is the singleton factory method
func GetService() service_interfaces.MessageRoutingService {
	once.Do(func() {
		singletonService = &messageRoutingService{
			connService:        connectionservice.GetService(),
			reqBlockCh:         make(chan *domain.InboundMessage, 128),
			publishBlockCh:     make(chan *domain.InboundMessage, 128),
			interestedChainsCh: make(chan *domain.InboundMessage, 128),
			watchBlockCh:       make(chan *domain.InboundMessage, 128),
			stop:               make(chan struct{}),
		}
	})
	return singletonService
}

// run is the main loop that reads from the ConnectionService and routes messages.
func (s *messageRoutingService) Start() {
	inboundQueue := s.connService.Subscribe()
	fmt.Println("[MessageRoutingService] Started.")
	for {
		select {
		case msg, ok := <-inboundQueue:
			if !ok {
				// The provider's channel was closed, so we should shut down.
				return
			}
			// Pass the message to the routing logic.
			s.routeMessage(msg)
		case <-s.stop:
			// Stop signal received.
			// Close all downstream channels to signal subscribers that no more data is coming.
			close(s.reqBlockCh)
			close(s.publishBlockCh)
			close(s.interestedChainsCh)
			close(s.watchBlockCh)
			fmt.Println("[MessageRoutingService] Stopped.")
			return
		}
	}
}

// Stop gracefully shuts down the service by closing the stop channel.
func (s *messageRoutingService) Stop() {
	close(s.stop)
}

// routeMessage deciphers the message type and passes it to the correct downstream channel.
func (s *messageRoutingService) routeMessage(msg *domain.InboundMessage) {
	// The core routing logic remains the same, as this is the service's primary responsibility.
	switch msg.Type {
	case "close.client":
		s.reqBlockCh <- msg
		s.interestedChainsCh <- msg
		s.watchBlockCh <- msg
	case "request.block":
		s.reqBlockCh <- msg
	case "publish.block":
		s.publishBlockCh <- msg
	case "interested.chains":
		s.interestedChainsCh <- msg
	case "watch.thought":
		s.watchBlockCh <- msg
	default:
		fmt.Printf("[MessageRoutingService] Dropping unroutable message of type: %s\n", msg.Type)
	}
}

func (s *messageRoutingService) GetRequestBlockChannel() <-chan *domain.InboundMessage {
	return s.reqBlockCh
}

func (s *messageRoutingService) GetPublishBlockChannel() <-chan *domain.InboundMessage {
	return s.publishBlockCh
}

func (s *messageRoutingService) GetInterestedChainsChannel() <-chan *domain.InboundMessage {
	return s.interestedChainsCh
}

func (s *messageRoutingService) GetWatchBlockChannel() <-chan *domain.InboundMessage {
	return s.watchBlockCh
}
