package messagerouting

import (
	"fmt"
	"sync"
	"uniun/pkg/domain"
	service_interfaces "uniun/pkg/service_interfaces"
)

var (
	once             sync.Once
	singletonService *messageRoutingService
)

// messageRoutingService is the private concrete implementation.
type messageRoutingService struct {
	inboundQueue <-chan *domain.InboundMessage

	// Output channels for subscribers, maintaining the routing logic
	requestBlockCh     chan *domain.InboundMessage
	publishBlockCh     chan *domain.InboundMessage
	interestedChainsCh chan *domain.InboundMessage

	stop chan struct{}
}

// GetInstance is the singleton factory method
func GetService(provider service_interfaces.ConnectionService) service_interfaces.MessageRoutingService {
	once.Do(func() {
		singletonService = &messageRoutingService{
			inboundQueue:       provider.Subscribe(),
			requestBlockCh:     make(chan *domain.InboundMessage, 128),
			publishBlockCh:     make(chan *domain.InboundMessage, 128),
			interestedChainsCh: make(chan *domain.InboundMessage, 128),
			stop:               make(chan struct{}),
		}
	})
	return singletonService
}

// run is the main loop that reads from the ConnectionService and routes messages.
func (s *messageRoutingService) Start() {
	fmt.Println("[MessageRoutingService] Started.")
	for {
		select {
		case msg, ok := <-s.inboundQueue:
			if !ok {
				// The provider's channel was closed, so we should shut down.
				return
			}
			// Pass the message to the routing logic.
			s.routeMessage(msg)
		case <-s.stop:
			// Stop signal received.
			// Close all downstream channels to signal subscribers that no more data is coming.
			close(s.requestBlockCh)
			close(s.publishBlockCh)
			close(s.interestedChainsCh)
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
	case "request.block":
		s.requestBlockCh <- msg
	case "publish.block":
		s.publishBlockCh <- msg
	case "interested.chains":
		s.interestedChainsCh <- msg
	default:
		fmt.Printf("[MessageRoutingService] Dropping unroutable message of type: %s\n", msg.Type)
	}
}

// GetRequestBlock returns the next available request.block message if any exists
func (s *messageRoutingService) GetRequestBlock() (*domain.InboundMessage, bool) {
	select {
	case msg, ok := <-s.requestBlockCh:
		return msg, ok
	default:
		return nil, true
	}
}

// GetPublishBlock returns the next available publish.block message if any exists
func (s *messageRoutingService) GetPublishBlock() (*domain.InboundMessage, bool) {
	select {
	case msg, ok := <-s.publishBlockCh:
		return msg, ok
	default:
		return nil, true
	}
}

// GetInterestedChains returns the next available interested.chains message if any exists
func (s *messageRoutingService) GetInterestedChains() (*domain.InboundMessage, bool) {
	select {
	case msg, ok := <-s.interestedChainsCh:
		return msg, ok
	default:
		return nil, true
	}
}
