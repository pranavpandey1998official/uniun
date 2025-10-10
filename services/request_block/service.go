package requestblock

import (
	"fmt"
	"log"
	"sync"

	"uniun/pkg/domain"
	"uniun/pkg/protobuf"
	service_interfaces "uniun/pkg/service_interfaces"
	connectionservice "uniun/services/connection_service"
	dbservice "uniun/services/database"
	messagerouting "uniun/services/message_routing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type requestBlockService struct {
	db         service_interfaces.DatabaseService
	sender     service_interfaces.ConnectionService
	msgRouting service_interfaces.MessageRoutingService

	// Control
	stop     chan struct{}
	stopOnce sync.Once
}

var (
	once             sync.Once
	singletonService *requestBlockService
)

// GetService returns the singleton instance.
func GetService() service_interfaces.RequestBlockService {
	once.Do(func() {
		singletonService = &requestBlockService{
			db:         dbservice.GetService(),
			sender:     connectionservice.GetService(),
			msgRouting: messagerouting.GetService(),
			stop:       make(chan struct{}),
		}
	})
	return singletonService
}

// Start simply launches the processor goroutine.
func (s *requestBlockService) Start() {
	log.Println("[RequestBlockService] started")
	reqBlockCh := s.msgRouting.GetRequestBlockChannel()
	for {
		select {
		case msg := <-reqBlockCh:
			if err := s.handleRequest(msg); err != nil {
				log.Printf("[RequestBlockService] handle error: %v", err)
			}
		case <-s.stop:
			log.Println("[RequestBlockService] processor stopped")
			return
		}
	}
}

// Stop gracefully shuts down the service using sync.Once for safety.
func (s *requestBlockService) Stop() {
	s.stopOnce.Do(func() {
		close(s.stop)
		log.Println("[RequestBlockService] stopped")
	})
}

// handleRequest fetches the block and forwards the message to the send queue with appended payload.
func (s *requestBlockService) handleRequest(msg *domain.InboundMessage) error {
	sendMessageCh := s.sender.GetSendMessageChannel()
	if msg == nil {
		return fmt.Errorf("received nil request")
	}
	// Expect the inbound payload to contain the block ID as a UTF-8 string.
	blockID := string(msg.Payload)
	if blockID == "" {
		return fmt.Errorf("empty block id in request payload")
	}

	blk, err := s.db.FetchBlock(blockID)
	if err != nil {
		// Forward a minimal error response to the client to keep the flow observable.
		errMsg := &domain.OutboundMessage{
			ClientID: msg.ClientID,
			Type:     "error.block_not_found",
			Payload:  []byte(fmt.Sprintf("block '%s' not found", blockID)),
		}
		sendMessageCh <- errMsg
		return fmt.Errorf("fetch block failed for id %s: %w", blockID, err)
	}

	// Convert domain.Block back to protobuf bytes to append to the original payload.
	pb := &protobuf.Block{
		Id:        blk.ID,
		Data:      blk.Data,
		Timestamp: timestamppb.New(blk.Timestamp),
	}
	blockBytes, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("marshal protobuf block failed: %w", err)
	}

	// Append the fetched block bytes to the original payload.
	combined := make([]byte, 0, len(msg.Payload)+len(blockBytes))
	combined = append(combined, msg.Payload...)
	combined = append(combined, blockBytes...)

	out := &domain.OutboundMessage{
		ClientID: msg.ClientID,
		// Keep the same type to correlate with the requestor, or adjust to "response.block" if preferred.
		Type:    msg.Type,
		Payload: combined,
	}

	// add more error handling later
	sendMessageCh <- out

	log.Printf("[RequestBlockService] responded to client %s for block %s", msg.ClientID, blockID)
	return nil
}
