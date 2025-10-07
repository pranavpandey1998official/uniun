package requestblock

import (
	"fmt"
	"log"
	"sync"

	"uniun/pkg/domain"
	"uniun/pkg/protobuf"
	service_interfaces "uniun/pkg/service_interfaces"
	dbservice "uniun/services/database"
	sendmessage "uniun/services/send_message"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type requestBlockService struct {
	db     service_interfaces.DatabaseService
	sender service_interfaces.SendMessageService

	// Internal channels
	requestQueue chan *domain.InboundMessage

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
			db:           dbservice.GetService(),
			sender:       sendmessage.GetService(),
			requestQueue: make(chan *domain.InboundMessage, 512),
			stop:         make(chan struct{}),
		}
	})
	return singletonService
}

// Start simply launches the processor goroutine.
func (s *requestBlockService) Start() {
	log.Println("[RequestBlockService] started")

	for {
		select {
		case msg := <-s.requestQueue:
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

// EnqueueRequest uses a select statement to be state-aware without a mutex.
func (s *requestBlockService) EnqueueRequest(msg *domain.InboundMessage) error {
	if msg == nil {
		return fmt.Errorf("received nil request")
	}

	select {
	case s.requestQueue <- msg:
		return nil
	case <-s.stop:
		return fmt.Errorf("RequestBlockService is not running")
	default:
		return fmt.Errorf("request queue is full")
	}
}

// handleRequest fetches the block and forwards the message to the send queue with appended payload.
func (s *requestBlockService) handleRequest(msg *domain.InboundMessage) error {
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
		_ = s.sender.EnqueueMessage(errMsg)
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

	if err := s.sender.EnqueueMessage(out); err != nil {
		return fmt.Errorf("enqueue outbound message failed: %w", err)
	}

	log.Printf("[RequestBlockService] responded to client %s for block %s", msg.ClientID, blockID)
	return nil
}
