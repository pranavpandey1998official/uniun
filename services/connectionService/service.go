package connectionservice

import (
	"fmt"
	"sync"
	"uniun/pkg/domain"
	"uniun/pkg/protobuf"
	service_interfaces "uniun/pkg/serviceInterfaces"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// connectionService is the concrete implementation. It's unexported.
type connectionService struct {
	clients         map[string]*domain.Client
	mu              sync.RWMutex
	register        chan *domain.Client
	unregister      chan *domain.Client
	inboundMessages chan *domain.InboundMessage
	stop            chan struct{}
}

// NewConnectionService is the constructor that returns the public interface.
func NewConnectionService() service_interfaces.ConnectionService {
	return &connectionService{
		clients:         make(map[string]*domain.Client),
		register:        make(chan *domain.Client),
		unregister:      make(chan *domain.Client),
		inboundMessages: make(chan *domain.InboundMessage, 256),
		stop:            make(chan struct{}),
	}
}

// Start runs the connectionService's central loop in a goroutine.
func (s *connectionService) Start() {
	go s.run()
}

// Stop signals the connectionService to shut down.
func (s *connectionService) Stop() {
	close(s.stop)
}

// GetMessageQueue returns a read-only channel for inbound messages.
func (s *connectionService) GetMessageQueue() <-chan *domain.InboundMessage {
	return s.inboundMessages
}

// RegisterClient creates a new client entity and sends it to the register channel.
func (s *connectionService) RegisterClient(conn *websocket.Conn) {
	client := &domain.Client{
		ID:   uuid.NewString(), // Generate a unique local ID
		Conn: conn,
		Send: make(chan []byte, 256),
	}
	s.register <- client
}

// SendMessage finds the client and sends the message payload.
func (s *connectionService) SendMessage(msg *domain.OutboundMessage) error {
	s.mu.RLock()
	client, ok := s.clients[msg.ClientID]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("client with ID %s not found", msg.ClientID)
	}

	// Convert our domain object back to a protobuf message
	protoMsg := &protobuf.DataBlock{
		Type:    msg.Type,
		Payload: msg.Payload,
	}

	bytes, err := proto.Marshal(protoMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal outbound message: %w", err)
	}

	// Send the marshalled bytes to the client's send channel.
	// This is non-blocking and safe for concurrent calls.
	select {
	case client.Send <- bytes:
	default:
		// The client's send buffer is full. We can choose to drop the message
		// or handle it in another way (e.g., log, close connection).
		return fmt.Errorf("client %s send buffer is full", msg.ClientID)
	}

	return nil
}

// run is the central loop that manages the connectionService's state.
// By handling registrations and unregistrations here, we avoid
// needing locks for map modifications.
func (s *connectionService) run() {
	fmt.Println("Connection Service started.")
	defer func() {
		fmt.Println("Connection Service stopped.")
	}()

	for {
		select {
		case client := <-s.register:
			s.mu.Lock()
			s.clients[client.ID] = client
			s.mu.Unlock()
			fmt.Printf("Client registered: %s\n", client.ID)
			// Start goroutines to handle reading and writing for this client.
			go s.writePump(client)
			go s.readPump(client)

		case client := <-s.unregister:
			s.mu.Lock()
			if _, ok := s.clients[client.ID]; ok {
				delete(s.clients, client.ID)
				client.Close()
				fmt.Printf("Client unregistered: %s\n", client.ID)
			}
			s.mu.Unlock()

		case <-s.stop:
			s.mu.Lock()
			for id, client := range s.clients {
				fmt.Printf("Closing connection for client: %s\n", id)
				client.Close()
				delete(s.clients, id)
			}
			s.mu.Unlock()
			close(s.inboundMessages) // Signal consumers that we are done.
			return
		}
	}
}

// readPump pumps messages from the websocket connection to the inboundMessages channel.
// This is the "receiveMessage" and "convertMessage" logic.
func (s *connectionService) readPump(client *domain.Client) {
	defer func() {
		// Ensure unregistration happens on any exit from this function.
		s.unregister <- client
	}()

	for {
		// Read message from websocket
		_, messageBytes, err := client.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				fmt.Printf("error reading message from client %s: %v\n", client.ID, err)
			}
			break
		}

		// Convert protobuf bytes to object instance
		protoMsg := &protobuf.DataBlock{}
		if err := proto.Unmarshal(messageBytes, protoMsg); err != nil {
			fmt.Printf("error unmarshalling protobuf from client %s: %v\n", client.ID, err)
			continue
		}

		// Create the internal domain object with metadata (client ID)
		inboundMsg := &domain.InboundMessage{
			ClientID: client.ID,
			Type:     protoMsg.Type,
			Payload:  protoMsg.Payload,
		}

		// Push the object instance into the queue for other services
		s.inboundMessages <- inboundMsg
	}
}

// writePump pumps messages from the client's Send channel to the websocket connection.
func (s *connectionService) writePump(client *domain.Client) {
	defer func() {
		if client != nil && client.Conn != nil {
			client.Conn.Close()
		}
	}()

	for message := range client.Send {
		// If connection was cleared elsewhere, stop the pump.
		if client == nil || client.Conn == nil {
			return
		}
		if err := client.Conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
			fmt.Printf("error writing message to client %s: %v\n", client.ID, err)
			return
		}
	}
}
