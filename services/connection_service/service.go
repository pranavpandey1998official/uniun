package connectionservice

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"uniun/pkg/domain"
	"uniun/pkg/protobuf"
	service_interfaces "uniun/pkg/service_interfaces"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// connectionService is the concrete implementation.
type connectionService struct {
	clients       map[string]*domain.Client
	mu            sync.RWMutex
	register      chan *domain.Client
	unregister    chan *domain.Client
	subscribers   []chan *domain.InboundMessage
	subsScriberMu sync.RWMutex
	server        *http.Server
	stop          chan struct{}
	sendMessageCh chan *domain.OutboundMessage
}

var (
	once             sync.Once
	singletonService *connectionService
)

// NewConnectionService is the constructor that returns the public interface.
func GetService() service_interfaces.ConnectionService {
	once.Do(func() {
		singletonService = &connectionService{
			clients:       make(map[string]*domain.Client),
			register:      make(chan *domain.Client),
			unregister:    make(chan *domain.Client),
			subscribers:   make([]chan *domain.InboundMessage, 0),
			stop:          make(chan struct{}),
			sendMessageCh: make(chan *domain.OutboundMessage, 1000),
		}
	})
	return singletonService
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all origins for demo
	},
}

// Start runs the connectionService's central loop in a goroutine.
func (s *connectionService) Start() {
	log.Println("[ConnectionService] started")
	websocketHandler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			fmt.Printf("failed to upgrade connection: %v\n", err)
			return
		}
		s.registerClient(conn)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", websocketHandler)
	s.server = &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		log.Println("Server starting on http://localhost:8080")
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("could not listen on %s: %v\n", s.server.Addr, err)
		}
	}()
	<-s.stop

	//Start the consumer goroutine to process outgoing messages.
	go func() {
		for msg := range s.sendMessageCh {
			if err := s.sendMessage(msg); err != nil {
				log.Printf("Error sending message to client %s: %v", msg.ClientID, err)
			}
		}
	}()
}

// GetSendMessageChannel returns the directional send only channel for messages.
func (s *connectionService) GetSendMessageChannel() chan<- *domain.OutboundMessage {
	return s.sendMessageCh
}

// Stop signals the connectionService to shut down.
func (s *connectionService) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
	close(s.stop)
}

// GetMessageQueue returns a read-only channel for inbound messages.
func (s *connectionService) Subscribe() <-chan *domain.InboundMessage {
	s.subsScriberMu.Lock()
	defer s.subsScriberMu.Unlock()

	ch := make(chan *domain.InboundMessage, 100)
	s.subscribers = append(s.subscribers, ch)
	return ch
}

// abstraction for registering a client connection
func (s *connectionService) RegisterConnection(conn any) error {
	wsConn, ok := conn.(*websocket.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type: expected *websocket.Conn")
	}
	s.registerClient(wsConn)
	return nil
}

// RegisterClient creates a new client entity and sends it to the register channel.
func (s *connectionService) registerClient(conn *websocket.Conn) {
	client := &domain.Client{
		ID:   uuid.NewString(), // Generate a unique local ID
		Conn: conn,
		Send: make(chan []byte, 256),
	}
	s.mu.Lock()
	s.clients[client.ID] = client
	s.mu.Unlock()
	fmt.Printf("Client registered: %s\n", client.ID)
	// Start goroutines to handle reading and writing for this client.
	go s.writePump(client)
	go s.readPump(client)
}

// SendMessage finds the client and sends the message payload.
func (s *connectionService) sendMessage(msg *domain.OutboundMessage) error {
	// add more error handling later
	client, ok := s.clients[msg.ClientID]

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

// readPump pumps messages from the websocket connection to the inboundMessages channel.
// This is the "receiveMessage" and "convertMessage" logic.
func (s *connectionService) readPump(client *domain.Client) {
	defer func() {
		s.mu.Lock()
		if _, ok := s.clients[client.ID]; ok {
			delete(s.clients, client.ID)
			client.Close()
			fmt.Printf("Client unregistered: %s\n", client.ID)
		}
		s.mu.Unlock()

		//send a close client message to other services
		// later add a close message type to the protobuf
		s.sendMessagesToSubscribers(&domain.InboundMessage{
			ClientID: client.ID,
			Type:     "close.client",
			Payload:  nil,
		})
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
		s.sendMessagesToSubscribers(inboundMsg)
	}
}

func (s *connectionService) sendMessagesToSubscribers(msg *domain.InboundMessage) {
	s.subsScriberMu.RLock()
	defer s.subsScriberMu.RUnlock()
	for _, ch := range s.subscribers {
		ch <- msg
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
