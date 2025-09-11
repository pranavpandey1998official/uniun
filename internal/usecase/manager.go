package usecase

import (
	"fmt"
	"sync"
	"uniun/network/internal/domain"
)

// IngressMessage bundles message with sender id
type IngressMessage struct {
	From uint64
	Data []byte
}

// Manager orchestrates clients, provides thread-safe map and ingress channel
type Manager struct {
	mu         sync.RWMutex
	clients    map[uint64]*domain.Client
	Ingress    chan IngressMessage
	Register   chan *domain.Client
	Unregister chan uint64
	quit       chan struct{}
}

func NewManager() *Manager {
	m := &Manager{
		clients:    make(map[uint64]*domain.Client),
		Ingress:    make(chan IngressMessage, 64),
		Register:   make(chan *domain.Client, 8),
		Unregister: make(chan uint64, 8),
		quit:       make(chan struct{}),
	}
	go m.run()
	return m
}

func (m *Manager) run() {
	// central event loop: register/unregister/ingress
	for {
		select {
		case c := <-m.Register:
			m.mu.Lock()
			m.clients[c.ID] = c
			m.mu.Unlock()
			fmt.Printf("Client registered: %d\n", c.ID)
		case id := <-m.Unregister:
			m.mu.Lock()
			if c, ok := m.clients[id]; ok {
				c.Close()
				delete(m.clients, id)
				fmt.Printf("Client unregistered: %d\n", id)
			}
			m.mu.Unlock()
		case msg := <-m.Ingress:
			// example routing: broadcast to everyone except sender
			m.mu.RLock()
			for id, c := range m.clients {
				if id == msg.From {
					continue
				}
				// non-blocking send to avoid goroutine leaks; drop if full
				select {
				case c.Send <- msg.Data:
				default:
					// channel full: decide to drop or unregister
					fmt.Printf("Send channel full for %d, dropping message\n", id)
				}
			}
			m.mu.RUnlock()
		case <-m.quit:
			// cleanup
			m.mu.Lock()
			for _, c := range m.clients {
				c.Close()
			}
			m.clients = nil
			m.mu.Unlock()
			return
		}
	}
}

func (m *Manager) Stop() {
	close(m.quit)
}

// Safe getters
func (m *Manager) GetClient(id uint64) (*domain.Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.clients[id]
	return c, ok
}
