package domain

import (
	"sync/atomic"

	"github.com/gorilla/websocket"
)

var nextID uint64

// Client represents a connected client
type Client struct {
	ID      uint64
	Conn    *websocket.Conn
	Send    chan []byte // buffered channel for outbound messages
	Closing chan struct{}
}

func NewClient(conn *websocket.Conn) *Client {
	id := atomic.AddUint64(&nextID, 1)
	return &Client{
		ID:      id,
		Conn:    conn,
		Send:    make(chan []byte, 16),
		Closing: make(chan struct{}),
	}
}

func (c *Client) Close() {
	select {
	case <-c.Closing:
		// already closed
	default:
		close(c.Closing)
		_ = c.Conn.Close()
		close(c.Send)
	}
}

func (c *Client) IsClosed() bool {
	select {
	case <-c.Closing:
		return true
	default:
		return false
	}
}
