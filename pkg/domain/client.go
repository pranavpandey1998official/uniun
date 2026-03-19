// Client entity definition for internal mapping of connected websocket clients.
package domain

import (
	"sync"

	"github.com/gorilla/websocket"
)

// Client represents a connected websocket client entity.
type Client struct {
	ID   string
	Conn *websocket.Conn
	Send chan []byte
	mu   sync.Mutex
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Conn != nil {
		c.Conn.Close()
		c.Conn = nil
	}
	if c.Send != nil {
		close(c.Send)
		c.Send = nil
	}
}
