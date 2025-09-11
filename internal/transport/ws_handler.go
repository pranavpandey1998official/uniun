package transport

import (
	"fmt"
	"net/http"
	"time"
	"uniun/network/internal/domain"
	"uniun/network/internal/usecase"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// ServeWS upgrades the http connection and hooks client to manager
func ServeWS(mgr *usecase.Manager, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "could not upgrade", http.StatusBadRequest)
		return
	}
	client := domain.NewClient(conn)
	mgr.Register <- client

	// start read and write pumps
	go readPump(mgr, client)
	go writePump(client)
}

func readPump(mgr *usecase.Manager, c *domain.Client) {
	defer func() {
		mgr.Unregister <- c.ID
	}()
	c.Conn.SetReadLimit(512)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error { c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second)); return nil })

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			fmt.Printf("read error from %d: %v\n", c.ID, err)
			return
		}
		// push to ingress channel
		mgr.Ingress <- usecase.IngressMessage{From: c.ID, Data: message}
	}
}

func writePump(c *domain.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case msg, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				// channel closed
				_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(msg)
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			// send ping to keep connection alive
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.Closing:
			return
		}
	}
}
