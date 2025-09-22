package websocket

import (
	"fmt"
	"net/http"
	service_interfaces "uniun/pkg/service_interfaces"

	"github.com/gorilla/websocket"
)

type Handler struct {
	connService service_interfaces.ConnectionService // Depends on the interface
	upgrader    websocket.Upgrader
}

func NewHandler(cs service_interfaces.ConnectionService) *Handler {
	return &Handler{
		connService: cs,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("failed to upgrade connection: %v\n", err)
		return
	}
	// Registers a new client connection in mapping
	if err := h.connService.RegisterConnection(conn); err != nil {
		fmt.Printf("failed to register client: %v\n", err)
		conn.Close()
		return
	}
}
