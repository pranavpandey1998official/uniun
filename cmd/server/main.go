package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Import concrete service packages
	"uniun/services/connectionservice"
	"uniun/services/connectionservice/transport/websocket"
	"uniun/services/notificationservice"
	"uniun/services/processingservice"
)

func main() {
	// --- Dependency Injection and Service Composition ---
	// 1. Create concrete service instances using their constructors.
	connService := connectionservice.NewConnectionService()
	notificationSvc := notificationservice.NewService(connService)              // Injects connService as a MessageSender
	processingSvc := processingservice.NewService(connService, notificationSvc) // Injects connService and notificationSvc

	// --- Start Services ---
	connService.Start()
	processingSvc.Start()

	// --- Set up Transport Layer ---
	wsHandler := websocket.NewHandler(connService) // Inject connService into its own handler
	mux := http.NewServeMux()
	mux.Handle("/ws", wsHandler)

	server := &http.Server{Addr: ":8080", Handler: mux}

	// --- Graceful Shutdown ---
	go func() {
		log.Println("Server starting on http://localhost:8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("could not listen on %s: %v\n", server.Addr, err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down application...")

	processingSvc.Stop()
	connService.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}

	log.Println("Application gracefully stopped")
}
