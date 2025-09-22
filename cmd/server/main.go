package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	// Import concrete service packages
	connectionservice "uniun/services/connection_service"
	messagerouting "uniun/services/message_routing"
	sendmessage "uniun/services/send_message"
)

func main() {
	// --- Dependency Injection and Service Composition ---
	// Create singleton service instance using their constructors.
	connService := connectionservice.GetService() // Singleton instance
	msgRoutingService := messagerouting.GetService(connService)
	sendMsgService := sendmessage.GetService(connService)

	// --- Start Services ---
	log.Println("Starting services...")
	go connService.Start()
	go msgRoutingService.Start()
	go sendMsgService.Start()

	// Wait for interrupt signal to gracefully shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down application...")

	connService.Stop()
	msgRoutingService.Stop()
	sendMsgService.Stop()

	log.Println("Application gracefully stopped")
}
