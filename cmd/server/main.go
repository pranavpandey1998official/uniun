package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	// Import concrete service packages
	connectionservice "uniun/services/connection_service"
	database "uniun/services/database"
	messagerouting "uniun/services/message_routing"
	requestblock "uniun/services/request_block"
	sendmessage "uniun/services/send_message"
)

func main() {
	// --- Dependency Injection and Service Composition ---
	// Create singleton service instance using their constructors.
	connService := connectionservice.GetService() // Singleton instance
	msgRoutingService := messagerouting.GetService()
	sendMsgService := sendmessage.GetService()
	databaseService := database.GetService()
	requestBlockService := requestblock.GetService()
	// --- Start Services ---
	log.Println("Starting services...")
	go connService.Start()
	go msgRoutingService.Start()
	go sendMsgService.Start()
	go databaseService.Start()
	go requestBlockService.Start()

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
