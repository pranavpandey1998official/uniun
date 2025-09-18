package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	// Import concrete service packages
	connectionservice "uniun/services/connectionService"
	"uniun/services/notificationservice"
	"uniun/services/processingservice"
)

func main() {
	// --- Dependency Injection and Service Composition ---
	// 1. Create concrete service instances using their constructors.
	connService := connectionservice.GetService()                               // Singleton instance
	notificationSvc := notificationservice.NewService(connService)              // Injects connService as a MessageSender
	processingSvc := processingservice.NewService(connService, notificationSvc) // Injects connService and notificationSvc

	// --- Start Services ---
	connService.Start()
	processingSvc.Start()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down application...")

	processingSvc.Stop()
	connService.Stop()

	log.Println("Application gracefully stopped")
}
