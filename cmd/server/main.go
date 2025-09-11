package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"uniun/network/internal/transport"
	"uniun/network/internal/usecase"
)

func main() {
	mgr := usecase.NewManager()
	defer mgr.Stop()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		transport.ServeWS(mgr, w, r)
	})

	srv := &http.Server{Addr: ":8080"}

	go func() {
		log.Println("Server listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	// graceful shutdown on SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("Shutting down server")
	_ = srv.Close()
}
