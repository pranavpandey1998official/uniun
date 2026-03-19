package main

import (
	"bufio"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"
	"uniun/pkg/protobuf" // Uses the same shared protobuf definition

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// Server address
const serverAddr = "localhost:8080"

func main() {
	fmt.Println("Starting Go WebSocket Client...")
	fmt.Printf("Connecting to %s...\n", serverAddr)

	// Setup a channel to catch interrupt signals (like Ctrl+C) for graceful shutdown.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Define the WebSocket server URL.
	u := url.URL{Scheme: "ws", Host: serverAddr, Path: "/ws"}
	log.Printf("Connecting to %s", u.String())

	// Dial the server to establish the connection.
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("Dial failed: %v", err)
	}
	defer c.Close()
	fmt.Println("✅ Connected to server. Type a message and press Enter to send. Press Ctrl+C to quit.")

	// Create a 'done' channel to signal when the read goroutine has finished.
	done := make(chan struct{})

	// Start a separate goroutine to continuously read messages from the server.
	go func() {
		defer close(done) // Close the 'done' channel when this function returns.
		for {
			_, messageBytes, err := c.ReadMessage()
			if err != nil {
				// This error is expected when we close the connection.
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("Read error: %v", err)
				}
				return // Exit the goroutine.
			}

			// Unmarshal the protobuf message.
			msg := &protobuf.DataBlock{}
			proto.Unmarshal(messageBytes, msg)

			// Print the received message.
			fmt.Printf("\n[<-- FROM SERVER] Type: %s, Payload: %s\n> ", msg.Type, string(msg.Payload))
		}
	}()

	// Start a scanner to read user input from the console.
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	// Main loop: handles user input and shutdown signals.
	for {
		select {
		case <-done:
			// If the read goroutine finished (e.g., server disconnected), exit.
			return
		case <-interrupt:
			// If Ctrl+C is pressed, send a clean close message to the server.
			log.Println("Interrupt signal received, closing connection...")
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("Write close error:", err)
				return
			}
			// Wait a moment for the message to be sent before exiting.
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		default:
			// This is a non-blocking way to check for user input.
			if scanner.Scan() {
				text := scanner.Text()
				if text == "" {
					fmt.Print("> ")
					continue
				}
				typ := "chat.message"
				payload := text
				if i := strings.IndexByte(text, ':'); i > 0 {
					typ = text[:i]
					payload = text[i+1:]
				}
				// Construct the protobuf message from user input.
				msgToSend := &protobuf.DataBlock{
					Type:    typ,
					Payload: []byte(payload),
				}
				bytesToSend, err := proto.Marshal(msgToSend)
				if err != nil {
					log.Printf("Failed to marshal message: %v", err)
					continue
				}

				// Send the message to the server.
				err = c.WriteMessage(websocket.BinaryMessage, bytesToSend)
				if err != nil {
					log.Println("Write error:", err)
					return
				}
				fmt.Print("> ")
			}
		}
	}
}
