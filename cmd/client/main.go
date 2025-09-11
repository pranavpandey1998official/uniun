package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "http service address")
	flag.Parse()

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	log.Printf("connecting to %s", u.String())
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// read messages from server
	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read error:", err)
				return
			}
			fmt.Printf("<- %s\n", string(message))
		}
	}()

	// read stdin and send
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Type messages and press enter. Ctrl+C to quit.")
	for scanner.Scan() {
		text := scanner.Text()
		if err := c.WriteMessage(websocket.TextMessage, []byte(text)); err != nil {
			log.Println("write error:", err)
			break
		}
	}

	// wait for interrupt
	select {
	case <-interrupt:
		log.Println("Interrupted, closing")
		_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		time.Sleep(1 * time.Second)
	}
}
