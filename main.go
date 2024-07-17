package main

import (
	"log"
	"sync"

	mesage_queue "github.com/jmv1006/go-message-queue/message_queue"
)

/*
TODO
1. Listen for messages & store them
	- producers will connect to this endpoint & write to it
2. Alert any consumers of new messages (push)
 	- consumers will connect to the same endpoint & read from it
*/

func main() {

	var wg sync.WaitGroup

	// Creating MQ
	wg.Add(1)

	mqConfig := mesage_queue.MessageQueueConfig{
		Address:  "localhost:8000",
		Protocol: "tcp",
		Wg:       &wg,
	}

	mq := mesage_queue.New(mqConfig)

	// Starting a listener for producers
	mq.Start()

	log.Printf("Listening for tcp connections on port %s...", "8000")

	wg.Wait()
}
