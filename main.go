package main

import (
	"log"
	"os"
	"sync"

	"github.com/jmv1006/go-message-queue/metrics"

	mesage_queue "github.com/jmv1006/go-message-queue/message_queue"
)

func main() {

	var wg sync.WaitGroup

	listenerAddress := os.Getenv("LISTENER_ADDRESS")

	if listenerAddress == "" {
		listenerAddress = "localhost:8000" // default
	}

	mh := metrics.NewMetricsHandler()

	// Creating WG
	wg.Add(2)

	mqConfig := mesage_queue.MessageQueueConfig{
		Address:        listenerAddress,
		Protocol:       "tcp",
		Wg:             &wg,
		MetricsHandler: mh,
		Debug:          true,
	}

	mq := mesage_queue.New(mqConfig)

	// Starting a listener for producers
	go mq.Start()

	// Starting metrics handler
	go mh.StartMetricsLoop(10, mq.GetMutex())

	log.Printf("Listening for tcp connections on %s TCP...", listenerAddress)

	wg.Wait()
}
