package main

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/jmv1006/go-message-queue/metrics"

	mesage_queue "github.com/jmv1006/go-message-queue/message_queue"
)

func main() {

	var wg sync.WaitGroup

	listenerAddress := os.Getenv("LISTENER_ADDRESS")

	if listenerAddress == "" {
		listenerAddress = "0.0.0.0:8000" // default
	}

	mh := metrics.NewMetricsHandler()

	// Creating WG
	wg.Add(2)

	mqConfig := mesage_queue.MessageQueueConfig{
		Address:        listenerAddress,
		Protocol:       "tcp",
		Wg:             &wg,
		MetricsHandler: mh,
		Debug:          false,
	}

	mq := mesage_queue.New(mqConfig)

	// Starting a listener for producers
	go mq.Start()

	// Starting HTTP Listener
	go startHttpApp()

	log.Printf("Listening for tcp connections on %s TCP...", listenerAddress)

	wg.Wait()
}

func startHttpApp() {
	addr := os.Getenv("HTTP_LISTENER")

	if len(addr) == 0 {
		addr = "localhost:8080"
	}

	http.Handle("/metrics", promhttp.Handler())

	err := http.ListenAndServe(addr, nil)

	if err != nil {
		panic(err)
	}
}
