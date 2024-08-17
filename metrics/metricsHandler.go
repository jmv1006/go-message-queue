package metrics

import (
	"log"
	"sync"
	"time"
)

type MetricsHandler struct {
	Received       int
	Sent           int
	ActiveChannels int
}

func NewMetricsHandler() *MetricsHandler {
	return &MetricsHandler{Received: 0, Sent: 0}
}

func (m *MetricsHandler) AddSent() {
	m.Sent += 1
}

func (m *MetricsHandler) AddReceived() {
	m.Received += 1
}

func (m *MetricsHandler) AddChannel() {
	m.ActiveChannels += 1
}

func (m *MetricsHandler) RemoveChannel() {
	m.ActiveChannels -= 1
}

func (m *MetricsHandler) StartMetricsLoop(delayInSeconds int, mqMutex *sync.Mutex) {
	duration := time.Second * time.Duration(delayInSeconds)

	for {
		mqMutex.Lock()
		// Print metrics
		log.Printf("%d events recieved", m.Received)
		log.Printf("%d events sent", m.Sent)
		log.Printf("%d active channels", m.ActiveChannels)
		log.Printf("--------------------")

		mqMutex.Unlock()

		time.Sleep(duration)
	}
}
