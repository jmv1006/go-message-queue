package mesage_queue

import (
	"log"
	"net"
	"time"
)

func (mq *MessageQueue) ProduceMessage(msg *StandardRequest, connPtr *net.TCPConn) {
	body := msg.Body
	conn := *connPtr

	if len([]byte(body)) > 1000 {
		log.Printf("recieved msg over 1000 bytes")
		return
	}

	queueMsg := Message{
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   body,
	}

	// metrics
	mq.mu.Lock()
	mq.cfg.MetricsHandler.AddReceived()
	mq.mu.Unlock()

	// Notifying channels
	mq.NotifyConsumers(queueMsg)

	err := conn.Close()

	if err != nil {
		log.Printf("error with closing connection: %s", err)
		return
	}
}

func (mq *MessageQueue) NotifyConsumers(msg Message) {
	mq.mu.Lock()

	for id, channel := range mq.channels {
		log.Printf("notifying channel %s of message", id)
		channel <- msg
	}

	mq.mu.Unlock()
}
