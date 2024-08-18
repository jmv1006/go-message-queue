package mesage_queue

import (
	"log"
	"net"
	"time"
)

func (mq *MessageQueue) ProduceMessage(msg *StandardRequest, connPtr *net.TCPConn) {
	conn := *connPtr

	body := msg.Body
	topicName := msg.Topic

	// check if topic exists
	topic := mq.ValidateTopic(topicName)

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
	mq.NotifyConsumers(topic, queueMsg)

	err := conn.Close()

	if err != nil {
		log.Printf("error with closing connection: %s", err)
		return
	}
}

func (mq *MessageQueue) NotifyConsumers(topic Topic, msg Message) {
	mq.mu.Lock()

	for id, channel := range topic.channels {
		log.Printf("notifying channel %s in topic %s of message", id, topic.name)
		channel <- msg
	}

	mq.mu.Unlock()
}
