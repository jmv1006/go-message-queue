package mesage_queue

import (
	"encoding/base64"
	"encoding/json"
	"github.com/google/uuid"
	"log"
	"net"
)

func (mq *MessageQueue) CreateConsumerStream(req *StandardRequest, connPtr *net.TCPConn) {
	conn := *connPtr

	topicName := req.Topic

	// Check if topic exists
	topic := mq.ValidateTopic(topicName)

	// Create a new channel
	consumerChan := make(chan Message)
	id := uuid.New()

	// Updating channels map
	mq.mu.Lock()
	topic.channels[id.String()] = consumerChan
	mq.mu.Unlock()

	// metrics
	mq.mu.Lock()
	mq.cfg.MetricsHandler.AddChannel()
	mq.mu.Unlock()

	defer func() {
		// Closing Connection
		err := conn.Close()
		if err != nil {
			log.Printf("%s", err)
			return
		}

		// Remove channel from metrics count
		mq.mu.Lock()
		mq.cfg.MetricsHandler.RemoveChannel()
		mq.mu.Unlock()

		// Deleting channel from map
		mq.mu.Lock()
		delete(topic.channels, id.String())
		mq.mu.Unlock()
	}()

	if mq.cfg.Debug {
		log.Printf("consumer %s connected to the server as a consumer stream", conn.LocalAddr())
	}

	// Heartbeat checker
	go mq.checkConnectionHeartbeat(connPtr, consumerChan)

	// Listen for queue updates - does not stop until the channel is closed
	for msg := range consumerChan {
		// marshal message
		jsonMsg, _ := json.Marshal(msg)

		resultMsg := make([]byte, 1000)

		base64.StdEncoding.Encode(resultMsg, jsonMsg)

		_, err := conn.Write(resultMsg)

		if err != nil {
			log.Printf("error producing message: %s", err)
			return
		}
	}

}