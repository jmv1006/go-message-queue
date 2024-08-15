package mesage_queue

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmv1006/go-message-queue/metrics"
)

type MessageQueueConfig struct {
	Address        string
	Protocol       string
	Wg             *sync.WaitGroup
	MetricsHandler *metrics.MetricsHandler
}

type MessageQueue struct {
	cfg      MessageQueueConfig
	channels map[string]chan Message
}

type Message struct {
	Timestamp string `json:"timestamp" binding:"required"`
	Payload   string `json:"payload" binding:"required"`
}

type StandardRequest struct {
	Type string `json:"type"`
	Body string `json:"body"`
}

func New(config MessageQueueConfig) *MessageQueue {
	return &MessageQueue{cfg: config, channels: make(map[string]chan Message)}
}

func (mq *MessageQueue) Start() {
	defer mq.cfg.Wg.Done()

	// Starting TCP listener
	ln, err := net.Listen(mq.cfg.Protocol, mq.cfg.Address)

	if err != nil {
		log.Println(err)
		return
	}

	for {
		conn, err := ln.Accept()

		if err != nil {
			log.Println(err)
			continue
		}

		var buf bytes.Buffer
		_, err = io.Copy(&buf, conn)

		if err != nil {
			return
		}

		mq.handleMessage(buf, &conn)
	}

}

func (mq *MessageQueue) handleMessage(msg bytes.Buffer, connPtr *net.Conn) {
	conn := *connPtr
	standardReq := Decode(msg.Bytes())

	if standardReq == nil {
		log.Println("message not valid")
		conn.Close()
		return
	}

	reqType := standardReq.Type

	if reqType == "PRODUCE" {
		// produce message to queue
		mq.ProduceMessage(standardReq, connPtr)
	} else if reqType == "CONSUME" {
		// establish consumer connection, using go routine to not block other processes
		go mq.CreateConsumerStream(connPtr)
	}

}

func (mq *MessageQueue) ProduceMessage(msg *StandardRequest, connPtr *net.Conn) {
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
	mq.cfg.MetricsHandler.AddReceived()

	// Notifying channels
	mq.NotifyConsumers(queueMsg)

	err := conn.Close()

	if err != nil {
		log.Printf("error with closing connection: %s", err)
		return
	}
}

func (mq *MessageQueue) CreateConsumerStream(connPtr *net.Conn) {
	conn := *connPtr

	// Create a new channel
	consumerChan := make(chan Message)
	id := uuid.New()

	mq.channels[id.String()] = consumerChan
	// metrics
	mq.cfg.MetricsHandler.AddChannel()

	defer func() {
		conn.Close()
		fmt.Println("EXITING")
		mq.cfg.MetricsHandler.RemoveChannel()
		delete(mq.channels, id.String())

	}()

	log.Printf("consumer %s connected to server", conn.LocalAddr())

	// Listen for queue updates
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

func (mq *MessageQueue) handleChannelClosure(ch chan Message, connPtr *net.Conn) {
	// Check if connection is alive every 3 seconds
	conn := *connPtr

	for {
		_, err := conn.Read(make([]byte, 1))

		if err != nil {
			if err == io.EOF {
				continue // Connection was closed by the peer
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Connection is still open, but timed out
			}

			close(ch)
			return
		}
	}

}

func (mq *MessageQueue) NotifyConsumers(msg Message) {
	for id, channel := range mq.channels {
		log.Printf("notifying channel %s of message", id)
		channel <- msg
	}
}
