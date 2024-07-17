package mesage_queue

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type MessageQueueConfig struct {
	Address  string
	Protocol string
	Wg       *sync.WaitGroup
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

		go mq.handleMessage(buf, &conn)
	}

}

func (mq *MessageQueue) handleMessage(msg bytes.Buffer, connPtr *net.Conn) {

	standardReq := Decode(msg.Bytes())

	if standardReq == nil {
		log.Println("message not valid")
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

	queueMsg := Message{
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   body,
	}

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

	defer func() {
		conn.Close()
		delete(mq.channels, id.String())
		log.Printf("deleted channel %s", id.String())
	}()

	mq.channels[id.String()] = consumerChan

	log.Printf("consumer %s connected to server", conn.LocalAddr())

	// Listen for queue updates
	for msg := range consumerChan {
		// marshal message
		jsonMsg, _ := json.Marshal(msg)

		_, err := conn.Write(jsonMsg)

		if err != nil {
			log.Printf("error producing message: %s", err)
			return
		}
	}

}

func (mq *MessageQueue) NotifyConsumers(msg Message) {
	for id, channel := range mq.channels {
		log.Printf("Notifying channel %s of message", id)
		channel <- msg
	}
}
