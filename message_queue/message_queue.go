package mesage_queue

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
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
	mu       sync.Mutex
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

	// Locking mutex to start
	mq.mu.Lock()

	addr, err := net.ResolveTCPAddr(mq.cfg.Protocol, mq.cfg.Address)

	if err != nil {
		log.Println(err)
		return
	}
	// Starting TCP listener
	ln, err := net.ListenTCP(mq.cfg.Protocol, addr)

	if err != nil {
		log.Println(err)
		return
	}

	for {
		conn, err := ln.AcceptTCP()

		if err != nil {
			log.Println(err)
			continue
		}

		// handle this connection on another threat
		go mq.handleConnection(conn)
	}

}

func (mq *MessageQueue) handleConnection(connPtr *net.TCPConn) {
	// Read from connection
	scanner := bufio.NewScanner(connPtr)

	for scanner.Scan() {
		req := Decode(scanner.Bytes())

		if req == nil {
			continue
		}

		if req.Type == "PRODUCE" {
			mq.ProduceMessage(req, connPtr)
		} else if req.Type == "CONSUME" {
			// This will become a consuming connection
			mq.CreateConsumerStream(connPtr)
		}
	}

}

func (mq *MessageQueue) CreateConsumerStream(connPtr *net.TCPConn) {
	conn := *connPtr

	// Create a new channel
	consumerChan := make(chan Message)
	id := uuid.New()

	// Updating channels map
	mq.mu.Unlock()
	mq.channels[id.String()] = consumerChan
	mq.mu.Lock()

	// metrics
	mq.cfg.MetricsHandler.AddChannel()

	defer func() {
		// Closing Connection
		err := conn.Close()
		if err != nil {
			log.Printf("%s", err)
			return
		}
		// Remove channel from metrics count
		mq.cfg.MetricsHandler.RemoveChannel()

		// Deleting channel from map
		mq.mu.Unlock()
		delete(mq.channels, id.String())
		mq.mu.Lock()
	}()

	log.Printf("consumer %s connected to server", conn.LocalAddr())

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

func (mq *MessageQueue) checkConnectionHeartbeat(conn *net.TCPConn, channel chan Message) {
	for {
		one := make([]byte, 1)
		_, err := conn.Write(one)

		if err != nil {
			// Write failed, connection might be dead
			close(channel)
			return
		}

		time.Sleep(time.Second * 2)
	}
}

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
	mq.cfg.MetricsHandler.AddReceived()

	// Notifying channels
	mq.NotifyConsumers(queueMsg)

	err := conn.Close()

	if err != nil {
		log.Printf("error with closing connection: %s", err)
		return
	}
}

func (mq *MessageQueue) NotifyConsumers(msg Message) {

	for id, channel := range mq.channels {
		log.Printf("notifying channel %s of message", id)
		channel <- msg
	}
}
