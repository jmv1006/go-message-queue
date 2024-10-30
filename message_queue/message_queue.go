package mesage_queue

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/jmv1006/go-message-queue/metrics"
)

type MessageQueueConfig struct {
	Address        string
	Protocol       string
	Wg             *sync.WaitGroup
	MetricsHandler *metrics.Handler
	Debug          bool
}

type MessageQueue struct {
	cfg    MessageQueueConfig
	topics map[string]Topic
	mu     sync.Mutex
}

type Message struct {
	Timestamp string `json:"timestamp" binding:"required"`
	Payload   string `json:"payload" binding:"required"`
}

type StandardRequest struct {
	Type  string `json:"type"`
	Body  string `json:"body"`
	Topic string `json:"topic"`
}

type Topic struct {
	name     string
	channels map[string]chan string
}

func New(config MessageQueueConfig) *MessageQueue {
	return &MessageQueue{cfg: config, topics: make(map[string]Topic)}
}

func (mq *MessageQueue) Start() {
	defer mq.cfg.Wg.Done()

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

		if mq.cfg.Debug {
			log.Printf("%s has connected to the server", conn.LocalAddr())
		}

		// handle this connection on another thread
		go mq.handleConnection(conn)

	}

}

func (mq *MessageQueue) handleConnection(connPtr *net.TCPConn) {
	initialBuffer := make([]byte, 1000)

	for {
		buff := bytes.NewBuffer(initialBuffer)

		written, err := connPtr.Read(buff.Bytes())
		if err != nil {
			if err != io.EOF {
				if mq.cfg.Debug {
					fmt.Println("read error:", err)
				}
			}
			break
		}

		writtenBytes := buff.Bytes()[:written]

		req := Decode(writtenBytes)

		if req == nil {
			continue
		} else if req.Topic == "" {
			continue
		}

		if req.Type == "PRODUCE" {
			mq.ProduceMessage(req, connPtr)
		} else if req.Type == "CONSUME" {
			// This will become a consuming connection
			mq.CreateConsumerStream(req, connPtr)
		}
	}

}

func (mq *MessageQueue) checkConnectionHeartbeat(conn *net.TCPConn, channel chan string) {
	for {
		one := make([]byte, 1)
		_, err := conn.Write(one)

		if err != nil {
			// Write failed, connection might be dead
			close(channel)
			return
		}

		time.Sleep(time.Second * 5)
	}
}

// ValidateTopic checks if a topic exists & creates it if it does not
func (mq *MessageQueue) ValidateTopic(name string) Topic {
	mq.mu.Lock()

	topic, exists := mq.topics[name]

	if !exists {
		// create this topic, it does not exist
		topic = Topic{
			name:     name,
			channels: make(map[string]chan string),
		}

		mq.topics[name] = topic

		// metrics
		mq.cfg.MetricsHandler.AddTopic()
	}

	mq.mu.Unlock()

	return topic
}

func (mq *MessageQueue) GetMutex() *sync.Mutex {
	return &mq.mu
}
