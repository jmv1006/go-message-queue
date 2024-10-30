package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	messagesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "messages_received",
		Help: "The total number of received messages",
	})

	messagesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "messages_sent",
		Help: "The total number of messages sent from the server",
	})

	activeChannels = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "active_channels",
		Help: "The total number of active channels/connections",
	})

	topics = promauto.NewCounter(prometheus.CounterOpts{
		Name: "topics",
		Help: "The total number of topics created",
	})
)

type Handler struct {
	Received       prometheus.Counter
	Sent           prometheus.Counter
	ActiveChannels prometheus.Gauge
	Topics         prometheus.Counter
}

func NewMetricsHandler() *Handler {
	return &Handler{Received: messagesReceived, Sent: messagesSent, ActiveChannels: activeChannels, Topics: topics}
}

func (m *Handler) AddSent() {
	m.Sent.Inc()
}

func (m *Handler) AddReceived() {
	m.Received.Inc()
}

func (m *Handler) AddChannel() {
	m.ActiveChannels.Inc()
}

func (m *Handler) RemoveChannel() {
	m.ActiveChannels.Dec()
}

func (m *Handler) AddTopic() {
	m.Topics.Inc()
}
