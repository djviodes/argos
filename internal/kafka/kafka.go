package kafka

import (
	"errors"
	"net/netip"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// FlowSource describes the fields kafka needs from a completed flow record.
// flow.FlowRecord satisfies this interface, but kafka depends only on the
// interface, never on flow's concrete type.
type FlowSource interface {
	SrcIP() netip.Addr
	DstIP() netip.Addr
	SrcPort() uint16
	DstPort() uint16
	Protocol() uint8
	ByteCount() int
	PacketCount() int
	FirstSeen() time.Time
	LastSeen() time.Time
}

// Kafka publishes flow records to a Kafka topic.
type Kafka struct {
	writer *kafkago.Writer
}

// New returns a Kafka ready to publish records to topic on the broker at
// brokerAddr.
func New(brokerAddr, topic string) (*Kafka, error) {
	if brokerAddr == "" {
		return nil, errors.New("brokerAddr must not be empty")
	}

	if topic == "" {
		return nil, errors.New("topic must not be empty")
	}

	writer := &kafkago.Writer{
		Addr:     kafkago.TCP(brokerAddr),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}

	return &Kafka{
		writer: writer,
	}, nil
}
