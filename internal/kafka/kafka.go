// Package kafka publishes completed flow records to a Kafka topic,
// serializing each record to JSON before publishing.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// Kafka publishes flow records to a Kafka topic.
type Kafka struct {
	writer messageWriter
}

type flowMessage struct {
	SrcIP       netip.Addr
	DstIP       netip.Addr
	SrcPort     uint16
	DstPort     uint16
	Protocol    uint8
	ByteCount   int
	PacketCount int
	FirstSeen   time.Time
	LastSeen    time.Time
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
		Balancer: &kafkago.Hash{},
	}

	return &Kafka{
		writer: writer,
	}, nil
}

// Run consumes flow records from source, serializing each to JSON and
// publishing it to the configured Kafka topic. When ctx is cancelled, Run
// attempts to keep publishing any remaining records within a bounded window
// before returning. Run also returns if source closes or a publish fails,
// closing the underlying writer in every case.
func (k *Kafka) Run(ctx context.Context, source <-chan FlowSource) error {
	const drainTimeout = 5 * time.Second

	defer k.writer.Close()

	for {
		select {
		case <-ctx.Done():
			drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
			defer cancel()

			for {
				select {
				case flow, ok := <-source:
					if !ok {
						return nil
					}

					if err := k.publish(drainCtx, flow); err != nil {
						return err
					}
				case <-drainCtx.Done():
					return nil
				}
			}
		case flow, ok := <-source:
			if !ok {
				return nil
			}

			if err := k.publish(ctx, flow); err != nil {
				return err
			}
		}
	}
}

func (k *Kafka) publish(ctx context.Context, flow FlowSource) error {
	key := fmt.Sprintf("srcIP:%s-dstIP:%s-srcPort:%d-dstPort:%d-protocol:%d",
		flow.SrcIP(), flow.DstIP(), flow.SrcPort(), flow.DstPort(), flow.Protocol())

	msg := flowMessage{
		SrcIP:       flow.SrcIP(),
		DstIP:       flow.DstIP(),
		SrcPort:     flow.SrcPort(),
		DstPort:     flow.DstPort(),
		Protocol:    flow.Protocol(),
		ByteCount:   flow.ByteCount(),
		PacketCount: flow.PacketCount(),
		FirstSeen:   flow.FirstSeen(),
		LastSeen:    flow.LastSeen(),
	}

	value, err := json.Marshal(msg)

	if err != nil {
		return fmt.Errorf("serializing kafka message: %w", err)
	}

	err = k.writer.WriteMessages(ctx,
		kafkago.Message{
			Key:   []byte(key),
			Value: value,
			Time:  flow.LastSeen(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}

	return nil
}
