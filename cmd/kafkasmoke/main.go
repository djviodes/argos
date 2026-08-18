// Command kafkasmoke manually verifies that the kafka package can publish a
// flow record to a real Kafka broker.
//
// Before running, start the broker and create the topic:
//
//	docker compose up -d
//	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
//		--topic some-topic --bootstrap-server localhost:9092
//
// Then run this program and confirm the record landed on the topic:
//
//	go run ./cmd/kafkasmoke
//	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
//		--topic some-topic --bootstrap-server localhost:9092 --from-beginning
package main

import (
	"context"
	"log"
	"net/netip"
	"sync"
	"time"

	"github.com/djviodes/argos/internal/kafka"
)

type fakeFlowSource struct {
	srcIP       netip.Addr
	dstIP       netip.Addr
	srcPort     uint16
	dstPort     uint16
	protocol    uint8
	byteCount   int
	packetCount int
	firstSeen   time.Time
	lastSeen    time.Time
}

func (f fakeFlowSource) SrcIP() netip.Addr    { return f.srcIP }
func (f fakeFlowSource) DstIP() netip.Addr    { return f.dstIP }
func (f fakeFlowSource) SrcPort() uint16      { return f.srcPort }
func (f fakeFlowSource) DstPort() uint16      { return f.dstPort }
func (f fakeFlowSource) Protocol() uint8      { return f.protocol }
func (f fakeFlowSource) ByteCount() int       { return f.byteCount }
func (f fakeFlowSource) PacketCount() int     { return f.packetCount }
func (f fakeFlowSource) FirstSeen() time.Time { return f.firstSeen }
func (f fakeFlowSource) LastSeen() time.Time  { return f.lastSeen }

func main() {
	ctx := context.Background()

	flow := fakeFlowSource{
		srcIP:       netip.AddrFrom4([4]byte{0xc0, 0xa8, 0x01, 0x0a}),
		dstIP:       netip.AddrFrom4([4]byte{0x5d, 0xb8, 0xd8, 0x22}),
		srcPort:     0xd431,
		dstPort:     0x01bb,
		protocol:    0x06,
		byteCount:   1500,
		packetCount: 1,
		firstSeen:   time.Now().Add(-5 * time.Second),
		lastSeen:    time.Now(),
	}

	broker := "localhost:9092"
	topic := "some-topic"

	kafkaWriter, err := kafka.New(broker, topic)
	if err != nil {
		log.Fatalf("creating new kafka writer: %v", err)
	}

	source := make(chan kafka.FlowSource)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := kafkaWriter.Run(ctx, source)
		if err != nil {
			log.Fatalf("starting kafka writer run: %v", err)
		}
	}()

	source <- flow
	close(source)

	wg.Wait()

	log.Printf("kafkasmoke: published 1 flow record to %q via %s", topic, broker)
}
