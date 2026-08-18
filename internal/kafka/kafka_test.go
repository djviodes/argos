package kafka

import (
	"context"
	"net/netip"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
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

type fakeMessageWriter struct {
	err      error
	messages []kafkago.Message
	closed   bool
}

func (m *fakeMessageWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	if m.err != nil {
		return m.err
	}

	m.messages = append(m.messages, msgs...)

	return nil
}

func (m *fakeMessageWriter) Close() error {
	m.closed = true

	return nil
}

func newFakeFlowSource(t *testing.T, srcIP netip.Addr) fakeFlowSource {
	t.Helper()

	flow := fakeFlowSource{
		srcIP:       srcIP,
		dstIP:       netip.AddrFrom4([4]byte{0x5d, 0xb8, 0xd8, 0x22}),
		srcPort:     0xd431,
		dstPort:     0x01bb,
		protocol:    0x06,
		byteCount:   1500,
		packetCount: 1,
		firstSeen:   time.Now().Add(-5 * time.Second),
		lastSeen:    time.Now(),
	}

	return flow
}

func TestNew(t *testing.T) {
	tests := []struct {
		name       string
		brokerAddr string
		topic      string
		wantErr    bool
	}{
		{name: "valid", brokerAddr: "localhost:9092", topic: "some-topic"},
		{name: "emptyBrokerAddr", topic: "some-topic", wantErr: true},
		{name: "emptyTopic", brokerAddr: "localhost:9092", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k, err := New(tt.brokerAddr, tt.topic)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if k == nil {
				t.Error("k returned nil when it should not have")
			}
		})
	}
}

func TestPublish(t *testing.T) {

}

func TestRunSourceCloses(t *testing.T) {

}

func TestRunPublishFailure(t *testing.T) {

}

func TestRunDrainsPendingRecord(t *testing.T) {

}

func TestRunDrainTimeout(t *testing.T) {

}
