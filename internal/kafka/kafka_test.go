package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

var errFakeWrite = errors.New("writing messages failed")

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
	tests := []struct {
		name     string
		writeErr error
		wantErr  bool
	}{
		{name: "valid", writeErr: nil},
		{name: "writeMessagesFails", writeErr: errFakeWrite, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeWriter := &fakeMessageWriter{err: tt.writeErr}
			k := &Kafka{writer: fakeWriter}
			flow := newFakeFlowSource(t, netip.AddrFrom4([4]byte{0xc0, 0xa8, 0x01, 0x0a}))
			err := k.publish(context.Background(), flow)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr && !errors.Is(err, errFakeWrite) {
				t.Errorf("err %v matches errFakeWrite %v", err, errFakeWrite)
				return
			}

			if tt.wantErr {
				return
			}

			if len(fakeWriter.messages) != 1 {
				t.Error("expected message length to be 1")
			}

			var msg flowMessage

			err = json.Unmarshal(fakeWriter.messages[0].Value, &msg)
			if err != nil {
				t.Fatalf("error unmarshalling json: %v", err)
			}

			if flow.SrcIP() != msg.SrcIP {
				t.Errorf("got message src IP %s, want message src IP %s", msg.SrcIP, flow.SrcIP())
			}

			if flow.DstIP() != msg.DstIP {
				t.Errorf("got message dst IP %s, want message dst IP %s", msg.DstIP, flow.DstIP())
			}

			if flow.SrcPort() != msg.SrcPort {
				t.Errorf("got message src Port %#x, want message src Port %#x", msg.SrcPort, flow.SrcPort())
			}

			if flow.DstPort() != msg.DstPort {
				t.Errorf("got message dst Port %#x, want message dst Port %#x", msg.DstPort, flow.DstPort())
			}

			if flow.Protocol() != msg.Protocol {
				t.Errorf("got message protocol %#x, want message protocol %#x", msg.Protocol, flow.Protocol())
			}

			if flow.ByteCount() != msg.ByteCount {
				t.Errorf("got message byte count %d, want message byte count %d", msg.ByteCount, flow.ByteCount())
			}

			if flow.PacketCount() != msg.PacketCount {
				t.Errorf("got message packet count %d, want message packet count %d", msg.PacketCount, flow.PacketCount())
			}

			if !flow.FirstSeen().Equal(msg.FirstSeen) {
				t.Errorf("got message first seen %v, want message first seen %v", msg.FirstSeen, flow.FirstSeen())
			}

			if !flow.LastSeen().Equal(msg.LastSeen) {
				t.Errorf("got message last seen %v, want message last seen %v", msg.LastSeen, flow.LastSeen())
			}

			msgKey := fmt.Sprintf("srcIP:%s-dstIP:%s-srcPort:%d-dstPort:%d-protocol:%d",
				msg.SrcIP, msg.DstIP, msg.SrcPort, msg.DstPort, msg.Protocol)

			if msgKey != string(fakeWriter.messages[0].Key) {
				t.Errorf("got message key %s, want message key %s", msgKey, fakeWriter.messages[0].Key)
			}
		})
	}
}

func TestRunSourceCloses(t *testing.T) {
	tests := []struct {
		name             string
		sendFlow         bool
		wantMessageCount int
	}{
		{name: "noRecordsSent", sendFlow: false, wantMessageCount: 0},
		{name: "oneRecordSent", sendFlow: true, wantMessageCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := make(chan FlowSource)
			fakeWriter := &fakeMessageWriter{}
			k := &Kafka{writer: fakeWriter}

			errCh := make(chan error, 1)

			go func() {
				errCh <- k.Run(context.Background(), source)
			}()

			if tt.sendFlow {
				source <- newFakeFlowSource(t, netip.AddrFrom4([4]byte{0xc0, 0xa8, 0x01, 0x0a}))
			}

			close(source)

			select {
			case err := <-errCh:
				if err != nil {
					t.Errorf("err returned: %v", err)
				}
			case <-time.After(1 * time.Second):
				t.Fatal("Run did not return")
			}

			if len(fakeWriter.messages) != tt.wantMessageCount {
				t.Errorf("got message count %d, want message count %d", len(fakeWriter.messages), tt.wantMessageCount)
			}

			if !fakeWriter.closed {
				t.Error("message writer failed to close properly")
			}
		})
	}
}

func TestRunPublishFailure(t *testing.T) {
	t.Run("duringNormalOperation", func(t *testing.T) {
		source := make(chan FlowSource)
		fakeWriter := &fakeMessageWriter{err: errFakeWrite}
		k := &Kafka{writer: fakeWriter}

		errCh := make(chan error, 1)

		go func() {
			errCh <- k.Run(context.Background(), source)
		}()

		source <- newFakeFlowSource(t, netip.AddrFrom4([4]byte{0xc0, 0xa8, 0x01, 0x0a}))

		select {
		case err := <-errCh:
			if !errors.Is(err, errFakeWrite) {
				t.Errorf("got error %v, want error %v", err, errFakeWrite)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Run did not return")
		}

		if !fakeWriter.closed {
			t.Error("expected fakeWriter to be closed")
		}
	})
}

func TestRunDrainsPendingRecord(t *testing.T) {

}

func TestRunDrainTimeout(t *testing.T) {

}
