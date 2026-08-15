package flow

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

type fakePacket struct {
	srcIP     net.IP
	dstIP     net.IP
	srcPort   uint16
	dstPort   uint16
	protocol  uint8
	length    int
	timestamp time.Time
}

func (p fakePacket) SrcIP() net.IP        { return p.srcIP }
func (p fakePacket) DstIP() net.IP        { return p.dstIP }
func (p fakePacket) SrcPort() uint16      { return p.srcPort }
func (p fakePacket) DstPort() uint16      { return p.dstPort }
func (p fakePacket) Protocol() uint8      { return p.protocol }
func (p fakePacket) Len() int             { return p.length }
func (p fakePacket) Timestamp() time.Time { return p.timestamp }

func newFakePacketAndKey(t *testing.T, srcIP net.IP) (fakePacket, FlowKey) {
	t.Helper()

	pkt := fakePacket{
		srcIP:     srcIP,
		dstIP:     net.IP{0x5d, 0xb8, 0xd8, 0x22},
		srcPort:   0xd431,
		dstPort:   0x01bb,
		protocol:  0x06,
		length:    38,
		timestamp: time.Now(),
	}

	flowKey, err := newFlowKey(pkt)

	if err != nil {
		t.Fatalf("error when creating flow key: %s", err)
	}

	return pkt, flowKey
}

func TestNewFlowKey(t *testing.T) {
	pkt, validFlowKey := newFakePacketAndKey(t, net.IP{0xc0, 0xa8, 0x01, 0x0a})

	invalidSrcIPPkt := pkt
	invalidSrcIPPkt.srcIP = net.IP{0xd4}
	invalidDstIPPkt := pkt
	invalidDstIPPkt.dstIP = net.IP{0x01}

	tests := []struct {
		name        string
		pkt         fakePacket
		wantFlowKey FlowKey
		wantErr     bool
	}{
		{name: "valid", pkt: pkt, wantFlowKey: validFlowKey},
		{name: "invalidSrcIP", pkt: invalidSrcIPPkt, wantErr: true},
		{name: "invalidDstIP", pkt: invalidDstIPPkt, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNewFlowKey, err := newFlowKey(tt.pkt)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if gotNewFlowKey.srcIP != tt.wantFlowKey.srcIP {
				t.Errorf("got new flow srcIP %s, want new flow srcIP %s", gotNewFlowKey.srcIP, tt.wantFlowKey.srcIP)
			}

			if gotNewFlowKey.dstIP != tt.wantFlowKey.dstIP {
				t.Errorf("got new flow dstIP %s, want new flow dstIP %s", gotNewFlowKey.dstIP, tt.wantFlowKey.dstIP)
			}

			if gotNewFlowKey.srcPort != tt.wantFlowKey.srcPort {
				t.Errorf("got new flow srcPort %#x, want new flow srcPort %#x", gotNewFlowKey.srcPort, tt.wantFlowKey.srcPort)
			}

			if gotNewFlowKey.dstPort != tt.wantFlowKey.dstPort {
				t.Errorf("got new flow dstPort %#x, want new flow dstPort %#x", gotNewFlowKey.dstPort, tt.wantFlowKey.dstPort)
			}

			if gotNewFlowKey.protocol != tt.wantFlowKey.protocol {
				t.Errorf("got new flow protocol %#x, want new flow protocol %#x", gotNewFlowKey.protocol, tt.wantFlowKey.protocol)
			}
		})
	}
}

func TestAdd(t *testing.T) {
	f := New()

	firstSeenTimestamp := time.Now().Add(-5 * time.Second)
	secondSeenTimestamp := time.Now()

	pkt, flowKey := newFakePacketAndKey(t, net.IP{0xc0, 0xa8, 0x01, 0x0a})
	pkt.timestamp = firstSeenTimestamp

	secondPkt := pkt
	secondPkt.timestamp = secondSeenTimestamp

	invalidIPPkt := pkt
	invalidIPPkt.srcIP = net.IP{0xd4}
	invalidIPPkt.dstIP = net.IP{0x01}

	tests := []struct {
		name            string
		pkt             fakePacket
		wantByteCount   int
		wantPacketCount int
		wantErr         bool
	}{
		{name: "validFirstPacket", pkt: pkt, wantByteCount: 38, wantPacketCount: 1},
		{name: "validSecondPacket", pkt: secondPkt, wantByteCount: 76, wantPacketCount: 2},
		{name: "invalidIP", pkt: invalidIPPkt, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := f.add(tt.pkt)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if f.flows[flowKey].byteCount != tt.wantByteCount {
				t.Errorf("got flow record byte count %#x, want flow record byte count %#x", f.flows[flowKey].byteCount, tt.wantByteCount)
			}

			if f.flows[flowKey].packetCount != tt.wantPacketCount {
				t.Errorf("got flow record packet count %#x, want flow record packet count %#x", f.flows[flowKey].packetCount, tt.wantPacketCount)
			}

			if f.flows[flowKey].firstSeen != firstSeenTimestamp {
				t.Errorf("got flow record first seen %v, want flow record first seen %v", f.flows[flowKey].firstSeen, firstSeenTimestamp)
			}

			if f.flows[flowKey].lastSeen != tt.pkt.timestamp {
				t.Errorf("got flow record last seen %v, want flow record last seen %v", f.flows[flowKey].lastSeen, tt.pkt.timestamp)
			}
		})
	}
}

func TestFlushAll(t *testing.T) {
	ctx := context.Background()
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	wantFlows := make(map[FlowKey]*FlowRecord)
	cancelledCtxWantFlows := make(map[FlowKey]*FlowRecord)

	_, firstFlowKey := newFakePacketAndKey(t, net.IP{0xc0, 0xa8, 0x01, 0x0a})
	_, secondFlowKey := newFakePacketAndKey(t, net.IP{0x0a, 0x00, 0x00, 0x05})

	firstSeenTimestamp := time.Now().Add(-5 * time.Second)
	secondSeenTimestamp := time.Now()

	firstFlowRecord := FlowRecord{
		FlowKey:     firstFlowKey,
		byteCount:   38,
		packetCount: 1,
		firstSeen:   firstSeenTimestamp,
		lastSeen:    firstSeenTimestamp,
	}
	secondFlowRecord := FlowRecord{
		FlowKey:     secondFlowKey,
		byteCount:   38,
		packetCount: 1,
		firstSeen:   secondSeenTimestamp,
		lastSeen:    secondSeenTimestamp,
	}

	wantFlows[firstFlowKey] = &firstFlowRecord
	wantFlows[secondFlowKey] = &secondFlowRecord

	tests := []struct {
		name               string
		ctx                context.Context
		wantFlows          map[FlowKey]*FlowRecord
		wantRemainingFlows int
	}{
		{name: "valid", ctx: ctx, wantFlows: wantFlows, wantRemainingFlows: 0},
		{name: "cancelledCtx", ctx: cancelledCtx, wantFlows: cancelledCtxWantFlows, wantRemainingFlows: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := New()
			f.flows[firstFlowKey] = &firstFlowRecord
			f.flows[secondFlowKey] = &secondFlowRecord

			recordSlice := []FlowRecord{}
			correctFlowsFlushed := make(map[FlowKey]bool)
			for key := range tt.wantFlows {
				correctFlowsFlushed[key] = false
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				f.flushAll(tt.ctx)
			}()

			for i := 0; i < len(tt.wantFlows); i++ {
				select {
				case record := <-f.Flushed():
					recordSlice = append(recordSlice, record)
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for flushed record")
				}
			}

			wg.Wait()

			for _, record := range recordSlice {
				if tt.wantFlows[record.FlowKey] == nil {
					t.Error("FlowKey does not exist in expected flows")
					continue
				}

				if record.byteCount != tt.wantFlows[record.FlowKey].byteCount {
					t.Errorf("got flow record byte count %#x, want flow record byte count %#x", record.byteCount, tt.wantFlows[record.FlowKey].byteCount)
				}

				if record.packetCount != tt.wantFlows[record.FlowKey].packetCount {
					t.Errorf("got flow record packet count %#x, want flow record packet count %#x", record.packetCount, tt.wantFlows[record.FlowKey].packetCount)
				}

				if record.firstSeen != tt.wantFlows[record.FlowKey].firstSeen {
					t.Errorf("got flow record first seen %v, want flow record first seen %v", record.firstSeen, tt.wantFlows[record.FlowKey].firstSeen)
				}

				if record.lastSeen != tt.wantFlows[record.FlowKey].lastSeen {
					t.Errorf("got flow record last seen %v, want flow record last seen %v", record.lastSeen, tt.wantFlows[record.FlowKey].lastSeen)
				}

				if _, ok := tt.wantFlows[record.FlowKey]; ok {
					correctFlowsFlushed[record.FlowKey] = true
				}
			}

			for _, foundFlowKey := range correctFlowsFlushed {
				if foundFlowKey == false {
					t.Errorf("flow key never got flushed")
				}
			}

			if len(f.flows) != tt.wantRemainingFlows {
				t.Errorf("got flow length %#x, want flow length %#x", len(f.flows), tt.wantRemainingFlows)
			}
		})
	}
}

func TestFlushIdle(t *testing.T) {
	ctx := context.Background()
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	wantFlows := make(map[FlowKey]*FlowRecord)
	cancelledCtxWantFlows := make(map[FlowKey]*FlowRecord)

	_, firstFlowKey := newFakePacketAndKey(t, net.IP{0xc0, 0xa8, 0x01, 0x0a})
	_, secondFlowKey := newFakePacketAndKey(t, net.IP{0x0a, 0x00, 0x00, 0x05})

	firstSeenTimestamp := time.Now().Add(-65 * time.Second)
	secondSeenTimestamp := time.Now()

	firstFlowRecord := FlowRecord{
		FlowKey:     firstFlowKey,
		byteCount:   38,
		packetCount: 1,
		firstSeen:   firstSeenTimestamp,
		lastSeen:    firstSeenTimestamp,
	}
	secondFlowRecord := FlowRecord{
		FlowKey:     secondFlowKey,
		byteCount:   38,
		packetCount: 1,
		firstSeen:   secondSeenTimestamp,
		lastSeen:    secondSeenTimestamp,
	}

	wantFlows[firstFlowKey] = &firstFlowRecord

	tests := []struct {
		name               string
		ctx                context.Context
		wantFlows          map[FlowKey]*FlowRecord
		wantRemainingFlows int
	}{
		{name: "valid", ctx: ctx, wantFlows: wantFlows, wantRemainingFlows: 1},
		{name: "cancelledCtx", ctx: cancelledCtx, wantFlows: cancelledCtxWantFlows, wantRemainingFlows: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := New()
			f.flows[firstFlowKey] = &firstFlowRecord
			f.flows[secondFlowKey] = &secondFlowRecord

			recordSlice := []FlowRecord{}
			correctFlowsFlushed := make(map[FlowKey]bool)
			for key := range tt.wantFlows {
				correctFlowsFlushed[key] = false
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				f.flushIdle(tt.ctx)
			}()

			for i := 0; i < len(tt.wantFlows); i++ {
				select {
				case record := <-f.Flushed():
					recordSlice = append(recordSlice, record)
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for flushed record")
				}
			}

			wg.Wait()

			for _, record := range recordSlice {
				if tt.wantFlows[record.FlowKey] == nil {
					t.Error("FlowKey does not exist in expected flows")
					continue
				}

				if record.byteCount != tt.wantFlows[record.FlowKey].byteCount {
					t.Errorf("got flow record byte count %#x, want flow record byte count %#x", record.byteCount, tt.wantFlows[record.FlowKey].byteCount)
				}

				if record.packetCount != tt.wantFlows[record.FlowKey].packetCount {
					t.Errorf("got flow record packet count %#x, want flow record packet count %#x", record.packetCount, tt.wantFlows[record.FlowKey].packetCount)
				}

				if record.firstSeen != tt.wantFlows[record.FlowKey].firstSeen {
					t.Errorf("got flow record first seen %v, want flow record first seen %v", record.firstSeen, tt.wantFlows[record.FlowKey].firstSeen)
				}

				if record.lastSeen != tt.wantFlows[record.FlowKey].lastSeen {
					t.Errorf("got flow record last seen %v, want flow record last seen %v", record.lastSeen, tt.wantFlows[record.FlowKey].lastSeen)
				}

				if _, ok := tt.wantFlows[record.FlowKey]; ok {
					correctFlowsFlushed[record.FlowKey] = true
				}
			}

			for _, foundFlowKey := range correctFlowsFlushed {
				if foundFlowKey == false {
					t.Errorf("flow key never got flushed")
				}
			}

			if len(f.flows) != tt.wantRemainingFlows {
				t.Errorf("got flow length %#x, want flow length %#x", len(f.flows), tt.wantRemainingFlows)
			}
		})
	}
}

func TestRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	pkt, flowKey := newFakePacketAndKey(t, net.IP{0xc0, 0xa8, 0x01, 0x0a})

	source := make(chan PacketSource)

	tests := []struct {
		name               string
		ctx                context.Context
		ctxCancel          context.CancelFunc
		source             chan PacketSource
		wantRemainingFlows int
	}{
		{name: "cancelledCtx", ctx: ctx, ctxCancel: cancel, source: source, wantRemainingFlows: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := New()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				f.Run(tt.ctx, tt.source)
			}()

			tt.source <- pkt
			tt.ctxCancel()
			record := <-f.Flushed()

			wg.Wait()

			if record.FlowKey != flowKey {
				t.Error("flow keys don't match")
			}

			if record.byteCount != pkt.length {
				t.Error("byte counts don't match")
			}

			if record.packetCount != 1 {
				t.Error("packet counts don't match")
			}

			if record.firstSeen != pkt.Timestamp() {
				t.Error("first seens don't match")
			}

			if record.lastSeen != pkt.Timestamp() {
				t.Error("last seens don't match")
			}

			if len(f.flows) != tt.wantRemainingFlows {
				t.Error("flushAll did not flush add flows correctly")
			}

			if _, ok := <-f.Flushed(); !ok {
				t.Error("flushed did not close correctly")
			}
		})
	}
}
