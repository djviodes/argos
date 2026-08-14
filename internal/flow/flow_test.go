package flow

import (
	"context"
	"net"
	"net/netip"
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

func TestNewFlowKey(t *testing.T) {
	pkt := fakePacket{
		srcIP:     net.IP{0xc0, 0xa8, 0x01, 0x0a},
		dstIP:     net.IP{0x5d, 0xb8, 0xd8, 0x22},
		srcPort:   0xd431,
		dstPort:   0x01bb,
		protocol:  0x06,
		length:    38,
		timestamp: time.Now(),
	}
	invalidSrcIPPkt := pkt
	invalidSrcIPPkt.srcIP = net.IP{0xd4}
	invalidDstIPPkt := pkt
	invalidDstIPPkt.dstIP = net.IP{0x01}
	validSrcAddr, _ := netip.AddrFromSlice(net.IP{0xc0, 0xa8, 0x01, 0x0a})
	validDstAddr, _ := netip.AddrFromSlice(net.IP{0x5d, 0xb8, 0xd8, 0x22})
	validFlowKey := FlowKey{
		srcIP:    validSrcAddr,
		dstIP:    validDstAddr,
		srcPort:  0xd431,
		dstPort:  0x01bb,
		protocol: 0x06,
	}
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
	pkt := fakePacket{
		srcIP:     net.IP{0xc0, 0xa8, 0x01, 0x0a},
		dstIP:     net.IP{0x5d, 0xb8, 0xd8, 0x22},
		srcPort:   0xd431,
		dstPort:   0x01bb,
		protocol:  0x06,
		length:    38,
		timestamp: firstSeenTimestamp,
	}
	secondPkt := pkt
	secondPkt.timestamp = secondSeenTimestamp
	invalidIPPkt := pkt
	invalidIPPkt.srcIP = net.IP{0xd4}
	invalidIPPkt.dstIP = net.IP{0x01}
	srcAddr, _ := netip.AddrFromSlice(pkt.srcIP)
	dstAddr, _ := netip.AddrFromSlice(pkt.dstIP)
	flowKey := FlowKey{
		srcIP:    srcAddr,
		dstIP:    dstAddr,
		srcPort:  pkt.srcPort,
		dstPort:  pkt.dstPort,
		protocol: pkt.protocol,
	}
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
	f := New()
	wantFlows := make(map[FlowKey]*FlowRecord)
	firstSrcAddr, _ := netip.AddrFromSlice(net.IP{0xc0, 0xa8, 0x01, 0x0a})
	secondSrcAddr, _ := netip.AddrFromSlice(net.IP{0x0a, 0x00, 0x00, 0x05})
	firstDstAddr, _ := netip.AddrFromSlice(net.IP{0x5d, 0xb8, 0xd8, 0x22})
	secondDstAddr, _ := netip.AddrFromSlice(net.IP{0x5d, 0xb8, 0xd8, 0x22})
	firstFlowKey := FlowKey{
		srcIP:    firstSrcAddr,
		dstIP:    firstDstAddr,
		srcPort:  0xd431,
		dstPort:  0x01bb,
		protocol: 0x06,
	}
	secondFlowKey := FlowKey{
		srcIP:    secondSrcAddr,
		dstIP:    secondDstAddr,
		srcPort:  0xd431,
		dstPort:  0x01bb,
		protocol: 0x06,
	}
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

	f.flows[firstFlowKey] = &firstFlowRecord
	f.flows[secondFlowKey] = &secondFlowRecord
	wantFlows[firstFlowKey] = &firstFlowRecord
	wantFlows[secondFlowKey] = &secondFlowRecord

	tests := []struct {
		name      string
		ctx       context.Context
		wantFlows map[FlowKey]*FlowRecord
	}{
		{name: "valid", ctx: ctx, wantFlows: wantFlows},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recordSlice := []FlowRecord{}
			correctFlowsFlushed := map[FlowKey]bool{
				firstFlowKey:  false,
				secondFlowKey: false,
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

			if len(f.flows) != 0 {
				t.Errorf("expected length of flows to be 0, got %#x", len(f.flows))
			}
		})
	}
}

func TestFlushAllCancelledCtx(t *testing.T) {
	f := New()
	srcAddr, _ := netip.AddrFromSlice(net.IP{0xc0, 0xa8, 0x01, 0x0a})
	dstAddr, _ := netip.AddrFromSlice(net.IP{0x5d, 0xb8, 0xd8, 0x22})
	flowKey := FlowKey{
		srcIP:    srcAddr,
		dstIP:    dstAddr,
		srcPort:  0xd431,
		dstPort:  0x01bb,
		protocol: 0x06,
	}
	seen := time.Now()
	flowRecord := FlowRecord{
		FlowKey:     flowKey,
		byteCount:   38,
		packetCount: 1,
		firstSeen:   seen,
		lastSeen:    seen,
	}

	f.flows[flowKey] = &flowRecord

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f.flushAll(ctx)

	if len(f.flows) != 1 {
		t.Errorf("expected length of flows to be 1, got %#x", len(f.flows))
	}
}
