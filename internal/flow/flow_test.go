package flow

import (
	"net"
	"net/netip"
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
	firstSeenTiemstamp := time.Now().Add(-5 * time.Second)
	secondSeenTimestamp := time.Now()
	pkt := fakePacket{
		srcIP:     net.IP{0xc0, 0xa8, 0x01, 0x0a},
		dstIP:     net.IP{0x5d, 0xb8, 0xd8, 0x22},
		srcPort:   0xd431,
		dstPort:   0x01bb,
		protocol:  0x06,
		length:    38,
		timestamp: firstSeenTiemstamp,
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

			if f.flows[flowKey].firstSeen != firstSeenTiemstamp {
				t.Errorf("got flow record first seen %v, want flow record first seen %v", f.flows[flowKey].firstSeen, firstSeenTiemstamp)
			}

			if f.flows[flowKey].lastSeen != tt.pkt.timestamp {
				t.Errorf("got flow record last seen %v, want flow record last seen %v", f.flows[flowKey].lastSeen, tt.pkt.timestamp)
			}
		})
	}
}
