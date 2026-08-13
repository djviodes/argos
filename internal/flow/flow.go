package flow

import (
	"fmt"
	"net"
	"net/netip"
	"time"
)

type PacketSource interface {
	SrcIP() net.IP
	DstIP() net.IP
	SrcPort() uint16
	DstPort() uint16
	Protocol() uint8
	Len() int
	Timestamp() time.Time
}

type FlowKey struct {
	srcIP    netip.Addr
	dstIP    netip.Addr
	srcPort  uint16
	dstPort  uint16
	protocol uint8
}

type FlowRecord struct {
	FlowKey
	byteCount   int
	packetCount int
	firstSeen   time.Time
	lastSeen    time.Time
}

type Flow struct {
	records map[FlowKey]*FlowRecord
}

func newFlowKey(pkt PacketSource) (FlowKey, error) {
	srcAddr, ok := netip.AddrFromSlice(pkt.SrcIP())
	if !ok {
		return FlowKey{}, fmt.Errorf("converting source IP %v to netip.Addr: invalid length", pkt.SrcIP())
	}

	dstAddr, ok := netip.AddrFromSlice(pkt.DstIP())
	if !ok {
		return FlowKey{}, fmt.Errorf("converting destination IP %v to netip.Addr: invalid length", pkt.DstIP())
	}

	return FlowKey{
		srcIP:    srcAddr,
		dstIP:    dstAddr,
		srcPort:  pkt.SrcPort(),
		dstPort:  pkt.DstPort(),
		protocol: pkt.Protocol(),
	}, nil
}
