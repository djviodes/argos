package capture

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// Packet holds the fields parsed from a captured Ethernet frame carrying an
// IPv4 TCP or UDP payload.
type Packet struct {
	srcIP     net.IP
	dstIP     net.IP
	srcPort   uint16
	dstPort   uint16
	protocol  uint8
	length    int
	timestamp time.Time
}

// SrcIP returns the packet's source IP address.
func (p Packet) SrcIP() net.IP { return p.srcIP }

// DstIP returns the packet's destination IP address.
func (p Packet) DstIP() net.IP { return p.dstIP }

// SrcPort returns the packet's source port.
func (p Packet) SrcPort() uint16 { return p.srcPort }

// DstPort returns the packet's destination port.
func (p Packet) DstPort() uint16 { return p.dstPort }

// Protocol returns the packet's IP protocol number (e.g. 6 for TCP, 17 for UDP).
func (p Packet) Protocol() uint8 { return p.protocol }

// Len returns the total length of the captured packet in bytes.
func (p Packet) Len() int { return p.length }

// Timestamp returns the time the packet was captured.
func (p Packet) Timestamp() time.Time { return p.timestamp }

func parsePacket(raw []byte, capturedAt time.Time) (p Packet, err error) {
	const ipv4EtherType = 0x0800
	const protocolTCP = 6
	const protocolUDP = 17

	etherType, err := parseEthernet(raw)
	if err != nil {
		return
	}

	if etherType != ipv4EtherType {
		err = fmt.Errorf("ethernet type %#x out of scope", etherType)
		return
	}

	protocol, srcIP, dstIP, headerLen, err := parseIPv4(raw[14:])
	if err != nil {
		return
	}

	if protocol != protocolTCP && protocol != protocolUDP {
		err = fmt.Errorf("protocol %d out of scope", protocol)
		return
	}

	srcPort, dstPort, err := parseTransport(raw[14+headerLen:])
	if err != nil {
		return
	}

	p = Packet{
		srcIP:     srcIP,
		dstIP:     dstIP,
		srcPort:   srcPort,
		dstPort:   dstPort,
		protocol:  protocol,
		length:    len(raw),
		timestamp: capturedAt,
	}
	return
}

func parseEthernet(raw []byte) (etherType uint16, err error) {
	if len(raw) < 14 {
		err = fmt.Errorf("ethernet header truncated: got %d bytes, want at least 14", len(raw))
		return
	}

	etherType = binary.BigEndian.Uint16(raw[12:14])
	return
}

func parseIPv4(raw []byte) (protocol uint8, srcIP, dstIP net.IP, headerLen int, err error) {
	if len(raw) < 20 {
		err = fmt.Errorf("IPv4 header truncated: got %d bytes, want at least 20", len(raw))
		return
	}

	// headerLen can exceed 20 if IP options are present.
	headerLen = int(raw[0]&0x0F) * 4
	if headerLen < 20 {
		err = fmt.Errorf("invalid IPv4 header length: %d bytes (minimum is 20)", headerLen)
		return
	}

	if len(raw) < headerLen {
		err = fmt.Errorf("header truncated: got %d bytes, want at least %d", len(raw), headerLen)
		return
	}

	protocol = raw[9]
	srcIP = make(net.IP, 4)
	dstIP = make(net.IP, 4)

	copy(srcIP, raw[12:16])
	copy(dstIP, raw[16:20])

	return
}

func parseTransport(raw []byte) (srcPort, dstPort uint16, err error) {
	if len(raw) < 4 {
		err = fmt.Errorf("transport header truncated: got %d bytes, want at least 4", len(raw))
		return
	}

	srcPort = binary.BigEndian.Uint16(raw[0:2])
	dstPort = binary.BigEndian.Uint16(raw[2:4])

	return
}
