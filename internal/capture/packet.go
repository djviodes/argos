package capture

import (
	"net"
	"time"
)

type Packet struct {
	srcIP     net.IP
	dstIP     net.IP
	srcPort   uint16
	dstPort   uint16
	protocol  uint8
	length    int
	timestamp time.Time
}
