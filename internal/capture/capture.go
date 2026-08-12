// Package capture opens a raw socket on a network interface and reads
// incoming packets for parsing into flow-level summaries.
package capture

import (
	"encoding/binary"
)

// Capture reads raw packets from a network interface through a bound
// AF_PACKET socket.
type Capture struct {
	fd      int
	ifIndex int
	packets chan Packet
}

// Packets returns the channel Run sends successfully parsed packets to.
// The channel is closed once Run returns.
func (c *Capture) Packets() <-chan Packet { return c.packets }

// htons converts a uint16 to network byte order for use in syscall structs
// that expect it, regardless of the host's native byte order.
func htons(host uint16) uint16 {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, host)
	return binary.NativeEndian.Uint16(buf)
}
