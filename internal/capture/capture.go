// Package capture opens a raw socket on a network interface and reads
// incoming packets for parsing into flow-level summaries.
package capture

import (
	"encoding/binary"
	"sync"

	"golang.org/x/sys/unix"
)

// Capture reads raw packets from a network interface through a bound
// AF_PACKET socket.
type Capture struct {
	fd        int
	ifIndex   int
	packets   chan Packet
	closeOnce sync.Once
}

// Packets returns the channel Run sends successfully parsed packets to.
// The channel is closed once Run returns.
func (c *Capture) Packets() <-chan Packet { return c.packets }

// Close closes the socket New opened, releasing its file descriptor. It is
// safe to call more than once and from more than one goroutine — the close
// happens at most once — so a caller that defers Close immediately after a
// successful New does not conflict with Run closing the socket on its way out.
// The underlying close error is discarded: there is no meaningful recovery
// from failing to close a socket that is already being abandoned.
func (c *Capture) Close() {
	c.closeOnce.Do(func() { unix.Close(c.fd) })
}

// htons converts a uint16 to network byte order for use in syscall structs
// that expect it, regardless of the host's native byte order.
func htons(host uint16) uint16 {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, host)
	return binary.NativeEndian.Uint16(buf)
}
