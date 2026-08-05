// Package capture opens a raw socket on a network interface and reads
// incoming packets for parsing into flow-level summaries.
package capture

import (
	"encoding/binary"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// Capture reads raw packets from a network interface through a bound
// AF_PACKET socket.
type Capture struct {
	fd      int
	ifIndex int
	packets chan Packet
}

// New opens a raw socket bound to the network interface named by iface and
// returns a Capture ready to read packets from it. It returns an error if
// the interface cannot be resolved, the socket cannot be opened, or the
// socket cannot be bound to the interface.
func New(iface string) (*Capture, error) {
	ifi, err := net.InterfaceByName(iface)
	if err != nil {
		return nil, fmt.Errorf("resolving interface %q: %w", iface, err)
	}

	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW, 0)
	if err != nil {
		return nil, fmt.Errorf("opening raw socket: %w", err)
	}

	addr := &unix.SockaddrLinklayer{
		Protocol: htons(unix.ETH_P_ALL),
		Ifindex:  ifi.Index,
	}
	if err := unix.Bind(fd, addr); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("binding to interface %q: %w", iface, err)
	}

	return &Capture{
		fd:      fd,
		ifIndex: ifi.Index,
		packets: make(chan Packet),
	}, nil
}

// htons converts a uint16 to network byte order for use in syscall structs
// that expect it, regardless of the host's native byte order.
func htons(host uint16) uint16 {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, host)
	return binary.NativeEndian.Uint16(buf)
}
