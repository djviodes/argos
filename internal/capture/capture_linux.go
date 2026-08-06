package capture

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

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
