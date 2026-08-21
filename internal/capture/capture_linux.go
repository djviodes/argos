package capture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

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

	err = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 1, Usec: 0})
	if err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("setting socket timeout: %w", err)
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

// Run reads packets from the bound socket until ctx is cancelled or a fatal
// read error occurs, sending each successfully parsed packet on the channel
// returned by Packets. Packets that fail to parse are logged and dropped
// rather than treated as fatal. Before Run returns, the channel returned by
// Packets is closed and the underlying socket is closed.
func (c *Capture) Run(ctx context.Context) error {
	const maxFrameSize = 1518

	buf := make([]byte, maxFrameSize)
	defer close(c.packets)
	defer c.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			n, err := unix.Read(c.fd, buf)

			if err != nil && errors.Is(err, unix.EAGAIN) {
				continue
			} else if err != nil {
				return fmt.Errorf("starting capture failed: %w", err)
			}

			capturedAt := time.Now()

			p, err := parsePacket(buf[:n], capturedAt)

			if err != nil {
				slog.Warn("dropped packet", "err", err)
				continue
			}

			select {
			case c.packets <- p:
				continue
			case <-ctx.Done():
				return nil
			}
		}
	}
}
