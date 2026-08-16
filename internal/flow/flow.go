// Package flow aggregates captured packets into flow records, grouped by
// source/destination IP and port, tracking byte counts and first/last-seen
// timestamps, and flushes completed records for publishing downstream.
package flow

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"time"
)

// PacketSource describes the fields flow needs from a captured packet.
// capture.Packet satisfies this interface, but flow depends only on the
// interface, never on capture's concrete type.
type PacketSource interface {
	SrcIP() net.IP
	DstIP() net.IP
	SrcPort() uint16
	DstPort() uint16
	Protocol() uint8
	Len() int
	Timestamp() time.Time
}

// FlowKey identifies a flow by its 5-tuple: source and destination address,
// source and destination port, and protocol. It is comparable, so it can be
// used as a map key.
type FlowKey struct {
	srcIP    netip.Addr
	dstIP    netip.Addr
	srcPort  uint16
	dstPort  uint16
	protocol uint8
}

// FlowRecord aggregates the packets seen for a single flow: total bytes and
// packet count, and the first and last time a packet was observed.
type FlowRecord struct {
	FlowKey
	byteCount   int
	packetCount int
	firstSeen   time.Time
	lastSeen    time.Time
}

// SrcIP returns the flow's source IP address.
func (fr FlowRecord) SrcIP() netip.Addr { return fr.FlowKey.srcIP }

// DstIP returns the flow's destination IP address.
func (fr FlowRecord) DstIP() netip.Addr { return fr.FlowKey.dstIP }

// SrcPort returns the flow's source port.
func (fr FlowRecord) SrcPort() uint16 { return fr.FlowKey.srcPort }

// DstPort returns the flow's destination port.
func (fr FlowRecord) DstPort() uint16 { return fr.FlowKey.dstPort }

// Protocol returns the flow's IP protocol number (e.g. 6 for TCP, 17 for UDP).
func (fr FlowRecord) Protocol() uint8 { return fr.FlowKey.protocol }

// ByteCount returns the total number of bytes observed across all packets in
// the flow.
func (fr FlowRecord) ByteCount() int { return fr.byteCount }

// PacketCount returns the total number of packets observed in the flow.
func (fr FlowRecord) PacketCount() int { return fr.packetCount }

// FirstSeen returns the time the flow's first packet was observed.
func (fr FlowRecord) FirstSeen() time.Time { return fr.firstSeen }

// LastSeen returns the time the flow's most recently observed packet arrived.
func (fr FlowRecord) LastSeen() time.Time { return fr.lastSeen }

// Flow aggregates packets into FlowRecords, keyed by FlowKey, and flushes
// them to the channel returned by Flushed once a flow has been idle past the
// timeout or Run is shutting down.
type Flow struct {
	flows   map[FlowKey]*FlowRecord
	flushed chan FlowRecord
}

// New returns a Flow ready to aggregate packets.
func New() *Flow {
	return &Flow{
		flows:   make(map[FlowKey]*FlowRecord),
		flushed: make(chan FlowRecord),
	}
}

// Flushed returns the channel completed flow records are sent to. A record
// is sent either when its flow has been idle past the timeout or when Run is
// shutting down. The channel is closed once Run returns.
func (f *Flow) Flushed() <-chan FlowRecord { return f.flushed }

// Run consumes packets from source, aggregating them into flow records via
// add. Records for flows that have been idle past the timeout are flushed
// periodically. When ctx is cancelled or source is closed, Run attempts to
// flush every remaining record before returning, but won't wait indefinitely
// for an unresponsive downstream consumer — a record can be left unflushed
// if it isn't received within a short, bounded window. The channel returned
// by Flushed is closed once Run returns.
func (f *Flow) Run(ctx context.Context, source <-chan PacketSource) error {
	const tickInterval = 1 * time.Second
	const drainTimeout = 5 * time.Second
	ticker := time.NewTicker(tickInterval)

	defer ticker.Stop()
	defer close(f.flushed)

	for {
		select {
		case <-ctx.Done():
			drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
			defer cancel()
			f.flushAll(drainCtx)

			return nil
		case pkt, ok := <-source:
			if !ok {
				drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
				defer cancel()
				f.flushAll(drainCtx)

				return nil
			}

			if err := f.add(pkt); err != nil {
				slog.Warn("adding packet to flow", "err", err)
				continue
			}
		case <-ticker.C:
			f.flushIdle(ctx)
		}
	}
}

func (f *Flow) add(pkt PacketSource) error {
	key, err := newFlowKey(pkt)

	if err != nil {
		return fmt.Errorf("creating flow key: %w", err)
	}

	record, exists := f.flows[key]

	if !exists {
		record = &FlowRecord{
			FlowKey:     key,
			byteCount:   pkt.Len(),
			packetCount: 1,
			firstSeen:   pkt.Timestamp(),
			lastSeen:    pkt.Timestamp(),
		}

		f.flows[key] = record
	} else {
		record.byteCount += pkt.Len()
		record.packetCount++
		record.lastSeen = pkt.Timestamp()
	}

	return nil
}

func (f *Flow) flushAll(ctx context.Context) {
	for _, flow := range f.flows {
		select {
		case f.flushed <- *flow:
			delete(f.flows, flow.FlowKey)
		case <-ctx.Done():
			return
		}
	}
}

func (f *Flow) flushIdle(ctx context.Context) {
	const idleTimeout = 60 * time.Second

	for _, flow := range f.flows {
		if time.Since(flow.lastSeen) <= idleTimeout {
			continue
		}

		select {
		case f.flushed <- *flow:
			delete(f.flows, flow.FlowKey)
		case <-ctx.Done():
			return
		}
	}
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
