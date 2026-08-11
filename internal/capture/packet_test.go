package capture

import (
	"net"
	"slices"
	"testing"
	"time"
)

func TestParseEthernet(t *testing.T) {
	rawBytes := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // dst MAC (unused by this test)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // src MAC (unused by this test)
		0x08, 0x00, // EtherType = IPv4
	}
	tests := []struct {
		name    string
		raw     []byte
		want    uint16
		wantErr bool
	}{
		{name: "valid", raw: rawBytes, want: 0x0800},
		{name: "truncated", raw: []byte{0x00, 0x01}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseEthernet(tt.raw)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if got != tt.want {
				t.Errorf("got %#x, want %#x", got, tt.want)
			}
		})
	}
}

func TestParseIPv4(t *testing.T) {
	rawBytes := []byte{
		0x45, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x06, // Protocol = TCP
		0x00, 0x00,
		0xc0, 0xa8, 0x01, 0x0a, // srcIP = 192.168.1.10
		0x5d, 0xb8, 0xd8, 0x22, // dstIP = 93.184.216.34
	}
	rawBytesShortHeaderLen := slices.Clone(rawBytes)
	rawBytesShortHeaderLen[0] = 0x44
	rawBytesShortArray := slices.Clone(rawBytes)
	rawBytesShortArray = slices.Delete(rawBytesShortArray, 1, 2)
	tests := []struct {
		name          string
		raw           []byte
		wantProtocol  uint8
		wantSrcIP     net.IP
		wantDstIP     net.IP
		wantHeaderLen int
		wantErr       bool
	}{
		{
			name:          "valid",
			raw:           rawBytes,
			wantProtocol:  0x06,
			wantSrcIP:     net.IP{0xc0, 0xa8, 0x01, 0x0a},
			wantDstIP:     net.IP{0x5d, 0xb8, 0xd8, 0x22},
			wantHeaderLen: 20,
		},
		{name: "truncated", raw: []byte{0x00, 0x01}, wantErr: true},
		{name: "shortHeaderLen", raw: rawBytesShortHeaderLen, wantErr: true},
		{name: "shortArrayLen", raw: rawBytesShortArray, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProtocol, gotSrcIP, gotDstIP, gotHeaderLen, err := parseIPv4(tt.raw)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if gotProtocol != tt.wantProtocol {
				t.Errorf("got protocol %#x, want protocol %#x", gotProtocol, tt.wantProtocol)
			}

			if !gotSrcIP.Equal(tt.wantSrcIP) {
				t.Errorf("got srcIP %#x, want srcIP %#x", gotSrcIP, tt.wantSrcIP)
			}

			if !gotDstIP.Equal(tt.wantDstIP) {
				t.Errorf("got dstIP %#x, want dstIP %#x", gotDstIP, tt.wantDstIP)
			}

			if gotHeaderLen != tt.wantHeaderLen {
				t.Errorf("got header length %d, want header length %d", gotHeaderLen, tt.wantHeaderLen)
			}
		})
	}
}

func TestParseTransport(t *testing.T) {
	rawBytes := []byte{
		0xd4, 0x31, // srcPort = 0xd431
		0x01, 0xbb, // dstPort = 0x01bb
	}
	rawBytesShortArray := slices.Clone(rawBytes)
	rawBytesShortArray = slices.Delete(rawBytesShortArray, 1, 2)
	tests := []struct {
		name        string
		raw         []byte
		wantSrcPort uint16
		wantDstPort uint16
		wantErr     bool
	}{
		{name: "valid", raw: rawBytes, wantSrcPort: 0xd431, wantDstPort: 0x01bb},
		{name: "truncated", raw: []byte{0x00, 0x01}, wantErr: true},
		{name: "shortArrayLen", raw: rawBytesShortArray, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSrcPort, gotDstPort, err := parseTransport(tt.raw)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if gotSrcPort != tt.wantSrcPort {
				t.Errorf("got srcPort %#x, want srcPort %#x", gotSrcPort, tt.wantSrcPort)
			}

			if gotDstPort != tt.wantDstPort {
				t.Errorf("got dstPort %#x, want dstPort %#x", gotDstPort, tt.wantDstPort)
			}
		})
	}
}

func TestParsePacket(t *testing.T) {
	rawBytes := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // dst MAC (unused by this test)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // src MAC (unused by this test)
		0x08, 0x00, // EtherType = IPv4
		0x45, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x06, // Protocol = TCP
		0x00, 0x00,
		0xc0, 0xa8, 0x01, 0x0a, // srcIP = 192.168.1.10
		0x5d, 0xb8, 0xd8, 0x22, // dstIP = 93.184.216.34
		0xd4, 0x31, // srcPort = 0xd431
		0x01, 0xbb, // dstPort = 0x01bb
	}
	currentTime := time.Now()
	validPacket := Packet{
		srcIP:     net.IP{0xc0, 0xa8, 0x01, 0x0a},
		dstIP:     net.IP{0x5d, 0xb8, 0xd8, 0x22},
		srcPort:   0xd431,
		dstPort:   0x01bb,
		protocol:  0x06,
		length:    len(rawBytes),
		timestamp: currentTime,
	}
	rawBytesBadEther := slices.Clone(rawBytes)
	rawBytesBadEther[12] = 0x86
	rawBytesBadEther[13] = 0xDD
	rawBytesBadProtocol := slices.Clone(rawBytes)
	rawBytesBadProtocol[23] = 0x17
	tests := []struct {
		name       string
		raw        []byte
		wantPacket Packet
		wantErr    bool
	}{
		{name: "valid", raw: rawBytes, wantPacket: validPacket},
		{name: "badEther", raw: rawBytesBadEther, wantErr: true},
		{name: "badProtocol", raw: rawBytesBadProtocol, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPacket, err := parsePacket(tt.raw, currentTime)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if !gotPacket.srcIP.Equal(tt.wantPacket.srcIP) {
				t.Errorf("got Packet srcIP %#x, want Packet srcIP %#x", gotPacket.srcIP, tt.wantPacket.srcIP)
			}

			if !gotPacket.dstIP.Equal(tt.wantPacket.dstIP) {
				t.Errorf("got Packet dstIP %#x, want Packet dstIP %#x", gotPacket.dstIP, tt.wantPacket.dstIP)
			}

			if gotPacket.srcPort != tt.wantPacket.srcPort {
				t.Errorf("got Packet srcPort %#x, want Packet srcPort %#x", gotPacket.srcPort, tt.wantPacket.srcPort)
			}

			if gotPacket.dstPort != tt.wantPacket.dstPort {
				t.Errorf("got Packet dstPort %#x, want Packet dstPort %#x", gotPacket.dstPort, tt.wantPacket.dstPort)
			}

			if gotPacket.protocol != tt.wantPacket.protocol {
				t.Errorf("got Packet protocol %#x, want Packet protocol %#x", gotPacket.protocol, tt.wantPacket.protocol)
			}

			if gotPacket.length != tt.wantPacket.length {
				t.Errorf("got Packet length %#x, want Packet length %#x", gotPacket.length, tt.wantPacket.length)
			}

			if !gotPacket.timestamp.Equal(tt.wantPacket.timestamp) {
				t.Errorf("got Packet timestamp %v, want Packet timestamp %v", gotPacket.timestamp, tt.wantPacket.timestamp)
			}
		})
	}
}
