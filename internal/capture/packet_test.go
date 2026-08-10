package capture

import (
	"net"
	"slices"
	"testing"
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
