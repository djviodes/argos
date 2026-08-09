package capture

import "testing"

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
