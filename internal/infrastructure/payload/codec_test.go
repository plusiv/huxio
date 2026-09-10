package payload_test

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/plusiv/huxio/internal/infrastructure/payload"
)

func newCodec(t *testing.T, threshold int) *payload.Codec {
	t.Helper()
	codec, err := payload.NewCodec(threshold)
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	t.Cleanup(codec.Close)
	return codec
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	codec := newCodec(t, 32)

	tests := []struct {
		name         string
		raw          string
		wantEncoding payload.Encoding
	}{
		{"below threshold stays raw", `{"a":1}`, payload.EncodingRaw},
		{"empty stays raw", "", payload.EncodingRaw},
		{"above threshold compresses", `{"items":[` + strings.Repeat(`"invoice.paid",`, 50) + `"end"]}`, payload.EncodingZstd},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			stored := codec.Encode([]byte(tc.raw))
			if payload.Encoding(stored[0]) != tc.wantEncoding {
				t.Errorf("encoding = %d, want %d", stored[0], tc.wantEncoding)
			}
			decoded, err := codec.Decode(stored)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !bytes.Equal(decoded, []byte(tc.raw)) {
				t.Errorf("Decode = %q, want %q", decoded, tc.raw)
			}
		})
	}
}

func TestCompressionShrinksRepetitiveJSON(t *testing.T) {
	t.Parallel()

	codec := newCodec(t, 0)
	raw := []byte(`{"events":[` + strings.Repeat(`{"type":"invoice.paid","amount":100},`, 200) + `{"type":"end"}]}`)

	stored := codec.Encode(raw)
	if len(stored) >= len(raw)/2 {
		t.Errorf("compressed %d bytes to %d; JSON should compress several-fold", len(raw), len(stored))
	}
}

func TestDecodeRejectsBadInput(t *testing.T) {
	t.Parallel()

	codec := newCodec(t, 32)

	if _, err := codec.Decode(nil); err == nil {
		t.Error("expected an error for an empty stored value")
	}
	if _, err := codec.Decode([]byte{9, 1, 2}); err == nil {
		t.Error("expected an error for an unknown encoding header")
	}
	if _, err := codec.Decode([]byte{byte(payload.EncodingZstd), 0, 1, 2}); err == nil {
		t.Error("expected an error for corrupt compressed bytes")
	}
}

func TestCodecIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	codec := newCodec(t, 16)
	raw := []byte(strings.Repeat(`{"k":"v"},`, 100))

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				decoded, err := codec.Decode(codec.Encode(raw))
				if err != nil {
					t.Errorf("Decode: %v", err)
					return
				}
				if !bytes.Equal(decoded, raw) {
					t.Error("payload corrupted under concurrent use")
					return
				}
			}
		}()
	}
	wg.Wait()
}
