// Package payload compresses and decompresses message payloads. Payload bytes
// are never unmarshalled: they are validated once at ingest, stored
// compressed, and written to the socket as-is after decompression.
package payload

import (
	"github.com/klauspost/compress/zstd"
	"github.com/rotisserie/eris"
)

// Encoding is the one-byte header recording how the stored bytes were written.
type Encoding byte

const (
	// EncodingRaw means the bytes after the header are the payload verbatim.
	// Small payloads skip compression, where the framing costs more than it
	// saves.
	EncodingRaw Encoding = 0
	// EncodingZstd means the bytes after the header are zstd-compressed. JSON
	// compresses 5-10x, which shrinks the page cache footprint proportionally.
	EncodingZstd Encoding = 1
)

// DefaultMinCompressBytes is the size below which compression is skipped.
const DefaultMinCompressBytes = 512

// Codec compresses and decompresses payloads. It is safe for concurrent use:
// the underlying zstd encoder and decoder are both goroutine-safe, which is
// why one Codec is shared per process rather than allocated per delivery.
type Codec struct {
	encoder          *zstd.Encoder
	decoder          *zstd.Decoder
	minCompressBytes int
}

// NewCodec builds a codec. A non-positive threshold uses the default.
func NewCodec(minCompressBytes int) (*Codec, error) {
	if minCompressBytes <= 0 {
		minCompressBytes = DefaultMinCompressBytes
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, eris.Wrap(err, "build zstd encoder")
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return nil, eris.Wrap(err, "build zstd decoder")
	}
	return &Codec{encoder: encoder, decoder: decoder, minCompressBytes: minCompressBytes}, nil
}

// Encode returns the storable representation of raw: a one-byte encoding
// header followed by the payload.
func (c *Codec) Encode(raw []byte) []byte {
	if len(raw) < c.minCompressBytes {
		out := make([]byte, 0, len(raw)+1)
		out = append(out, byte(EncodingRaw))
		return append(out, raw...)
	}
	out := make([]byte, 1, len(raw)/2+64)
	out[0] = byte(EncodingZstd)
	return c.encoder.EncodeAll(raw, out)
}

// Decode returns the original payload bytes for a stored value.
func (c *Codec) Decode(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return nil, eris.New("payload: stored value is empty")
	}
	switch Encoding(stored[0]) {
	case EncodingRaw:
		return stored[1:], nil
	case EncodingZstd:
		decoded, err := c.decoder.DecodeAll(stored[1:], nil)
		if err != nil {
			return nil, eris.Wrap(err, "decompress payload")
		}
		return decoded, nil
	default:
		return nil, eris.Errorf("payload: unknown encoding %d", stored[0])
	}
}

// Close releases the codec's resources.
func (c *Codec) Close() {
	_ = c.encoder.Close()
	c.decoder.Close()
}
