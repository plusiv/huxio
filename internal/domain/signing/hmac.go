package signing

import (
	"crypto/sha256"
	"hash"
	"sync"
)

// sha256BlockSize is the block size HMAC pads keys to.
const sha256BlockSize = 64

// hasherPool keeps SHA-256 hashers alive across deliveries. crypto/hmac
// cannot be re-keyed, so a pool of hmac.Hash values is useless; pooling the
// underlying hashers and doing the two-pass padding here is what actually
// removes the per-signature allocations.
var hasherPool = sync.Pool{
	New: func() any { return sha256.New() },
}

// hmacSHA256 computes HMAC-SHA256 with pooled hashers. It is byte-for-byte
// identical to crypto/hmac, which the tests assert against random inputs.
func hmacSHA256(key, message []byte) []byte {
	// A key longer than the block size is replaced by its own digest.
	if len(key) > sha256BlockSize {
		digest := sha256.Sum256(key)
		key = digest[:]
	}

	var ipad, opad [sha256BlockSize]byte
	copy(ipad[:], key)
	copy(opad[:], key)
	for i := range sha256BlockSize {
		ipad[i] ^= 0x36
		opad[i] ^= 0x5c
	}

	inner, _ := hasherPool.Get().(hash.Hash)
	inner.Reset()
	inner.Write(ipad[:])
	inner.Write(message)
	innerSum := inner.Sum(nil)
	hasherPool.Put(inner)

	outer, _ := hasherPool.Get().(hash.Hash)
	outer.Reset()
	outer.Write(opad[:])
	outer.Write(innerSum)
	sum := outer.Sum(nil)
	hasherPool.Put(outer)

	return sum
}
