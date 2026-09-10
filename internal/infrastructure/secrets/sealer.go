// Package secrets seals and opens endpoint signing secrets at rest with
// XChaCha20-Poly1305, using a key list so operators can rotate: seal with the
// first key, try every key on open.
package secrets

import (
	"crypto/rand"
	"encoding/base64"
	"strings"

	"github.com/rotisserie/eris"
	"golang.org/x/crypto/chacha20poly1305"
)

// ErrNoKeys is returned when a Sealer is built without any usable key.
var ErrNoKeys = eris.New("secrets: no encryption keys configured")

// ErrOpen is returned when no configured key can open a ciphertext.
var ErrOpen = eris.New("secrets: unable to open sealed value with any configured key")

// Sealer performs authenticated encryption of secret material.
type Sealer struct {
	keys [][]byte
}

// NewSealer builds a Sealer from base64 (standard or raw URL) encoded 32-byte
// keys. The first key is the sealing key; the rest are accepted on open.
func NewSealer(encodedKeys []string) (*Sealer, error) {
	keys := make([][]byte, 0, len(encodedKeys))
	for _, encoded := range encodedKeys {
		encoded = strings.TrimSpace(encoded)
		if encoded == "" {
			continue
		}
		key, err := decodeKey(encoded)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, ErrNoKeys
	}
	return &Sealer{keys: keys}, nil
}

// GenerateKey returns a fresh base64-encoded key suitable for configuration.
func GenerateKey() (string, error) {
	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := rand.Read(key); err != nil {
		return "", eris.Wrap(err, "generate encryption key")
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

// Seal encrypts plaintext with the primary key. The nonce is prepended to the
// returned ciphertext.
func (s *Sealer) Seal(plaintext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(s.keys[0])
	if err != nil {
		return nil, eris.Wrap(err, "build aead")
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, eris.Wrap(err, "generate nonce")
	}
	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Open decrypts a value sealed by any configured key.
func (s *Sealer) Open(sealed []byte) ([]byte, error) {
	for _, key := range s.keys {
		aead, err := chacha20poly1305.NewX(key)
		if err != nil {
			return nil, eris.Wrap(err, "build aead")
		}
		if len(sealed) < aead.NonceSize() {
			return nil, ErrOpen
		}
		nonce, ciphertext := sealed[:aead.NonceSize()], sealed[aead.NonceSize():]
		plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
		if err == nil {
			return plaintext, nil
		}
	}
	return nil, ErrOpen
}

func decodeKey(encoded string) ([]byte, error) {
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if key, err := enc.DecodeString(encoded); err == nil {
			if len(key) != chacha20poly1305.KeySize {
				return nil, eris.Errorf("secrets: encryption key must be %d bytes, got %d", chacha20poly1305.KeySize, len(key))
			}
			return key, nil
		}
	}
	return nil, eris.New("secrets: encryption key is not valid base64")
}
