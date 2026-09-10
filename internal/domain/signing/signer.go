// Package signing implements the Standard Webhooks signature format. It is
// pure: no I/O, no framework, no clock of its own. Match this byte for byte or
// every receiver verification library breaks.
package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/rotisserie/eris"
)

// HeaderScheme selects the header naming used on the wire.
type HeaderScheme string

const (
	// SchemeStandard emits webhook-id / webhook-timestamp / webhook-signature.
	SchemeStandard HeaderScheme = "standard"
	// SchemeHuxio emits huxio-prefixed names instead. Receivers written against
	// the incumbent look for those, so supporting both means a migrating
	// tenant changes one config value rather than redeploying every receiver.
	SchemeHuxio HeaderScheme = "huxio"
)

// Header names for both schemes.
const (
	HeaderStandardID        = "webhook-id"
	HeaderStandardTimestamp = "webhook-timestamp"
	HeaderStandardSignature = "webhook-signature"

	HeaderHuxioID        = "huxio-id"
	HeaderHuxioTimestamp = "huxio-timestamp"
	HeaderHuxioSignature = "huxio-signature"
)

// Signature version tags. v1 is symmetric, v1a is asymmetric.
const (
	VersionHMAC256 = "v1"
	VersionEd25519 = "v1a"
)

// SecretPrefix is stripped from a secret before use: receivers are given the
// prefixed form, but the bytes that are signed with are the ones after it.
const SecretPrefix = "whsec_"

// Key is one signing key in the form the signer consumes.
type Key struct {
	// Secret is the plaintext key material, already decrypted.
	Secret []byte
	Type   entities.SecretType
}

// Signer produces Standard Webhooks signatures.
type Signer struct {
	scheme HeaderScheme
}

// NewSigner builds a signer for a header scheme. An unknown scheme falls back
// to the standard names.
func NewSigner(scheme HeaderScheme) *Signer {
	if scheme != SchemeHuxio {
		scheme = SchemeStandard
	}
	return &Signer{scheme: scheme}
}

// SignedPayload returns the exact bytes that are signed:
// "{msg_id}.{timestamp}.{payload}", with the timestamp in unix seconds.
func SignedPayload(msgID string, timestamp time.Time, payload []byte) []byte {
	unix := strconv.FormatInt(timestamp.Unix(), 10)

	signed := make([]byte, 0, len(msgID)+1+len(unix)+1+len(payload))
	signed = append(signed, msgID...)
	signed = append(signed, '.')
	signed = append(signed, unix...)
	signed = append(signed, '.')
	return append(signed, payload...)
}

// Sign returns the value of the signature header: one signature per key,
// space-separated, so a receiver verifies against whichever key it holds and
// rotation needs no downtime.
func (s *Signer) Sign(msgID string, timestamp time.Time, payload []byte, keys []Key) (string, error) {
	if len(keys) == 0 {
		return "", eris.New("signing: no keys supplied")
	}

	signed := SignedPayload(msgID, timestamp, payload)
	signatures := make([]string, 0, len(keys))

	for _, key := range keys {
		signature, err := signOne(signed, key)
		if err != nil {
			return "", err
		}
		signatures = append(signatures, signature)
	}
	return strings.Join(signatures, " "), nil
}

// Headers returns every header a signed request carries, keyed by the name to
// send. Custom endpoint headers are applied by the caller afterwards and may
// never override these.
func (s *Signer) Headers(msgID string, timestamp time.Time, payload []byte, keys []Key) (map[string]string, error) {
	signature, err := s.Sign(msgID, timestamp, payload, keys)
	if err != nil {
		return nil, err
	}

	idHeader, timestampHeader, signatureHeader := s.HeaderNames()
	return map[string]string{
		idHeader:        msgID,
		timestampHeader: strconv.FormatInt(timestamp.Unix(), 10),
		signatureHeader: signature,
	}, nil
}

// HeaderNames returns the id, timestamp and signature header names for the
// configured scheme.
func (s *Signer) HeaderNames() (id, timestamp, signature string) {
	if s.scheme == SchemeHuxio {
		return HeaderHuxioID, HeaderHuxioTimestamp, HeaderHuxioSignature
	}
	return HeaderStandardID, HeaderStandardTimestamp, HeaderStandardSignature
}

// Scheme reports the configured header scheme.
func (s *Signer) Scheme() HeaderScheme { return s.scheme }

func signOne(signed []byte, key Key) (string, error) {
	secret := normalizeSecret(key.Secret)

	switch key.Type {
	case entities.SecretTypeEd25519:
		private, err := ed25519PrivateKey(secret)
		if err != nil {
			return "", err
		}
		return VersionEd25519 + "," + base64.StdEncoding.EncodeToString(ed25519.Sign(private, signed)), nil

	case entities.SecretTypeHMAC256, "":
		return VersionHMAC256 + "," + base64.StdEncoding.EncodeToString(hmacSHA256(secret, signed)), nil

	default:
		return "", eris.Errorf("signing: unsupported secret type %q", key.Type)
	}
}

// normalizeSecret strips the receiver-facing prefix, so the signature is over
// the key material itself.
func normalizeSecret(secret []byte) []byte {
	if strings.HasPrefix(string(secret), SecretPrefix) {
		return secret[len(SecretPrefix):]
	}
	return secret
}

func ed25519PrivateKey(secret []byte) (ed25519.PrivateKey, error) {
	// An ed25519 secret is stored base64-encoded, as either a 64-byte private
	// key or a 32-byte seed.
	decoded, err := base64.StdEncoding.DecodeString(string(secret))
	if err != nil {
		decoded = secret
	}
	switch len(decoded) {
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(decoded), nil
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(decoded), nil
	default:
		return nil, eris.Errorf("signing: ed25519 key must be %d or %d bytes, got %d",
			ed25519.SeedSize, ed25519.PrivateKeySize, len(decoded))
	}
}
