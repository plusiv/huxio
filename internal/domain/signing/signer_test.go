package signing_test

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/signing"
)

// The vector below is the shape every Standard Webhooks receiver library
// verifies: base64(hmac_sha256(secret, "{id}.{unix}.{payload}")), tagged v1.
const (
	vectorMsgID   = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	vectorSecret  = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	vectorPayload = `{"test":2432232314}`
)

var vectorTimestamp = time.Unix(1614265330, 0)

func TestSignedPayloadFormat(t *testing.T) {
	t.Parallel()

	got := string(signing.SignedPayload(vectorMsgID, vectorTimestamp, []byte(vectorPayload)))
	want := vectorMsgID + "." + strconv.FormatInt(vectorTimestamp.Unix(), 10) + "." + vectorPayload
	if got != want {
		t.Errorf("SignedPayload = %q, want %q", got, want)
	}
}

// TestSignMatchesReferenceVerification reproduces exactly what a receiver
// library does, so a change to the signing code that a receiver would reject
// fails here.
func TestSignMatchesReferenceVerification(t *testing.T) {
	t.Parallel()

	signer := signing.NewSigner(signing.SchemeStandard)
	value, err := signer.Sign(vectorMsgID, vectorTimestamp, []byte(vectorPayload), []signing.Key{
		{Secret: []byte(vectorSecret), Type: entities.SecretTypeHMAC256},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// A receiver strips the whsec_ prefix, base64-decodes nothing, and signs
	// the same string with crypto/hmac.
	secret := strings.TrimPrefix(vectorSecret, "whsec_")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(vectorMsgID + "." + strconv.FormatInt(vectorTimestamp.Unix(), 10) + "." + vectorPayload))
	want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))

	if value != want {
		t.Errorf("signature = %q, want %q", value, want)
	}
}

func TestHMACPoolMatchesCryptoHMAC(t *testing.T) {
	t.Parallel()

	signer := signing.NewSigner(signing.SchemeStandard)

	// Random keys and payloads across the interesting length boundaries: the
	// pooled implementation must agree with crypto/hmac everywhere, including
	// keys longer than the SHA-256 block size.
	for _, keyLen := range []int{1, 16, 32, 63, 64, 65, 200} {
		for _, payloadLen := range []int{0, 1, 64, 1000} {
			key := make([]byte, keyLen)
			payload := make([]byte, payloadLen)
			if _, err := rand.Read(key); err != nil {
				t.Fatalf("rand: %v", err)
			}
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand: %v", err)
			}

			got, err := signer.Sign("msg_1", vectorTimestamp, payload, []signing.Key{
				{Secret: key, Type: entities.SecretTypeHMAC256},
			})
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}

			mac := hmac.New(sha256.New, key)
			mac.Write(signing.SignedPayload("msg_1", vectorTimestamp, payload))
			want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))

			if got != want {
				t.Fatalf("key %d bytes, payload %d bytes: signature mismatch\n got %q\nwant %q",
					keyLen, payloadLen, got, want)
			}
		}
	}
}

func TestSignEmitsOneSignaturePerKey(t *testing.T) {
	t.Parallel()

	signer := signing.NewSigner(signing.SchemeStandard)
	value, err := signer.Sign(vectorMsgID, vectorTimestamp, []byte(vectorPayload), []signing.Key{
		{Secret: []byte("whsec_current"), Type: entities.SecretTypeHMAC256},
		{Secret: []byte("whsec_previous"), Type: entities.SecretTypeHMAC256},
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// Space-separated, so a receiver holding either key verifies. This is
	// what makes secret rotation need no downtime.
	parts := strings.Split(value, " ")
	if len(parts) != 2 {
		t.Fatalf("signature = %q, want two space-separated signatures", value)
	}
	for _, part := range parts {
		if !strings.HasPrefix(part, "v1,") {
			t.Errorf("signature %q is missing the v1 tag", part)
		}
	}
	if parts[0] == parts[1] {
		t.Error("two different keys must produce two different signatures")
	}
}

func TestSignEd25519(t *testing.T) {
	t.Parallel()

	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	signer := signing.NewSigner(signing.SchemeStandard)
	for _, key := range []signing.Key{
		{Secret: []byte(base64.StdEncoding.EncodeToString(private)), Type: entities.SecretTypeEd25519},
		{Secret: []byte(base64.StdEncoding.EncodeToString(private.Seed())), Type: entities.SecretTypeEd25519},
	} {
		value, err := signer.Sign(vectorMsgID, vectorTimestamp, []byte(vectorPayload), []signing.Key{key})
		if err != nil {
			t.Fatalf("Sign: %v", err)
		}
		// Asymmetric signatures carry the v1a tag, not v1.
		encoded, ok := strings.CutPrefix(value, "v1a,")
		if !ok {
			t.Fatalf("signature = %q, want the v1a tag", value)
		}
		signature, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("decode signature: %v", err)
		}
		if !ed25519.Verify(public, signing.SignedPayload(vectorMsgID, vectorTimestamp, []byte(vectorPayload)), signature) {
			t.Error("a receiver holding the public key must verify the signature")
		}
	}
}

func TestSignRejectsBadInput(t *testing.T) {
	t.Parallel()

	signer := signing.NewSigner(signing.SchemeStandard)

	if _, err := signer.Sign(vectorMsgID, vectorTimestamp, nil, nil); err == nil {
		t.Error("signing with no keys must fail")
	}
	if _, err := signer.Sign(vectorMsgID, vectorTimestamp, nil, []signing.Key{
		{Secret: []byte("x"), Type: entities.SecretType("rsa")},
	}); err == nil {
		t.Error("an unsupported algorithm must fail")
	}
	if _, err := signer.Sign(vectorMsgID, vectorTimestamp, nil, []signing.Key{
		{Secret: []byte("too-short"), Type: entities.SecretTypeEd25519},
	}); err == nil {
		t.Error("a malformed ed25519 key must fail")
	}
}

func TestHeaderSchemes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		scheme   signing.HeaderScheme
		wantID   string
		wantTime string
		wantSig  string
	}{
		{"standard", signing.SchemeStandard, "webhook-id", "webhook-timestamp", "webhook-signature"},
		{"huxio compatibility", signing.SchemeHuxio, "huxio-id", "huxio-timestamp", "huxio-signature"},
		{"unknown falls back to standard", signing.HeaderScheme("nonsense"), "webhook-id", "webhook-timestamp", "webhook-signature"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			signer := signing.NewSigner(tc.scheme)
			headers, err := signer.Headers(vectorMsgID, vectorTimestamp, []byte(vectorPayload), []signing.Key{
				{Secret: []byte(vectorSecret), Type: entities.SecretTypeHMAC256},
			})
			if err != nil {
				t.Fatalf("Headers: %v", err)
			}
			if headers[tc.wantID] != vectorMsgID {
				t.Errorf("%s = %q", tc.wantID, headers[tc.wantID])
			}
			if headers[tc.wantTime] != strconv.FormatInt(vectorTimestamp.Unix(), 10) {
				t.Errorf("%s = %q", tc.wantTime, headers[tc.wantTime])
			}
			if !strings.HasPrefix(headers[tc.wantSig], "v1,") {
				t.Errorf("%s = %q", tc.wantSig, headers[tc.wantSig])
			}
			if len(headers) != 3 {
				t.Errorf("headers = %v, want exactly the three signature headers", headers)
			}
		})
	}
}

func BenchmarkSignHMAC256(b *testing.B) {
	signer := signing.NewSigner(signing.SchemeStandard)
	keys := []signing.Key{{Secret: []byte(vectorSecret), Type: entities.SecretTypeHMAC256}}
	payload := []byte(strings.Repeat(`{"k":"v"},`, 100))

	b.ReportAllocs()
	for b.Loop() {
		if _, err := signer.Sign(vectorMsgID, vectorTimestamp, payload, keys); err != nil {
			b.Fatal(err)
		}
	}
}
