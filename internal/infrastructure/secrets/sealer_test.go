package secrets_test

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/plusiv/huxio/internal/infrastructure/secrets"
)

func newKey(t *testing.T) string {
	t.Helper()
	key, err := secrets.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return key
}

func TestSealOpenRoundTrip(t *testing.T) {
	t.Parallel()

	sealer, err := secrets.NewSealer([]string{newKey(t)})
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}

	want := []byte("whsec_topsecret")
	sealed, err := sealer.Seal(want)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Contains(sealed, want) {
		t.Fatal("sealed value must not contain plaintext")
	}
	got, err := sealer.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Open = %q, want %q", got, want)
	}
}

func TestOpenAcceptsRotatedKey(t *testing.T) {
	t.Parallel()

	oldKey, newKeyEncoded := newKey(t), newKey(t)

	oldSealer, err := secrets.NewSealer([]string{oldKey})
	if err != nil {
		t.Fatalf("NewSealer(old): %v", err)
	}
	sealed, err := oldSealer.Seal([]byte("rotate me"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// New primary key first, previous key retained for reads.
	rotated, err := secrets.NewSealer([]string{newKeyEncoded, oldKey})
	if err != nil {
		t.Fatalf("NewSealer(rotated): %v", err)
	}
	got, err := rotated.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(got) != "rotate me" {
		t.Errorf("Open = %q", got)
	}
}

func TestOpenFailsWithUnknownKey(t *testing.T) {
	t.Parallel()

	sealerA, _ := secrets.NewSealer([]string{newKey(t)})
	sealerB, _ := secrets.NewSealer([]string{newKey(t)})

	sealed, err := sealerA.Seal([]byte("secret"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := sealerB.Open(sealed); err == nil {
		t.Fatal("expected open to fail with an unrelated key")
	}
}

func TestNewSealerValidatesKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		keys []string
	}{
		{"empty list", nil},
		{"blank entries", []string{"", "   "}},
		{"not base64", []string{"not-base64-!!"}},
		{"wrong length", []string{base64.StdEncoding.EncodeToString([]byte("short"))}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := secrets.NewSealer(tc.keys); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
