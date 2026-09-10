package auth_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/infrastructure/auth"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/rotisserie/eris"
)

var (
	testOrgID = ids.New(ids.PrefixOrganization)
	testAppID = ids.New(ids.PrefixApplication)
)

func hmacManager(t *testing.T) *auth.Manager {
	t.Helper()
	manager, err := auth.NewManager(auth.Config{Algorithm: "HS256", Secret: "test-secret", Issuer: "huxio"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return manager
}

func TestIssueAndParseOrgToken(t *testing.T) {
	t.Parallel()

	manager := hmacManager(t)
	token, expiry, err := manager.IssueOrgToken(testOrgID, time.Hour)
	if err != nil {
		t.Fatalf("IssueOrgToken: %v", err)
	}
	if time.Until(expiry) > time.Hour+time.Minute {
		t.Errorf("expiry = %s, want roughly one hour out", expiry)
	}

	claims, err := manager.Parse(token)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if claims.Subject != testOrgID {
		t.Errorf("Subject = %q", claims.Subject)
	}
	if claims.Scope != "" {
		t.Errorf("Scope = %q, want empty on an org token", claims.Scope)
	}
}

func TestIssueAndParsePortalToken(t *testing.T) {
	t.Parallel()

	manager := hmacManager(t)
	token, _, err := manager.IssuePortalToken(testAppID, testOrgID, 8*time.Hour)
	if err != nil {
		t.Fatalf("IssuePortalToken: %v", err)
	}

	claims, err := manager.Parse(token)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if claims.Subject != testAppID || claims.Scope != auth.ScopePortal || claims.OrgID != testOrgID {
		t.Errorf("claims = %+v", claims)
	}
}

func TestIssueRejectsWrongSubjectPrefix(t *testing.T) {
	t.Parallel()

	manager := hmacManager(t)
	if _, _, err := manager.IssueOrgToken(testAppID, time.Hour); err == nil {
		t.Error("an org token must not be issued for an application id")
	}
	if _, _, err := manager.IssuePortalToken(testOrgID, testOrgID, time.Hour); err == nil {
		t.Error("a portal token must not be issued for an organization id")
	}
}

func TestParseRejectsBadTokens(t *testing.T) {
	t.Parallel()

	manager := hmacManager(t)
	other, err := auth.NewManager(auth.Config{Algorithm: "HS256", Secret: "different-secret", Issuer: "huxio"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	wrongIssuer, err := auth.NewManager(auth.Config{Algorithm: "HS256", Secret: "test-secret", Issuer: "someone-else"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	valid, _, err := manager.IssueOrgToken(testOrgID, time.Hour)
	if err != nil {
		t.Fatalf("IssueOrgToken: %v", err)
	}
	// Built by hand: IssueOrgToken deliberately refuses to mint an already
	// expired token, applying a default TTL instead.
	expired, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   testOrgID,
		Issuer:    "huxio",
		IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
	}).SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("build expired token: %v", err)
	}
	notYetValid, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   testOrgID,
		Issuer:    "huxio",
		NotBefore: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(2 * time.Hour)),
	}).SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("build not-yet-valid token: %v", err)
	}
	noSubject, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Issuer:    "huxio",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}).SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("build subjectless token: %v", err)
	}
	portalOnOrgSubject, err := jwt.NewWithClaims(jwt.SigningMethodHS256, auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   testOrgID,
			Issuer:    "huxio",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		Scope: auth.ScopePortal,
	}).SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("build mis-scoped token: %v", err)
	}
	foreign, _, err := other.IssueOrgToken(testOrgID, time.Hour)
	if err != nil {
		t.Fatalf("IssueOrgToken(foreign): %v", err)
	}
	unsigned, err := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.RegisteredClaims{
		Subject: testOrgID, Issuer: "huxio",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("build unsigned token: %v", err)
	}

	tests := []struct {
		name    string
		manager *auth.Manager
		token   string
	}{
		{"garbage", manager, "not-a-token"},
		{"empty", manager, ""},
		{"expired", manager, expired},
		{"not yet valid", manager, notYetValid},
		{"no subject", manager, noSubject},
		{"portal scope on an organization subject", manager, portalOnOrgSubject},
		{"signed with another key", manager, foreign},
		{"alg none", manager, unsigned},
		{"wrong issuer", wrongIssuer, valid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := tc.manager.Parse(tc.token)
			var appErr *apperrors.AppError
			if !eris.As(err, &appErr) || appErr.Kind != apperrors.KindUnauthorized {
				t.Fatalf("Parse = %v, want an unauthorized AppError", err)
			}
			// The reason must never leak: every failure looks the same.
			if appErr.Message == "" {
				t.Error("expected a client-safe message")
			}
		})
	}
}

func TestNewManagerRejectsBadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  auth.Config
	}{
		{"unknown algorithm", auth.Config{Algorithm: "HS999", Secret: "x"}},
		{"hmac without secret", auth.Config{Algorithm: "HS256"}},
		{"asymmetric without public key", auth.Config{Algorithm: "EdDSA"}},
		{"invalid public key pem", auth.Config{Algorithm: "EdDSA", PublicKeyPEM: "not pem"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := auth.NewManager(tc.cfg); err == nil {
				t.Error("expected NewManager to fail")
			}
		})
	}
}

func TestEdDSARoundTrip(t *testing.T) {
	t.Parallel()

	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}

	manager, err := auth.NewManager(auth.Config{
		Algorithm:     "EdDSA",
		PrivateKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})),
		PublicKeyPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})),
		Issuer:        "huxio",
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	token, _, err := manager.IssueOrgToken(testOrgID, time.Hour)
	if err != nil {
		t.Fatalf("IssueOrgToken: %v", err)
	}
	claims, err := manager.Parse(token)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if claims.Subject != testOrgID {
		t.Errorf("Subject = %q", claims.Subject)
	}

	// A verify-only deployment cannot mint tokens, and must say so rather
	// than silently producing something unusable.
	verifyOnly, err := auth.NewManager(auth.Config{
		Algorithm:    "EdDSA",
		PublicKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})),
	})
	if err != nil {
		t.Fatalf("NewManager(verify only): %v", err)
	}
	if _, _, err := verifyOnly.IssueOrgToken(testOrgID, time.Hour); err == nil {
		t.Error("expected issuing to fail without a signing key")
	}
	if _, err := verifyOnly.Parse(token); err != nil {
		t.Errorf("a verify-only manager must still validate tokens: %v", err)
	}
}
