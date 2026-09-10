// Package auth mints and validates the API's bearer tokens. Two flavours
// exist: an org token whose subject is an organization and which has full
// access to it, and a portal token whose subject is one application and which
// may only manage that application's endpoints and read its attempts.
package auth

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/rotisserie/eris"
)

// ScopePortal marks a token restricted to one application.
const ScopePortal = "portal"

// supportedAlgorithms maps the case-insensitive configured name onto the exact
// JWS algorithm identifier, which is case-sensitive ("EdDSA", not "EDDSA").
var supportedAlgorithms = map[string]string{
	"":      "HS256",
	"HS256": "HS256",
	"HS384": "HS384",
	"HS512": "HS512",
	"RS256": "RS256",
	"RS384": "RS384",
	"RS512": "RS512",
	"EDDSA": "EdDSA",
}

// Config configures the token manager.
type Config struct {
	// Algorithm is one of HS256, HS384, HS512, RS256, RS384, RS512, EdDSA.
	Algorithm string
	// Secret is the shared secret for the HS family.
	Secret string
	// PrivateKeyPEM signs tokens for the asymmetric algorithms.
	PrivateKeyPEM string
	// PublicKeyPEM verifies tokens for the asymmetric algorithms.
	PublicKeyPEM string
	// Issuer is the iss claim written and required.
	Issuer string
}

// Claims is the token payload.
type Claims struct {
	jwt.RegisteredClaims

	// Scope is "portal" on a portal token and empty on an org token.
	Scope string `json:"scope,omitempty"`
	// OrgID is carried on portal tokens, whose subject is an application.
	OrgID string `json:"org,omitempty"`
}

// Manager issues and validates tokens.
type Manager struct {
	method    jwt.SigningMethod
	issuer    string
	signKey   any
	verifyKey any
	parser    *jwt.Parser
}

// NewManager builds a token manager, rejecting a configuration that cannot
// both sign and verify.
func NewManager(cfg Config) (*Manager, error) {
	algorithm, ok := supportedAlgorithms[strings.ToUpper(strings.TrimSpace(cfg.Algorithm))]
	if !ok {
		return nil, eris.Errorf("auth: unsupported jwt algorithm %q", cfg.Algorithm)
	}
	method := jwt.GetSigningMethod(algorithm)
	if method == nil {
		return nil, eris.Errorf("auth: unsupported jwt algorithm %q", algorithm)
	}

	m := &Manager{
		method: method,
		issuer: cfg.Issuer,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{algorithm}),
			jwt.WithIssuedAt(),
		),
	}

	switch {
	case strings.HasPrefix(algorithm, "HS"):
		if cfg.Secret == "" {
			return nil, eris.New("auth: HUXIO_JWT_SECRET is required for the HS algorithms")
		}
		m.signKey = []byte(cfg.Secret)
		m.verifyKey = []byte(cfg.Secret)

	default:
		if cfg.PublicKeyPEM == "" {
			return nil, eris.Errorf("auth: %s requires a public key to verify tokens", algorithm)
		}
		verify, err := parsePublicKey(cfg.PublicKeyPEM)
		if err != nil {
			return nil, err
		}
		m.verifyKey = verify

		// A private key is optional: a deployment that only validates tokens
		// minted elsewhere cannot issue portal tokens, and says so when asked.
		if cfg.PrivateKeyPEM != "" {
			sign, err := parsePrivateKey(cfg.PrivateKeyPEM)
			if err != nil {
				return nil, err
			}
			m.signKey = sign
		}
	}

	return m, nil
}

// IssueOrgToken mints a token with full access to one organization.
func (m *Manager) IssueOrgToken(orgID string, ttl time.Duration) (string, time.Time, error) {
	if !ids.HasKindPrefix(orgID, ids.PrefixOrganization) {
		return "", time.Time{}, apperrors.NewValidationError("subject must be an organization id")
	}
	return m.issue(orgID, "", "", ttl)
}

// IssuePortalToken mints a short-lived token scoped to one application. It is
// minted by the sender's backend and never in a browser.
func (m *Manager) IssuePortalToken(appID, orgID string, ttl time.Duration) (string, time.Time, error) {
	if !ids.HasKindPrefix(appID, ids.PrefixApplication) {
		return "", time.Time{}, apperrors.NewValidationError("subject must be an application id")
	}
	return m.issue(appID, ScopePortal, orgID, ttl)
}

func (m *Manager) issue(subject, scope, orgID string, ttl time.Duration) (string, time.Time, error) {
	if m.signKey == nil {
		return "", time.Time{}, apperrors.NewInternalError(eris.New("auth: no signing key configured"))
	}
	if ttl <= 0 {
		ttl = time.Hour
	}

	now := time.Now().UTC()
	expiry := now.Add(ttl)
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			Issuer:    m.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiry),
		},
		Scope: scope,
		OrgID: orgID,
	}

	signed, err := jwt.NewWithClaims(m.method, claims).SignedString(m.signKey)
	if err != nil {
		return "", time.Time{}, apperrors.NewInternalError(eris.Wrap(err, "sign token"))
	}
	return signed, expiry, nil
}

// Parse validates a token and returns its claims. Every failure maps to a
// single opaque 401: which check failed is not the caller's business.
func (m *Manager) Parse(token string) (*Claims, error) {
	claims := &Claims{}
	parsed, err := m.parser.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) {
		return m.verifyKey, nil
	})
	if err != nil || !parsed.Valid {
		return nil, apperrors.NewUnauthorizedError("invalid or expired token")
	}
	if claims.Subject == "" {
		return nil, apperrors.NewUnauthorizedError("token is missing a subject")
	}
	if m.issuer != "" && claims.Issuer != m.issuer {
		return nil, apperrors.NewUnauthorizedError("token issuer is not accepted")
	}
	if claims.Scope == ScopePortal && !ids.HasKindPrefix(claims.Subject, ids.PrefixApplication) {
		return nil, apperrors.NewUnauthorizedError("portal token subject must be an application")
	}
	return claims, nil
}

func parsePublicKey(pemData string) (crypto.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, eris.New("auth: public key is not valid PEM")
	}
	if key, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		switch typed := key.(type) {
		case *rsa.PublicKey, ed25519.PublicKey:
			return typed, nil
		default:
			return nil, eris.New("auth: unsupported public key type")
		}
	}
	key, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		return nil, eris.Wrap(err, "auth: parse public key")
	}
	return key, nil
}

func parsePrivateKey(pemData string) (crypto.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, eris.New("auth: private key is not valid PEM")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		switch typed := key.(type) {
		case *rsa.PrivateKey, ed25519.PrivateKey:
			return typed, nil
		default:
			return nil, eris.New("auth: unsupported private key type")
		}
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, eris.Wrap(err, "auth: parse private key")
	}
	return key, nil
}
