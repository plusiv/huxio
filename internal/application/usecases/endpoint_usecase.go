package usecases

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// SecretPrefix marks a generated signing secret, matching the Standard
// Webhooks convention receivers already expect.
const SecretPrefix = "whsec_"

// SecretRotationOverlap is how long a rotated-out secret keeps producing
// signatures. Never invalidate immediately: receivers need a window to pick up
// the new key.
const SecretRotationOverlap = 24 * time.Hour

// reservedHeaders may never be set by a custom endpoint header. Signature
// headers in particular decide whether a receiver trusts the request.
var reservedHeaders = map[string]struct{}{
	"webhook-id": {}, "webhook-timestamp": {}, "webhook-signature": {},
	"huxio-id": {}, "huxio-timestamp": {}, "huxio-signature": {},
	"content-type": {}, "content-length": {}, "host": {}, "user-agent": {},
	"transfer-encoding": {}, "connection": {},
}

// CreateEndpointInput is a create request.
type CreateEndpointInput struct {
	OrgID       string
	AppID       string
	URL         string
	Description string
	UID         *string
	Version     *int
	RateLimit   *int
	EventTypes  []string
	Channels    []string
	Headers     map[string]string
	// Secret is the caller-supplied signing secret. Empty means generate one.
	Secret     string
	SecretType *entities.SecretType
	Pool       string
}

// UpdateEndpointInput is a full or partial update.
type UpdateEndpointInput struct {
	URL         *string
	Description *string
	UID         *string
	Version     *int
	RateLimit   *int
	EventTypes  []string
	Channels    []string
	// Disabled toggles the endpoint off or back on.
	Disabled *bool
	// ClearEventTypes and ClearChannels distinguish "leave unchanged" from
	// "subscribe to everything", which a nil slice cannot express.
	ClearEventTypes bool
	ClearChannels   bool
}

// ListEndpointsInput is a list request.
type ListEndpointsInput struct {
	AppID  string
	Cursor string
	Limit  int
}

// EndpointUseCase owns endpoint lifecycle, including signing secrets.
type EndpointUseCase struct {
	endpointRepo repositories.EndpointRepository
	sealer       SecretSealer
	invalidator  ConfigInvalidator
}

// NewEndpointUseCase builds the use case.
func NewEndpointUseCase(
	endpointRepo repositories.EndpointRepository,
	sealer SecretSealer,
	invalidator ConfigInvalidator,
) *EndpointUseCase {
	return &EndpointUseCase{endpointRepo: endpointRepo, sealer: sealer, invalidator: invalidator}
}

// CreateEndpoint inserts an endpoint, generating and sealing a signing secret
// when the caller did not supply one.
func (uc *EndpointUseCase) CreateEndpoint(ctx context.Context, in CreateEndpointInput) (*entities.Endpoint, string, error) {
	if err := validateEndpointURL(in.URL); err != nil {
		return nil, "", err
	}
	// Header names are rejected at create time, not at delivery: a broken
	// header must fail the request that introduced it.
	headers, err := encodeCustomHeaders(in.Headers)
	if err != nil {
		return nil, "", err
	}
	secretType := utils.Deref(in.SecretType, entities.SecretTypeHMAC256)
	if !secretType.Valid() {
		return nil, "", apperrors.NewValidationError("unsupported secret type")
	}
	if in.RateLimit != nil && *in.RateLimit <= 0 {
		return nil, "", apperrors.NewValidationError("rateLimit must be positive")
	}

	secret := in.Secret
	if utils.IsBlank(secret) {
		generated, err := GenerateSecret()
		if err != nil {
			return nil, "", apperrors.NewInternalError(err)
		}
		secret = generated
	} else if !strings.HasPrefix(secret, SecretPrefix) {
		return nil, "", apperrors.NewValidationError("secret must start with " + SecretPrefix)
	}

	sealed, err := uc.sealer.Seal([]byte(secret))
	if err != nil {
		return nil, "", apperrors.NewInternalError(err)
	}

	now := time.Now().UTC()
	ep := &entities.Endpoint{
		ID:          ids.New(ids.PrefixEndpoint),
		AppID:       in.AppID,
		OrgID:       in.OrgID,
		UID:         utils.NilIfBlank(utils.Deref(in.UID, "")),
		URL:         in.URL,
		Description: in.Description,
		Version:     max(utils.Deref(in.Version, 1), 1),
		RateLimit:   in.RateLimit,
		EventTypes:  utils.Unique(in.EventTypes),
		Channels:    utils.Unique(in.Channels),
		Headers:     headers,
		Secret:      entities.SealedSecret{Sealed: sealed},
		SecretType:  secretType,
		Pool:        utils.Fallback(in.Pool, "default"),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := uc.endpointRepo.CreateEndpoint(ctx, ep); err != nil {
		if eris.Is(err, repositories.ErrConflict) {
			return nil, "", apperrors.NewConflictError("an endpoint with this uid already exists")
		}
		return nil, "", apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, in.OrgID)

	// The plaintext secret is returned exactly once, at create time.
	return ep, secret, nil
}

// GetEndpoint resolves an endpoint by id or uid within an application.
func (uc *EndpointUseCase) GetEndpoint(ctx context.Context, appID, idOrUID string) (*entities.Endpoint, error) {
	ep, err := uc.endpointRepo.GetEndpoint(ctx, repositories.EndpointFilters{
		AppID: repositories.Eq(appID),
		ID:    repositories.Eq(idOrUID),
	})
	if err == nil {
		return ep, nil
	}
	if !eris.Is(err, repositories.ErrNotFound) {
		return nil, apperrors.NewInternalError(err)
	}

	ep, err = uc.endpointRepo.GetEndpoint(ctx, repositories.EndpointFilters{
		AppID: repositories.Eq(appID),
		UID:   repositories.Eq(idOrUID),
	})
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("endpoint")
		}
		return nil, apperrors.NewInternalError(err)
	}
	return ep, nil
}

// UpdateEndpoint applies the supplied fields.
func (uc *EndpointUseCase) UpdateEndpoint(
	ctx context.Context,
	appID, idOrUID string,
	in UpdateEndpointInput,
) (*entities.Endpoint, error) {
	ep, err := uc.GetEndpoint(ctx, appID, idOrUID)
	if err != nil {
		return nil, err
	}

	if in.URL != nil {
		if err := validateEndpointURL(*in.URL); err != nil {
			return nil, err
		}
		ep.URL = *in.URL
	}
	if in.Description != nil {
		ep.Description = *in.Description
	}
	if in.UID != nil {
		ep.UID = utils.NilIfBlank(*in.UID)
	}
	if in.Version != nil {
		if *in.Version < 1 {
			return nil, apperrors.NewValidationError("version must be at least 1")
		}
		ep.Version = *in.Version
	}
	if in.RateLimit != nil {
		if *in.RateLimit <= 0 {
			return nil, apperrors.NewValidationError("rateLimit must be positive")
		}
		ep.RateLimit = in.RateLimit
	}
	switch {
	case in.ClearEventTypes:
		ep.EventTypes = nil
	case in.EventTypes != nil:
		ep.EventTypes = utils.Unique(in.EventTypes)
	}
	switch {
	case in.ClearChannels:
		ep.Channels = nil
	case in.Channels != nil:
		ep.Channels = utils.Unique(in.Channels)
	}
	ep.UpdatedAt = time.Now().UTC()

	if err := uc.endpointRepo.UpdateEndpoint(ctx, ep); err != nil {
		if eris.Is(err, repositories.ErrConflict) {
			return nil, apperrors.NewConflictError("an endpoint with this uid already exists")
		}
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("endpoint")
		}
		return nil, apperrors.NewInternalError(err)
	}

	if in.Disabled != nil {
		if err := uc.setDisabled(ctx, ep, *in.Disabled); err != nil {
			return nil, err
		}
	}
	uc.invalidate(ctx, ep.OrgID)
	return ep, nil
}

// SetDisabled switches an endpoint off or back on. Disabling is not deleting:
// a disabled endpoint can be re-enabled from the portal.
func (uc *EndpointUseCase) SetDisabled(ctx context.Context, appID, idOrUID string, disabled bool) (*entities.Endpoint, error) {
	ep, err := uc.GetEndpoint(ctx, appID, idOrUID)
	if err != nil {
		return nil, err
	}
	if err := uc.setDisabled(ctx, ep, disabled); err != nil {
		return nil, err
	}
	uc.invalidate(ctx, ep.OrgID)
	return ep, nil
}

func (uc *EndpointUseCase) setDisabled(ctx context.Context, ep *entities.Endpoint, disabled bool) error {
	var at *time.Time
	if disabled {
		at = utils.Ptr(time.Now().UTC())
	}
	if err := uc.endpointRepo.SetDisabled(ctx, ep.ID, at); err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return apperrors.NewNotFoundError("endpoint")
		}
		return apperrors.NewInternalError(err)
	}
	ep.DisabledAt = at

	if !disabled {
		// Re-enabling starts the failure window over.
		if err := uc.endpointRepo.SetFirstFailure(ctx, ep.ID, nil); err != nil {
			return apperrors.NewInternalError(err)
		}
		ep.FirstFailureAt = nil
	}
	return nil
}

// DeleteEndpoint soft-deletes an endpoint. Unlike disabling, this is final.
func (uc *EndpointUseCase) DeleteEndpoint(ctx context.Context, appID, idOrUID string) error {
	ep, err := uc.GetEndpoint(ctx, appID, idOrUID)
	if err != nil {
		return err
	}
	if err := uc.endpointRepo.DeleteEndpoint(ctx, ep.ID); err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return apperrors.NewNotFoundError("endpoint")
		}
		return apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, ep.OrgID)
	return nil
}

// ListEndpoints returns a cursor-paginated page.
func (uc *EndpointUseCase) ListEndpoints(
	ctx context.Context,
	in ListEndpointsInput,
) (repositories.CursorResult[*entities.Endpoint], error) {
	result, err := uc.endpointRepo.GetEndpoints(ctx,
		repositories.EndpointFilters{AppID: repositories.Eq(in.AppID)},
		repositories.CursorPagination{Cursor: in.Cursor, Limit: in.Limit},
	)
	if err != nil {
		return result, apperrors.NewInternalError(err)
	}
	return result, nil
}

// RevealSecret returns an endpoint's current signing secret in plaintext.
func (uc *EndpointUseCase) RevealSecret(ctx context.Context, appID, idOrUID string) (string, error) {
	ep, err := uc.GetEndpoint(ctx, appID, idOrUID)
	if err != nil {
		return "", err
	}
	plaintext, err := uc.sealer.Open(ep.Secret.Sealed)
	if err != nil {
		return "", apperrors.NewInternalError(err)
	}
	return string(plaintext), nil
}

// RotateSecret installs a new signing secret and keeps the previous one alive
// for the overlap window, so both signatures are emitted and receivers can
// roll over without downtime.
func (uc *EndpointUseCase) RotateSecret(ctx context.Context, appID, idOrUID, newSecret string) (string, error) {
	ep, err := uc.GetEndpoint(ctx, appID, idOrUID)
	if err != nil {
		return "", err
	}

	secret := newSecret
	if utils.IsBlank(secret) {
		generated, err := GenerateSecret()
		if err != nil {
			return "", apperrors.NewInternalError(err)
		}
		secret = generated
	} else if !strings.HasPrefix(secret, SecretPrefix) {
		return "", apperrors.NewValidationError("secret must start with " + SecretPrefix)
	}

	sealed, err := uc.sealer.Seal([]byte(secret))
	if err != nil {
		return "", apperrors.NewInternalError(err)
	}

	now := time.Now().UTC()
	retired := entities.SealedSecret{Sealed: ep.Secret.Sealed, ExpiresAt: utils.Ptr(now.Add(SecretRotationOverlap))}
	old := make([]entities.SealedSecret, 0, len(ep.OldSecrets)+1)
	old = append(old, retired)
	for _, prior := range ep.OldSecrets {
		if !prior.Expired(now) {
			old = append(old, prior)
		}
	}

	if err := uc.endpointRepo.RotateSecret(ctx, ep.ID, entities.SealedSecret{Sealed: sealed}, old); err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return "", apperrors.NewNotFoundError("endpoint")
		}
		return "", apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, ep.OrgID)
	return secret, nil
}

// GetHeaders returns an endpoint's custom headers.
func (uc *EndpointUseCase) GetHeaders(ctx context.Context, appID, idOrUID string) (map[string]string, error) {
	ep, err := uc.GetEndpoint(ctx, appID, idOrUID)
	if err != nil {
		return nil, err
	}
	if len(ep.Headers) == 0 {
		return map[string]string{}, nil
	}
	headers := map[string]string{}
	if err := json.Unmarshal(ep.Headers, &headers); err != nil {
		return nil, apperrors.NewInternalError(err)
	}
	return headers, nil
}

// PatchHeaders merges headers into an endpoint's set. An empty value removes a
// header.
func (uc *EndpointUseCase) PatchHeaders(ctx context.Context, appID, idOrUID string, patch map[string]string) (map[string]string, error) {
	ep, err := uc.GetEndpoint(ctx, appID, idOrUID)
	if err != nil {
		return nil, err
	}
	current, err := uc.GetHeaders(ctx, appID, idOrUID)
	if err != nil {
		return nil, err
	}

	for name, value := range patch {
		if value == "" {
			delete(current, name)
			continue
		}
		current[name] = value
	}
	encoded, err := encodeCustomHeaders(current)
	if err != nil {
		return nil, err
	}

	ep.Headers = encoded
	ep.UpdatedAt = time.Now().UTC()
	if err := uc.endpointRepo.UpdateEndpoint(ctx, ep); err != nil {
		return nil, apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, ep.OrgID)
	return current, nil
}

// SetPool moves an endpoint between worker pools, which is how quarantine is
// applied and lifted.
func (uc *EndpointUseCase) SetPool(ctx context.Context, endpointID, pool string) error {
	if utils.IsBlank(pool) {
		return apperrors.NewValidationError("pool is required")
	}
	if err := uc.endpointRepo.SetPool(ctx, endpointID, pool); err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return apperrors.NewNotFoundError("endpoint")
		}
		return apperrors.NewInternalError(err)
	}
	return nil
}

// GenerateSecret returns a fresh signing secret in the Standard Webhooks form.
func GenerateSecret() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", eris.Wrap(err, "generate signing secret")
	}
	return SecretPrefix + base64.StdEncoding.EncodeToString(raw), nil
}

func validateEndpointURL(raw string) error {
	if utils.IsBlank(raw) {
		return apperrors.NewValidationError("url is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return apperrors.NewValidationError("url is not a valid URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return apperrors.NewValidationError("url must use http or https")
	}
	if parsed.Host == "" {
		return apperrors.NewValidationError("url must include a host")
	}
	return nil
}

func encodeCustomHeaders(headers map[string]string) (json.RawMessage, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	for name := range headers {
		if !isValidHeaderName(name) {
			return nil, apperrors.NewValidationError("invalid header name: " + name)
		}
		if _, reserved := reservedHeaders[strings.ToLower(name)]; reserved {
			return nil, apperrors.NewValidationError("header may not be overridden: " + name)
		}
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return nil, apperrors.NewInternalError(err)
	}
	return encoded, nil
}

func isValidHeaderName(name string) bool {
	if name == "" {
		return false
	}
	// http.CanonicalHeaderKey returns the input unchanged when it contains a
	// character that is not valid in a header name.
	return http.CanonicalHeaderKey(name) == http.CanonicalHeaderKey(strings.ToLower(name)) &&
		!strings.ContainsAny(name, " \t\r\n:()<>@,;\\\"/[]?={}")
}

func (uc *EndpointUseCase) invalidate(ctx context.Context, orgID string) {
	if uc.invalidator == nil {
		return
	}
	if err := uc.invalidator.NotifyConfigChanged(ctx, orgID); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("config invalidation failed")
	}
}
