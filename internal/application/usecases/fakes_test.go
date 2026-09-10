package usecases_test

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/payload"
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/rotisserie/eris"
)

// fakeTx runs the unit of work inline and can be made to fail the commit.
type fakeTx struct {
	commitErr error
	calls     int
}

func (f *fakeTx) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	f.calls++
	if err := fn(ctx); err != nil {
		return err
	}
	return f.commitErr
}

// fakeMessageRepo records inserted messages.
type fakeMessageRepo struct {
	mu        sync.Mutex
	created   []*entities.Message
	createErr error
	payloads  map[string][]byte
}

func newFakeMessageRepo() *fakeMessageRepo {
	return &fakeMessageRepo{payloads: map[string][]byte{}}
}

func (f *fakeMessageRepo) CreateMessage(_ context.Context, msg *entities.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	f.created = append(f.created, msg)
	f.payloads[msg.ID] = msg.Payload
	return nil
}

func (f *fakeMessageRepo) GetMessage(context.Context, repositories.MessageFilters, ...repositories.OrderBy) (*entities.Message, error) {
	return nil, repositories.ErrNotFound
}

func (f *fakeMessageRepo) GetMessages(context.Context, repositories.MessageFilters, repositories.CursorPagination, ...repositories.OrderBy) (repositories.CursorResult[*entities.Message], error) {
	return repositories.CursorResult[*entities.Message]{}, nil
}

func (f *fakeMessageRepo) GetPayload(_ context.Context, id string, _ time.Time) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.payloads[id]
	if !ok {
		return nil, repositories.ErrNotFound
	}
	return stored, nil
}

func (f *fakeMessageRepo) lastCreated() *entities.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) == 0 {
		return nil
	}
	return f.created[len(f.created)-1]
}

// fakeQueue records enqueues, notifications and completions.
type fakeQueue struct {
	mu         sync.Mutex
	enqueued   []repositories.EnqueueTask
	notified   []int16
	enqueueErr error
	notifyErr  error
}

func (f *fakeQueue) Claim(context.Context, repositories.ClaimRequest) ([]entities.DeliveryTask, error) {
	return nil, nil
}
func (f *fakeQueue) Complete(context.Context, int64) error             { return nil }
func (f *fakeQueue) CompleteMany(context.Context, []int64) error       { return nil }
func (f *fakeQueue) Retry(context.Context, int64, time.Duration) error { return nil }
func (f *fakeQueue) Defer(context.Context, int64, time.Duration) error { return nil }
func (f *fakeQueue) Release(context.Context, []int64) error            { return nil }
func (f *fakeQueue) RescueStuck(context.Context) (int64, error)        { return 0, nil }

func (f *fakeQueue) Enqueue(_ context.Context, tasks []repositories.EnqueueTask) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.enqueueErr != nil {
		return f.enqueueErr
	}
	f.enqueued = append(f.enqueued, tasks...)
	return nil
}

func (f *fakeQueue) Notify(_ context.Context, partitionKey int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.notifyErr != nil {
		return f.notifyErr
	}
	f.notified = append(f.notified, partitionKey)
	return nil
}

func (f *fakeQueue) Stats(context.Context, []string) ([]repositories.QueueStats, error) {
	return nil, nil
}

func (f *fakeQueue) tasks() []repositories.EnqueueTask {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]repositories.EnqueueTask(nil), f.enqueued...)
}

func (f *fakeQueue) notifications() []int16 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int16(nil), f.notified...)
}

// staticSnapshot serves one prebuilt snapshot.
type staticSnapshot struct {
	snapshot *config.Snapshot
}

func (s staticSnapshot) Current() *config.Snapshot { return s.snapshot }

// testSealer builds a sealer with one throwaway key.
func testSealer() (*secrets.Sealer, error) {
	key, err := secrets.GenerateKey()
	if err != nil {
		return nil, err
	}
	return secrets.NewSealer([]string{key})
}

// buildSnapshot assembles a snapshot from loose entities, sealing every
// endpoint secret first.
func buildSnapshot(sealer *secrets.Sealer, data *repositories.SnapshotData) (*config.Snapshot, error) {
	for _, ep := range data.Endpoints {
		if len(ep.Secret.Sealed) == 0 {
			sealed, err := sealer.Seal([]byte("whsec_" + ep.ID))
			if err != nil {
				return nil, err
			}
			ep.Secret = entities.SealedSecret{Sealed: sealed}
		}
		if ep.SecretType == "" {
			ep.SecretType = entities.SecretTypeHMAC256
		}
	}
	snapshot, problems := config.BuildSnapshot(data, sealer)
	if len(problems) > 0 {
		return nil, eris.Errorf("snapshot build reported %d problems: %v", len(problems), problems)
	}
	return snapshot, nil
}

// testCodec is the real payload codec: compression is cheap and the tests
// should exercise what production runs.
func testCodec() (*payload.Codec, error) { return payload.NewCodec(512) }

// fakeInvalidator counts config invalidations and can fail.
type fakeInvalidator struct {
	mu   sync.Mutex
	orgs []string
	err  error
}

func (f *fakeInvalidator) NotifyConfigChanged(_ context.Context, orgID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.orgs = append(f.orgs, orgID)
	return nil
}

func (f *fakeInvalidator) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.orgs)
}

// fakeApplicationRepo is an in-memory application store enforcing the same
// uniqueness rule as the database.
type fakeApplicationRepo struct {
	mu   sync.Mutex
	rows map[string]*entities.Application
}

func newFakeApplicationRepo() *fakeApplicationRepo {
	return &fakeApplicationRepo{rows: map[string]*entities.Application{}}
}

func (f *fakeApplicationRepo) CreateApplication(_ context.Context, app *entities.Application) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if app.UID != nil {
		for _, existing := range f.rows {
			if existing.OrgID == app.OrgID && existing.UID != nil && *existing.UID == *app.UID {
				return repositories.ErrConflict
			}
		}
	}
	clone := *app
	f.rows[app.ID] = &clone
	return nil
}

func (f *fakeApplicationRepo) UpdateApplication(_ context.Context, app *entities.Application) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[app.ID]; !ok {
		return repositories.ErrNotFound
	}
	if app.UID != nil {
		for id, existing := range f.rows {
			if id != app.ID && existing.OrgID == app.OrgID && existing.UID != nil && *existing.UID == *app.UID {
				return repositories.ErrConflict
			}
		}
	}
	clone := *app
	f.rows[app.ID] = &clone
	return nil
}

func (f *fakeApplicationRepo) DeleteApplication(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[id]; !ok {
		return repositories.ErrNotFound
	}
	delete(f.rows, id)
	return nil
}

func (f *fakeApplicationRepo) GetApplication(
	_ context.Context,
	filters repositories.ApplicationFilters,
	_ ...repositories.OrderBy,
) (*entities.Application, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, app := range f.rows {
		if !matchString(filters.OrgID, app.OrgID) || !matchString(filters.ID, app.ID) {
			continue
		}
		if filters.UID != nil {
			if app.UID == nil || !matchString(filters.UID, *app.UID) {
				continue
			}
		}
		clone := *app
		return &clone, nil
	}
	return nil, repositories.ErrNotFound
}

func (f *fakeApplicationRepo) GetApplications(
	_ context.Context,
	filters repositories.ApplicationFilters,
	page repositories.CursorPagination,
	_ ...repositories.OrderBy,
) (repositories.CursorResult[*entities.Application], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]*entities.Application, 0, len(f.rows))
	for _, app := range f.rows {
		if !matchString(filters.OrgID, app.OrgID) {
			continue
		}
		clone := *app
		items = append(items, &clone)
	}
	return repositories.CursorResult[*entities.Application]{Items: items, Limit: page.GetLimit()}, nil
}

// fakeEndpointRepo is an in-memory endpoint store.
type fakeEndpointRepo struct {
	mu   sync.Mutex
	rows map[string]*entities.Endpoint
}

func newFakeEndpointRepo() *fakeEndpointRepo {
	return &fakeEndpointRepo{rows: map[string]*entities.Endpoint{}}
}

func (f *fakeEndpointRepo) CreateEndpoint(_ context.Context, ep *entities.Endpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ep.UID != nil {
		for _, existing := range f.rows {
			if existing.AppID == ep.AppID && existing.UID != nil && *existing.UID == *ep.UID {
				return repositories.ErrConflict
			}
		}
	}
	clone := *ep
	f.rows[ep.ID] = &clone
	return nil
}

func (f *fakeEndpointRepo) UpdateEndpoint(_ context.Context, ep *entities.Endpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[ep.ID]; !ok {
		return repositories.ErrNotFound
	}
	clone := *ep
	f.rows[ep.ID] = &clone
	return nil
}

func (f *fakeEndpointRepo) DeleteEndpoint(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[id]; !ok {
		return repositories.ErrNotFound
	}
	delete(f.rows, id)
	return nil
}

func (f *fakeEndpointRepo) GetEndpoint(
	_ context.Context,
	filters repositories.EndpointFilters,
	_ ...repositories.OrderBy,
) (*entities.Endpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ep := range f.rows {
		if !matchString(filters.AppID, ep.AppID) || !matchString(filters.ID, ep.ID) {
			continue
		}
		if filters.UID != nil {
			if ep.UID == nil || !matchString(filters.UID, *ep.UID) {
				continue
			}
		}
		clone := *ep
		return &clone, nil
	}
	return nil, repositories.ErrNotFound
}

func (f *fakeEndpointRepo) GetEndpoints(
	_ context.Context,
	filters repositories.EndpointFilters,
	page repositories.CursorPagination,
	_ ...repositories.OrderBy,
) (repositories.CursorResult[*entities.Endpoint], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]*entities.Endpoint, 0, len(f.rows))
	for _, ep := range f.rows {
		if !matchString(filters.AppID, ep.AppID) {
			continue
		}
		clone := *ep
		items = append(items, &clone)
	}
	return repositories.CursorResult[*entities.Endpoint]{Items: items, Limit: page.GetLimit()}, nil
}

func (f *fakeEndpointRepo) SetPool(_ context.Context, id, pool string) error {
	return f.mutate(id, func(ep *entities.Endpoint) { ep.Pool = pool })
}

func (f *fakeEndpointRepo) SetDisabled(_ context.Context, id string, at *time.Time) error {
	return f.mutate(id, func(ep *entities.Endpoint) { ep.DisabledAt = at })
}

func (f *fakeEndpointRepo) SetFirstFailure(_ context.Context, id string, at *time.Time) error {
	return f.mutate(id, func(ep *entities.Endpoint) { ep.FirstFailureAt = at })
}

func (f *fakeEndpointRepo) RotateSecret(
	_ context.Context,
	id string,
	secret entities.SealedSecret,
	old []entities.SealedSecret,
) error {
	return f.mutate(id, func(ep *entities.Endpoint) {
		ep.Secret = secret
		ep.OldSecrets = old
	})
}

func (f *fakeEndpointRepo) mutate(id string, fn func(*entities.Endpoint)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ep, ok := f.rows[id]
	if !ok {
		return repositories.ErrNotFound
	}
	fn(ep)
	return nil
}

func (f *fakeEndpointRepo) get(id string) *entities.Endpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	ep, ok := f.rows[id]
	if !ok {
		return nil
	}
	clone := *ep
	return &clone
}

// fakeEventTypeRepo is an in-memory event type store with the same
// archive-and-revive semantics as the database.
type fakeEventTypeRepo struct {
	mu   sync.Mutex
	rows map[string]*entities.EventType
}

func newFakeEventTypeRepo() *fakeEventTypeRepo {
	return &fakeEventTypeRepo{rows: map[string]*entities.EventType{}}
}

func eventTypeRowKey(orgID, name string) string { return orgID + "/" + name }

func (f *fakeEventTypeRepo) CreateEventType(_ context.Context, et *entities.EventType) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := eventTypeRowKey(et.OrgID, et.Name)
	if existing, ok := f.rows[key]; ok && existing.DeletedAt == nil {
		return repositories.ErrConflict
	}
	clone := *et
	f.rows[key] = &clone
	return nil
}

func (f *fakeEventTypeRepo) UpdateEventType(_ context.Context, et *entities.EventType) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := eventTypeRowKey(et.OrgID, et.Name)
	existing, ok := f.rows[key]
	if !ok || existing.DeletedAt != nil {
		return repositories.ErrNotFound
	}
	clone := *et
	f.rows[key] = &clone
	return nil
}

func (f *fakeEventTypeRepo) ArchiveEventType(_ context.Context, orgID, name string) error {
	return f.setDeleted(orgID, name, true)
}

func (f *fakeEventTypeRepo) UnarchiveEventType(_ context.Context, orgID, name string) error {
	return f.setDeleted(orgID, name, false)
}

func (f *fakeEventTypeRepo) setDeleted(orgID, name string, deleted bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	et, ok := f.rows[eventTypeRowKey(orgID, name)]
	if !ok {
		return repositories.ErrNotFound
	}
	if deleted == (et.DeletedAt != nil) {
		return repositories.ErrNotFound
	}
	if deleted {
		now := time.Now().UTC()
		et.DeletedAt = &now
	} else {
		et.DeletedAt = nil
	}
	return nil
}

func (f *fakeEventTypeRepo) GetEventType(
	_ context.Context,
	filters repositories.EventTypeFilters,
	_ ...repositories.OrderBy,
) (*entities.EventType, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, et := range f.rows {
		if !matchString(filters.OrgID, et.OrgID) || !matchString(filters.Name, et.Name) {
			continue
		}
		if !filters.IncludeArchived && et.DeletedAt != nil {
			continue
		}
		clone := *et
		return &clone, nil
	}
	return nil, repositories.ErrNotFound
}

func (f *fakeEventTypeRepo) GetEventTypes(
	_ context.Context,
	filters repositories.EventTypeFilters,
	page repositories.CursorPagination,
	_ ...repositories.OrderBy,
) (repositories.CursorResult[*entities.EventType], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]*entities.EventType, 0, len(f.rows))
	for _, et := range f.rows {
		if !matchString(filters.OrgID, et.OrgID) {
			continue
		}
		if !filters.IncludeArchived && et.DeletedAt != nil {
			continue
		}
		clone := *et
		items = append(items, &clone)
	}
	return repositories.CursorResult[*entities.EventType]{Items: items, Limit: page.GetLimit()}, nil
}

// matchString applies the subset of Filter semantics the fakes need.
func matchString(filter *repositories.Filter[string], value string) bool {
	if filter == nil || filter.Is == nil {
		return true
	}
	return *filter.Is == value
}

// fakeOrganizationRepo is an in-memory tenant store. Names are not unique,
// matching the schema.
type fakeOrganizationRepo struct {
	mu   sync.Mutex
	rows map[string]*entities.Organization
}

func newFakeOrganizationRepo() *fakeOrganizationRepo {
	return &fakeOrganizationRepo{rows: map[string]*entities.Organization{}}
}

func (f *fakeOrganizationRepo) CreateOrganization(_ context.Context, org *entities.Organization) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := *org
	f.rows[org.ID] = &clone
	return nil
}

func (f *fakeOrganizationRepo) UpdateOrganization(_ context.Context, org *entities.Organization) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[org.ID]; !ok {
		return repositories.ErrNotFound
	}
	clone := *org
	f.rows[org.ID] = &clone
	return nil
}

func (f *fakeOrganizationRepo) DeleteOrganization(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[id]; !ok {
		return repositories.ErrNotFound
	}
	delete(f.rows, id)
	return nil
}

func (f *fakeOrganizationRepo) GetOrganization(
	_ context.Context,
	filters repositories.OrganizationFilters,
	_ ...repositories.OrderBy,
) (*entities.Organization, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, org := range f.rows {
		if !matchString(filters.ID, org.ID) || !matchString(filters.Name, org.Name) {
			continue
		}
		clone := *org
		return &clone, nil
	}
	return nil, repositories.ErrNotFound
}

func (f *fakeOrganizationRepo) GetOrganizations(
	_ context.Context,
	filters repositories.OrganizationFilters,
	page repositories.CursorPagination,
	_ ...repositories.OrderBy,
) (repositories.CursorResult[*entities.Organization], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]*entities.Organization, 0, len(f.rows))
	for _, org := range f.rows {
		if filters.Name != nil && filters.Name.IContains != nil &&
			!strings.Contains(strings.ToLower(org.Name), strings.ToLower(*filters.Name.IContains)) {
			continue
		}
		clone := *org
		items = append(items, &clone)
	}
	return repositories.CursorResult[*entities.Organization]{Items: items, Limit: page.GetLimit()}, nil
}
