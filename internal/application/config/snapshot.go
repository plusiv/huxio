// Package config owns the in-memory configuration snapshot. Apps, endpoints,
// event types and decrypted signing keys live behind one atomic pointer in
// every process, so the delivery path reads them with a pointer load and never
// with a query.
package config

import (
	"encoding/json"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/rotisserie/eris"
)

// Secret is a signing key already decrypted, so no delivery ever pays for a
// decryption.
type Secret struct {
	Key       []byte
	Type      entities.SecretType
	ExpiresAt *time.Time
}

// Endpoint is the delivery-ready view of an endpoint: the stored row plus the
// values that would otherwise be parsed or decrypted per delivery.
type Endpoint struct {
	*entities.Endpoint

	// Secrets holds the current signing key first, followed by every rotated-out
	// key still inside its overlap window.
	Secrets []Secret
	// CustomHeaders are the endpoint's extra request headers, parsed once.
	CustomHeaders map[string]string
}

// Application is an application plus its endpoints, with the event-type match
// precomputed so fan-out is a map lookup rather than a scan.
type Application struct {
	*entities.Application

	Endpoints []*Endpoint

	byEventType   map[string][]*Endpoint
	allEventTypes []*Endpoint
}

// Snapshot is an immutable view of the whole configuration set.
type Snapshot struct {
	builtAt time.Time

	orgs       map[string]*entities.Organization
	apps       map[string]*Application
	appsByUID  map[string]*Application
	endpoints  map[string]*Endpoint
	eventTypes map[string]*entities.EventType
}

// BuildSnapshot assembles a snapshot, decrypting every signing secret exactly
// once. A secret that cannot be opened does not fail the build: that endpoint
// is skipped and the rest of the tenant keeps working.
func BuildSnapshot(data *repositories.SnapshotData, sealer *secrets.Sealer) (*Snapshot, []error) {
	snapshot := &Snapshot{
		builtAt:    time.Now().UTC(),
		orgs:       make(map[string]*entities.Organization, len(data.Organizations)),
		apps:       make(map[string]*Application, len(data.Applications)),
		appsByUID:  make(map[string]*Application, len(data.Applications)),
		endpoints:  make(map[string]*Endpoint, len(data.Endpoints)),
		eventTypes: make(map[string]*entities.EventType, len(data.EventTypes)),
	}

	for _, org := range data.Organizations {
		snapshot.orgs[org.ID] = org
	}
	for _, et := range data.EventTypes {
		snapshot.eventTypes[eventTypeKey(et.OrgID, et.Name)] = et
	}
	for _, app := range data.Applications {
		entry := &Application{
			Application: app,
			byEventType: make(map[string][]*Endpoint),
		}
		snapshot.apps[app.ID] = entry
		if app.UID != nil && *app.UID != "" {
			snapshot.appsByUID[appUIDKey(app.OrgID, *app.UID)] = entry
		}
	}

	var problems []error
	now := time.Now().UTC()
	for _, ep := range data.Endpoints {
		entry, err := buildEndpoint(ep, sealer, now)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		snapshot.endpoints[ep.ID] = entry

		app, ok := snapshot.apps[ep.AppID]
		if !ok {
			// An endpoint whose application was deleted: nothing can route to it, so
			// leave it out of the fan-out index.
			continue
		}
		app.Endpoints = append(app.Endpoints, entry)
		if len(ep.EventTypes) == 0 {
			app.allEventTypes = append(app.allEventTypes, entry)
			continue
		}
		for _, name := range ep.EventTypes {
			app.byEventType[name] = append(app.byEventType[name], entry)
		}
	}

	return snapshot, problems
}

func buildEndpoint(ep *entities.Endpoint, sealer *secrets.Sealer, now time.Time) (*Endpoint, error) {
	entry := &Endpoint{Endpoint: ep}

	for _, sealed := range ep.ActiveSecrets(now) {
		key, err := sealer.Open(sealed.Sealed)
		if err != nil {
			return nil, eris.Wrapf(err, "open signing secret for endpoint %s", ep.ID)
		}
		entry.Secrets = append(entry.Secrets, Secret{
			Key:       key,
			Type:      ep.SecretType,
			ExpiresAt: sealed.ExpiresAt,
		})
	}

	if len(ep.Headers) > 0 {
		headers := map[string]string{}
		if err := json.Unmarshal(ep.Headers, &headers); err != nil {
			return nil, eris.Wrapf(err, "parse custom headers for endpoint %s", ep.ID)
		}
		entry.CustomHeaders = headers
	}

	return entry, nil
}

// BuiltAt reports when this snapshot was assembled.
func (s *Snapshot) BuiltAt() time.Time { return s.builtAt }

// Age reports how stale this snapshot is. Above five minutes means LISTEN died
// and readiness must fail.
func (s *Snapshot) Age() time.Duration { return time.Since(s.builtAt) }

// Org returns a tenant, or nil.
func (s *Snapshot) Org(id string) *entities.Organization { return s.orgs[id] }

// App returns an application by id, or nil.
func (s *Snapshot) App(id string) *Application { return s.apps[id] }

// AppByUID returns an application by its tenant-assigned identifier, or nil.
func (s *Snapshot) AppByUID(orgID, uid string) *Application {
	return s.appsByUID[appUIDKey(orgID, uid)]
}

// ResolveApp accepts either the native id or the tenant-assigned uid, which
// the compatible SDKs rely on, and returns nil when neither matches inside the
// organization.
func (s *Snapshot) ResolveApp(orgID, idOrUID string) *Application {
	if app := s.apps[idOrUID]; app != nil && app.OrgID == orgID {
		return app
	}
	return s.AppByUID(orgID, idOrUID)
}

// Endpoint returns an endpoint by id, or nil.
func (s *Snapshot) Endpoint(id string) *Endpoint { return s.endpoints[id] }

// EventType returns an event type by tenant and name, or nil.
func (s *Snapshot) EventType(orgID, name string) *entities.EventType {
	return s.eventTypes[eventTypeKey(orgID, name)]
}

// Counts reports the snapshot's size, for metrics and the admin endpoint.
func (s *Snapshot) Counts() (orgs, apps, endpoints, eventTypes int) {
	return len(s.orgs), len(s.apps), len(s.endpoints), len(s.eventTypes)
}

// MatchingEndpoints returns every endpoint of an application that should
// receive a message: subscribed to the event type, matching the channel
// filter, and neither disabled nor deleted. It is pure in-memory set logic
// against precomputed indexes, so fan-out costs no I/O.
func (s *Snapshot) MatchingEndpoints(appID, eventType string, channels []string) []*Endpoint {
	app := s.apps[appID]
	if app == nil {
		return nil
	}

	subscribed := app.byEventType[eventType]
	matched := make([]*Endpoint, 0, len(subscribed)+len(app.allEventTypes))

	for _, ep := range subscribed {
		if deliverableTo(ep, channels) {
			matched = append(matched, ep)
		}
	}
	for _, ep := range app.allEventTypes {
		if deliverableTo(ep, channels) {
			matched = append(matched, ep)
		}
	}
	return matched
}

func deliverableTo(ep *Endpoint, channels []string) bool {
	return ep.Deliverable() && ep.WantsChannels(channels)
}

func appUIDKey(orgID, uid string) string     { return orgID + "\x00" + uid }
func eventTypeKey(orgID, name string) string { return orgID + "\x00" + name }
