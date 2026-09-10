package http_test

import (
	"encoding/json"
	"testing"

	"github.com/labstack/echo/v5"

	inboundhttp "github.com/plusiv/huxio/internal/adapters/inbound/http"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/openapi"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
)

// specRouter builds the real route table with empty handlers: the spec is
// derived from routes and payload types, never from a live server.
func specRouter() *echo.Echo {
	return inboundhttp.NewRouter(inboundhttp.RouterDeps{
		Metrics:            telemetry.New(),
		HealthHandler:      &handlers.HealthHandler{},
		ApplicationHandler: &handlers.ApplicationHandler{},
		EndpointHandler:    &handlers.EndpointHandler{},
		EventTypeHandler:   &handlers.EventTypeHandler{},
		MessageHandler:     &handlers.MessageHandler{},
		AttemptHandler:     &handlers.AttemptHandler{},
		PortalTokenHandler: &handlers.PortalHandler{},
		AdminHandler:       &handlers.AdminHandler{},
		StreamHandler:      &handlers.StreamHandler{},
	})
}

// TestEveryRouteIsDocumented is the guard that keeps the published spec
// honest: adding a route without documenting it fails here rather than
// shipping a gap.
func TestEveryRouteIsDocumented(t *testing.T) {
	t.Parallel()

	docs := handlers.RouteDocs()
	router := specRouter()

	var undocumented []string
	for _, route := range router.Router().Routes() {
		candidate := openapi.Route{Method: route.Method, Path: route.Path}
		if !openapi.Documentable(candidate) {
			continue
		}
		if _, ok := docs[route.Method+" "+route.Path]; !ok {
			undocumented = append(undocumented, route.Method+" "+route.Path)
		}
	}
	if len(undocumented) > 0 {
		t.Errorf("routes without an entry in RouteDocs: %v", undocumented)
	}
}

// TestNoStaleRouteDocs is the other direction: a documented route that no
// longer exists means the spec advertises something that 404s.
func TestNoStaleRouteDocs(t *testing.T) {
	t.Parallel()

	registered := map[string]struct{}{}
	for _, route := range specRouter().Router().Routes() {
		registered[route.Method+" "+route.Path] = struct{}{}
	}

	var stale []string
	for key := range handlers.RouteDocs() {
		if _, ok := registered[key]; !ok {
			stale = append(stale, key)
		}
	}
	if len(stale) > 0 {
		t.Errorf("documented routes that are not registered: %v", stale)
	}
}

func TestSpecShape(t *testing.T) {
	t.Parallel()

	document := inboundhttp.Spec(specRouter(),
		openapi.Info{Title: "huxio", Version: "test"},
		[]openapi.Server{{URL: "http://localhost:8080"}},
	)

	if document.OpenAPI != "3.0.3" {
		t.Errorf("openapi = %q", document.OpenAPI)
	}
	if len(document.Paths) == 0 {
		t.Fatal("the document has no paths")
	}
	if _, ok := document.Components.SecuritySchemes["bearerAuth"]; !ok {
		t.Error("the document must declare the bearer scheme")
	}

	// The document must be valid JSON and round-trip.
	encoded, err := document.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("the generated document is not valid JSON: %v", err)
	}

	t.Run("ingest operation", func(t *testing.T) {
		item, ok := document.Paths["/api/v1/app/:app_id/msg"]
		if !ok || item.Post == nil {
			t.Fatal("the ingest route is missing")
		}
		post := item.Post

		if post.RequestBody == nil {
			t.Fatal("ingest must document a request body")
		}
		schema := post.RequestBody.Content["application/json"].Schema
		if schema == nil || schema.Properties["eventType"] == nil {
			t.Fatalf("request schema = %+v", schema)
		}
		// The schema is reflected off the struct the binder validates, so required
		// fields come from the validate tags.
		var hasEventType, hasPayload bool
		for _, name := range schema.Required {
			switch name {
			case "eventType":
				hasEventType = true
			case "payload":
				hasPayload = true
			}
		}
		if !hasEventType || !hasPayload {
			t.Errorf("required = %v, want eventType and payload", schema.Required)
		}
		if got := schema.Properties["eventType"].MaxLength; got == nil || *got != 256 {
			t.Errorf("eventType maxLength = %v, want the validated 256", got)
		}

		if _, ok := post.Responses["202"]; !ok {
			t.Errorf("ingest must document a 202, got %v", responseCodes(post))
		}
		if _, ok := post.Responses["401"]; !ok {
			t.Error("an authenticated route must document its 401")
		}
	})

	t.Run("path parameters are derived", func(t *testing.T) {
		item := document.Paths["/api/v1/app/:app_id/endpoint/:endpoint_id"]
		if item == nil || item.Get == nil {
			t.Fatal("the endpoint route is missing")
		}
		names := map[string]bool{}
		for _, parameter := range item.Get.Parameters {
			if parameter.In == "path" {
				names[parameter.Name] = parameter.Required
			}
		}
		for _, want := range []string{"app_id", "endpoint_id"} {
			if !names[want] {
				t.Errorf("path parameter %q missing or optional: %v", want, names)
			}
		}
	})

	t.Run("health is public", func(t *testing.T) {
		item := document.Paths["/api/v1/health"]
		if item == nil || item.Get == nil {
			t.Fatal("the health route is missing")
		}
		if item.Get.Security == nil || len(item.Get.Security) != 0 {
			t.Errorf("health security = %v, want an explicit empty list", item.Get.Security)
		}
		if _, ok := item.Get.Responses["401"]; ok {
			t.Error("a public route must not document a 401")
		}
	})

	t.Run("secret rotation returns no content", func(t *testing.T) {
		item := document.Paths["/api/v1/app/:app_id/endpoint/:endpoint_id/secret/rotate"]
		if item == nil || item.Post == nil {
			t.Fatal("the rotate route is missing")
		}
		if _, ok := item.Post.Responses["204"]; !ok {
			t.Errorf("responses = %v, want a 204", responseCodes(item.Post))
		}
	})
}

func responseCodes(operation *openapi.Operation) []string {
	codes := make([]string, 0, len(operation.Responses))
	for code := range operation.Responses {
		codes = append(codes, code)
	}
	return codes
}
