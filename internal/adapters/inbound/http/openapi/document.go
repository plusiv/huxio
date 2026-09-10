package openapi

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
)

// Document is the generated OpenAPI document.
type Document struct {
	OpenAPI    string                `json:"openapi"`
	Info       Info                  `json:"info"`
	Servers    []Server              `json:"servers,omitempty"`
	Paths      map[string]*PathItem  `json:"paths"`
	Components Components            `json:"components"`
	Security   []map[string][]string `json:"security,omitempty"`
	Tags       []Tag                 `json:"tags,omitempty"`
}

// Info describes the API.
type Info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

// Server is one base URL the API is served from.
type Server struct {
	URL         string `json:"url"`
	Description string `json:"description,omitempty"`
}

// Tag groups operations.
type Tag struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// PathItem holds the operations for one path.
type PathItem struct {
	Get    *Operation `json:"get,omitempty"`
	Post   *Operation `json:"post,omitempty"`
	Put    *Operation `json:"put,omitempty"`
	Patch  *Operation `json:"patch,omitempty"`
	Delete *Operation `json:"delete,omitempty"`
}

// Operation is one method on one path.
type Operation struct {
	OperationID string                `json:"operationId,omitempty"`
	Summary     string                `json:"summary,omitempty"`
	Description string                `json:"description,omitempty"`
	Tags        []string              `json:"tags,omitempty"`
	Parameters  []Parameter           `json:"parameters,omitempty"`
	RequestBody *RequestBody          `json:"requestBody,omitempty"`
	Responses   map[string]*Response  `json:"responses"`
	Security    []map[string][]string `json:"security,omitempty"`
}

// Parameter is a path, query or header parameter.
type Parameter struct {
	Name        string  `json:"name"`
	In          string  `json:"in"`
	Required    bool    `json:"required,omitempty"`
	Description string  `json:"description,omitempty"`
	Schema      *Schema `json:"schema,omitempty"`
}

// RequestBody describes an operation's body.
type RequestBody struct {
	Required bool                 `json:"required,omitempty"`
	Content  map[string]MediaType `json:"content"`
}

// Response is one documented status code.
type Response struct {
	Description string               `json:"description"`
	Content     map[string]MediaType `json:"content,omitempty"`
}

// MediaType pairs a content type with its schema.
type MediaType struct {
	Schema *Schema `json:"schema,omitempty"`
}

// Components holds the reusable pieces.
type Components struct {
	Schemas         map[string]*Schema        `json:"schemas,omitempty"`
	SecuritySchemes map[string]SecurityScheme `json:"securitySchemes,omitempty"`
}

// SecurityScheme describes how requests authenticate.
type SecurityScheme struct {
	Type         string `json:"type"`
	Scheme       string `json:"scheme,omitempty"`
	BearerFormat string `json:"bearerFormat,omitempty"`
	Description  string `json:"description,omitempty"`
}

// Route is one registered route, as the router reports it.
type Route struct {
	Method string
	Path   string
}

// RouteDoc is the documentation attached to one route. Handlers own these,
// because they own the payload types.
type RouteDoc struct {
	Summary     string
	Description string
	Tags        []string
	// Request and Response are the payload types, reflected into schemas.
	Request  reflect.Type
	Response reflect.Type
	// SuccessStatus is the status code the response schema documents.
	SuccessStatus string
	// Query lists the query parameters the handler reads.
	Query []Parameter
	// Public marks a route that needs no bearer token.
	Public bool
}

// Documentable reports whether a registered route belongs in the document.
// Echo auto-registers catch-all routes for every group, and the metrics
// endpoint speaks the Prometheus text format rather than JSON.
func Documentable(route Route) bool {
	switch {
	case route.Method == "", route.Method == "echo_route_not_found":
		return false
	case strings.Contains(route.Path, "*"):
		return false
	case strings.HasPrefix(route.Path, "/metrics"):
		return false
	case strings.HasPrefix(route.Path, "/portal"):
		// The portal is a browser surface with its own session cookie, not part of
		// the JSON API the SDKs speak.
		return false
	default:
		return true
	}
}

// Build assembles the document from the registered routes and their docs.
// A route with no entry in docs still appears: an undocumented endpoint is
// better than a missing one, and the gap is obvious.
func Build(info Info, servers []Server, routes []Route, docs map[string]RouteDoc) *Document {
	document := &Document{
		OpenAPI: "3.0.3",
		Info:    info,
		Servers: servers,
		Paths:   map[string]*PathItem{},
		Components: Components{
			Schemas: map[string]*Schema{
				"Error": SchemaFor(reflect.TypeOf(errorSchema{})),
			},
			SecuritySchemes: map[string]SecurityScheme{
				"bearerAuth": {
					Type:         "http",
					Scheme:       "bearer",
					BearerFormat: "JWT",
					Description:  "An organization token, or an application-scoped portal token.",
				},
			},
		},
		Security: []map[string][]string{{"bearerAuth": {}}},
	}

	tags := map[string]struct{}{}

	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Path != routes[j].Path {
			return routes[i].Path < routes[j].Path
		}
		return routes[i].Method < routes[j].Method
	})

	for _, route := range routes {
		if !Documentable(route) {
			continue
		}

		key := route.Method + " " + route.Path
		doc := docs[key]
		operation := buildOperation(route, doc)

		for _, tag := range doc.Tags {
			tags[tag] = struct{}{}
		}

		item := document.Paths[route.Path]
		if item == nil {
			item = &PathItem{}
			document.Paths[route.Path] = item
		}
		switch route.Method {
		case "GET":
			item.Get = operation
		case "POST":
			item.Post = operation
		case "PUT":
			item.Put = operation
		case "PATCH":
			item.Patch = operation
		case "DELETE":
			item.Delete = operation
		}
	}

	for tag := range tags {
		document.Tags = append(document.Tags, Tag{Name: tag})
	}
	sort.Slice(document.Tags, func(i, j int) bool { return document.Tags[i].Name < document.Tags[j].Name })

	return document
}

// errorSchema mirrors the error envelope every failure uses.
type errorSchema struct {
	Code             string `json:"code"`
	Detail           string `json:"detail"`
	ValidationErrors []struct {
		Loc  []string `json:"loc"`
		Msg  string   `json:"msg"`
		Type string   `json:"type"`
	} `json:"validationErrors,omitempty"`
}

func buildOperation(route Route, doc RouteDoc) *Operation {
	operation := &Operation{
		OperationID: operationID(route),
		Summary:     doc.Summary,
		Description: doc.Description,
		Tags:        doc.Tags,
		Parameters:  append(pathParameters(route.Path), doc.Query...),
		Responses:   map[string]*Response{},
	}

	if doc.Public {
		// An empty security list means "no authentication required" and overrides
		// the document-level default.
		operation.Security = []map[string][]string{}
	}

	if doc.Request != nil {
		operation.RequestBody = &RequestBody{
			Required: true,
			Content:  map[string]MediaType{"application/json": {Schema: SchemaFor(doc.Request)}},
		}
	}

	status := doc.SuccessStatus
	if status == "" {
		status = "200"
	}
	success := &Response{Description: "Success"}
	if doc.Response != nil {
		success.Content = map[string]MediaType{"application/json": {Schema: SchemaFor(doc.Response)}}
	}
	if status == "204" {
		success.Description = "No content"
	}
	operation.Responses[status] = success

	// Every authenticated route can produce these, and saying so once here
	// beats repeating it per route.
	if !doc.Public {
		operation.Responses["401"] = errorResponse("Missing or invalid token")
		operation.Responses["403"] = errorResponse("The token does not have access to this resource")
		operation.Responses["429"] = errorResponse("Rate limit exceeded")
	}
	if doc.Request != nil {
		operation.Responses["422"] = errorResponse("Validation failed")
	}
	if len(pathParameters(route.Path)) > 0 {
		operation.Responses["404"] = errorResponse("Not found")
	}
	operation.Responses["500"] = errorResponse("Internal error")

	return operation
}

func errorResponse(description string) *Response {
	return &Response{
		Description: description,
		Content: map[string]MediaType{
			"application/json": {Schema: &Schema{Ref: "#/components/schemas/Error"}},
		},
	}
}

// pathParameters derives the parameters from the route template, so a new path
// segment cannot be forgotten in the docs.
func pathParameters(path string) []Parameter {
	var parameters []Parameter
	for _, segment := range strings.Split(path, "/") {
		if !strings.HasPrefix(segment, ":") {
			continue
		}
		name := strings.TrimPrefix(segment, ":")
		parameters = append(parameters, Parameter{
			Name:        name,
			In:          "path",
			Required:    true,
			Description: pathParameterDescription(name),
			Schema:      &Schema{Type: "string"},
		})
	}
	return parameters
}

func pathParameterDescription(name string) string {
	switch name {
	case "app_id":
		return "Application id, or the tenant-assigned uid."
	case "endpoint_id":
		return "Endpoint id, or the tenant-assigned uid."
	case "msg_id":
		return "Message id."
	case "event_type_name":
		return "Event type name."
	default:
		return ""
	}
}

// operationID builds a stable, readable id from the method and path.
func operationID(route Route) string {
	parts := []string{strings.ToLower(route.Method)}
	for _, segment := range strings.Split(strings.Trim(route.Path, "/"), "/") {
		if segment == "" || segment == "api" || segment == "v1" {
			continue
		}
		segment = strings.TrimPrefix(segment, ":")
		segment = strings.ReplaceAll(segment, "-", "_")
		segment = strings.ReplaceAll(segment, ".", "_")
		parts = append(parts, segment)
	}
	return strings.Join(parts, "_")
}

// JSON renders the document.
func (d *Document) JSON() ([]byte, error) { return json.MarshalIndent(d, "", "  ") }
