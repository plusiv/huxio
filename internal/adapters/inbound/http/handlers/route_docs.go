package handlers

import (
	"reflect"

	"github.com/plusiv/huxio/internal/adapters/inbound/http/openapi"
)

// paginationQuery is the cursor pagination every list route accepts.
var paginationQuery = []openapi.Parameter{
	{
		Name: "limit", In: "query",
		Description: "Page size, 1-100. Defaults to 20.",
		Schema:      &openapi.Schema{Type: "integer"},
	},
	{
		Name: "iterator", In: "query",
		Description: "Opaque cursor from a previous page's iterator field.",
		Schema:      &openapi.Schema{Type: "string"},
	},
}

// attemptFilterQuery is the shared attempt filter set.
var attemptFilterQuery = append([]openapi.Parameter{
	{
		Name: "status", In: "query",
		Description: "success, pending or fail (numeric forms also accepted).",
		Schema:      &openapi.Schema{Type: "string", Enum: []string{"success", "pending", "fail"}},
	},
	{
		Name: "statusCodeClass", In: "query",
		Description: "HTTP status class, e.g. 4 matches 400-499.",
		Schema:      &openapi.Schema{Type: "integer"},
	},
	{
		Name: "channel", In: "query",
		Schema: &openapi.Schema{Type: "string"},
	},
	{
		Name: "before", In: "query",
		Description: "Only attempts before this RFC 3339 timestamp.",
		Schema:      &openapi.Schema{Type: "string", Format: "date-time"},
	},
	{
		Name: "after", In: "query",
		Description: "Only attempts at or after this RFC 3339 timestamp.",
		Schema:      &openapi.Schema{Type: "string", Format: "date-time"},
	},
}, paginationQuery...)

// RouteDocs describes every route, keyed by "METHOD /path". It lives in this
// package because the payload types do: the spec is reflected off the same
// structs the binder validates, so it cannot drift from what the API accepts.
func RouteDocs() map[string]openapi.RouteDoc {
	return map[string]openapi.RouteDoc{
		"GET /api/v1/health": {
			Summary: "Liveness", Tags: []string{"admin"}, Public: true,
			Description: "Reports only that the process is alive.",
			Response:    reflect.TypeOf(healthResponse{}),
		},
		"GET /api/v1/health/ready": {
			Summary: "Readiness", Tags: []string{"admin"}, Public: true,
			Description: "Fails while the database is unreachable, the config snapshot is stale, or the process is draining.",
			Response:    reflect.TypeOf(readinessResponse{}),
		},

		"POST /api/v1/app": {
			Summary: "Create an application", Tags: []string{"application"},
			Request: reflect.TypeOf(createApplicationRequest{}), Response: reflect.TypeOf(applicationResponse{}),
			SuccessStatus: "201",
		},
		"GET /api/v1/app": {
			Summary: "List applications", Tags: []string{"application"},
			Response: reflect.TypeOf(CursorResponse[applicationResponse]{}),
			Query: append([]openapi.Parameter{{
				Name: "search", In: "query", Schema: &openapi.Schema{Type: "string"},
			}}, paginationQuery...),
		},
		"GET /api/v1/app/:app_id": {
			Summary: "Get an application", Tags: []string{"application"},
			Response: reflect.TypeOf(applicationResponse{}),
		},
		"PUT /api/v1/app/:app_id": {
			Summary: "Replace an application", Tags: []string{"application"},
			Request: reflect.TypeOf(updateApplicationRequest{}), Response: reflect.TypeOf(applicationResponse{}),
		},
		"PATCH /api/v1/app/:app_id": {
			Summary: "Update an application", Tags: []string{"application"},
			Request: reflect.TypeOf(updateApplicationRequest{}), Response: reflect.TypeOf(applicationResponse{}),
		},
		"DELETE /api/v1/app/:app_id": {
			Summary: "Delete an application", Tags: []string{"application"},
			SuccessStatus: "204",
		},

		"POST /api/v1/app/:app_id/msg": {
			Summary: "Send a message", Tags: []string{"message"},
			Description: "Accepted only once the message is committed. Send an Idempotency-Key header to make retries safe.",
			Request:     reflect.TypeOf(createMessageRequest{}), Response: reflect.TypeOf(messageResponse{}),
			SuccessStatus: "202",
		},
		"GET /api/v1/app/:app_id/msg": {
			Summary: "List messages", Tags: []string{"message"},
			Description: "Payloads are omitted from listings.",
			Response:    reflect.TypeOf(CursorResponse[messageResponse]{}),
			Query: append([]openapi.Parameter{
				{Name: "eventTypes", In: "query", Schema: &openapi.Schema{Type: "array", Items: &openapi.Schema{Type: "string"}}},
				{Name: "channel", In: "query", Schema: &openapi.Schema{Type: "string"}},
				{Name: "before", In: "query", Schema: &openapi.Schema{Type: "string", Format: "date-time"}},
				{Name: "after", In: "query", Schema: &openapi.Schema{Type: "string", Format: "date-time"}},
			}, paginationQuery...),
		},
		"GET /api/v1/app/:app_id/msg/:msg_id": {
			Summary: "Get a message", Tags: []string{"message"},
			Response: reflect.TypeOf(messageResponse{}),
		},

		"POST /api/v1/app/:app_id/endpoint": {
			Summary: "Create an endpoint", Tags: []string{"endpoint"},
			Description: "The signing secret is returned once, here.",
			Request:     reflect.TypeOf(createEndpointRequest{}), Response: reflect.TypeOf(endpointCreatedResponse{}),
			SuccessStatus: "201",
		},
		"GET /api/v1/app/:app_id/endpoint": {
			Summary: "List endpoints", Tags: []string{"endpoint"},
			Response: reflect.TypeOf(CursorResponse[endpointResponse]{}), Query: paginationQuery,
		},
		"GET /api/v1/app/:app_id/endpoint/:endpoint_id": {
			Summary: "Get an endpoint", Tags: []string{"endpoint"},
			Response: reflect.TypeOf(endpointResponse{}),
		},
		"PUT /api/v1/app/:app_id/endpoint/:endpoint_id": {
			Summary: "Replace an endpoint", Tags: []string{"endpoint"},
			Request: reflect.TypeOf(updateEndpointRequest{}), Response: reflect.TypeOf(endpointResponse{}),
		},
		"PATCH /api/v1/app/:app_id/endpoint/:endpoint_id": {
			Summary: "Update an endpoint", Tags: []string{"endpoint"},
			Request: reflect.TypeOf(updateEndpointRequest{}), Response: reflect.TypeOf(endpointResponse{}),
		},
		"DELETE /api/v1/app/:app_id/endpoint/:endpoint_id": {
			Summary: "Delete an endpoint", Tags: []string{"endpoint"}, SuccessStatus: "204",
		},
		"GET /api/v1/app/:app_id/endpoint/:endpoint_id/secret": {
			Summary: "Reveal the signing secret", Tags: []string{"endpoint"},
			Response: reflect.TypeOf(secretResponse{}),
		},
		"POST /api/v1/app/:app_id/endpoint/:endpoint_id/secret/rotate": {
			Summary: "Rotate the signing secret", Tags: []string{"endpoint"},
			Description: "The previous secret keeps signing for 24 hours, so receivers can roll over without downtime.",
			Request:     reflect.TypeOf(rotateSecretRequest{}), SuccessStatus: "204",
		},
		"GET /api/v1/app/:app_id/endpoint/:endpoint_id/headers": {
			Summary: "Get custom headers", Tags: []string{"endpoint"},
			Response: reflect.TypeOf(headersResponse{}),
		},
		"PATCH /api/v1/app/:app_id/endpoint/:endpoint_id/headers": {
			Summary: "Patch custom headers", Tags: []string{"endpoint"},
			Description: "An empty value removes a header. Signature headers may not be overridden.",
			Request:     reflect.TypeOf(patchHeadersRequest{}), Response: reflect.TypeOf(headersResponse{}),
		},
		"GET /api/v1/app/:app_id/endpoint/:endpoint_id/stats": {
			Summary: "Endpoint delivery stats", Tags: []string{"endpoint"},
			Response: reflect.TypeOf(struct {
				Success int64 `json:"success"`
				Pending int64 `json:"pending"`
				Fail    int64 `json:"fail"`
			}{}),
			Query: []openapi.Parameter{{
				Name: "since", In: "query",
				Schema: &openapi.Schema{Type: "string", Format: "date-time"},
			}},
		},
		"POST /api/v1/app/:app_id/endpoint/:endpoint_id/recover": {
			Summary: "Replay failed deliveries to an endpoint", Tags: []string{"endpoint"},
			Request: reflect.TypeOf(recoverRequest{}), Response: reflect.TypeOf(replayResponse{}),
			SuccessStatus: "202",
		},

		"GET /api/v1/app/:app_id/attempt/msg/:msg_id": {
			Summary: "Attempts for a message", Tags: []string{"attempt"},
			Response: reflect.TypeOf(CursorResponse[attemptResponse]{}), Query: attemptFilterQuery,
		},
		"GET /api/v1/app/:app_id/attempt/endpoint/:endpoint_id": {
			Summary: "Attempts for an endpoint", Tags: []string{"attempt"},
			Response: reflect.TypeOf(CursorResponse[attemptResponse]{}), Query: attemptFilterQuery,
		},
		"GET /api/v1/app/:app_id/msg/:msg_id/endpoint/:endpoint_id/attempt": {
			Summary: "Attempts for one message and endpoint", Tags: []string{"attempt"},
			Response: reflect.TypeOf(CursorResponse[attemptResponse]{}), Query: attemptFilterQuery,
		},
		"POST /api/v1/app/:app_id/msg/:msg_id/endpoint/:endpoint_id/resend": {
			Summary: "Resend a message to an endpoint", Tags: []string{"attempt"},
			Description:   "One attempt, not retried on failure.",
			SuccessStatus: "202",
		},
		"GET /api/v1/app/:app_id/attempt/stream": {
			Summary: "Live tail of delivery attempts", Tags: []string{"attempt"},
			Description: "Server-sent events. Each event carries one attempt in the same shape as the listing.",
			Query:       attemptFilterQuery,
		},
		"POST /api/v1/app/:app_id/replay": {
			Summary: "Bulk replay by filter", Tags: []string{"attempt"},
			Request: reflect.TypeOf(replayRequest{}), Response: reflect.TypeOf(replayResponse{}),
			SuccessStatus: "202",
		},
		"POST /api/v1/app/:app_id/portal-access": {
			Summary: "Mint a portal token", Tags: []string{"portal"},
			Description: "Short-lived and scoped to one application. Mint it in your backend, never in a browser.",
			Request:     reflect.TypeOf(portalAccessRequest{}),
			Response: reflect.TypeOf(struct {
				URL    string `json:"url"`
				Token  string `json:"token"`
				Expiry string `json:"expiry"`
			}{}),
		},

		"POST /api/v1/event-type": {
			Summary: "Create an event type", Tags: []string{"event-type"},
			Request: reflect.TypeOf(createEventTypeRequest{}), Response: reflect.TypeOf(eventTypeResponse{}),
			SuccessStatus: "201",
		},
		"GET /api/v1/event-type": {
			Summary: "List event types", Tags: []string{"event-type"},
			Response: reflect.TypeOf(CursorResponse[eventTypeResponse]{}),
			Query: append([]openapi.Parameter{{
				Name: "includeArchived", In: "query", Schema: &openapi.Schema{Type: "boolean"},
			}}, paginationQuery...),
		},
		"GET /api/v1/event-type/:event_type_name": {
			Summary: "Get an event type", Tags: []string{"event-type"},
			Response: reflect.TypeOf(eventTypeResponse{}),
		},
		"PUT /api/v1/event-type/:event_type_name": {
			Summary: "Update an event type", Tags: []string{"event-type"},
			Request: reflect.TypeOf(updateEventTypeRequest{}), Response: reflect.TypeOf(eventTypeResponse{}),
		},
		"DELETE /api/v1/event-type/:event_type_name": {
			Summary: "Archive an event type", Tags: []string{"event-type"},
			Description:   "Always a soft delete: historical messages reference it by name.",
			SuccessStatus: "204",
		},

		"POST /api/v1/admin/rescue-stuck": {
			Summary: "Force the stuck-task sweep", Tags: []string{"admin"},
			Response: reflect.TypeOf(rescueResponse{}),
		},
		"GET /api/v1/admin/queue": {
			Summary: "Queue depth and lag", Tags: []string{"admin"},
			Response: reflect.TypeOf([]queueStatsResponse{}),
			Query: []openapi.Parameter{{
				Name: "pool", In: "query", Schema: &openapi.Schema{Type: "string"},
			}},
		},
		"GET /api/v1/admin/partitions": {
			Summary: "Partition lease table", Tags: []string{"admin"},
			Response: reflect.TypeOf([]partitionLeaseResponse{}),
			Query: []openapi.Parameter{{
				Name: "pool", In: "query", Schema: &openapi.Schema{Type: "string"},
			}},
		},
		"POST /api/v1/admin/pool": {
			Summary: "Move an endpoint between worker pools", Tags: []string{"admin"},
			Request: reflect.TypeOf(poolAssignmentRequest{}), SuccessStatus: "204",
		},
		"POST /api/v1/admin/endpoint/enable": {
			Summary: "Re-enable a disabled endpoint", Tags: []string{"admin"},
			Request: reflect.TypeOf(reEnableRequest{}), SuccessStatus: "204",
		},
	}
}
