// Package binder holds the custom Echo binder: it binds the request and
// validates struct tags in one step, so validation happens at the transport
// boundary and never leaks into a use case.
package binder

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v5"
)

// ValidatingBinder binds a request and then validates it. Binding failures are
// 400 (the client sent something unparseable); validation failures are 422
// (parseable, but not acceptable).
type ValidatingBinder struct {
	defaultBinder echo.DefaultBinder
	validate      *validator.Validate
}

// New returns a binder ready to register on an Echo instance.
func New() *ValidatingBinder {
	return &ValidatingBinder{validate: validator.New(validator.WithRequiredStructEnabled())}
}

// Bind implements echo.Binder.
func (vb *ValidatingBinder) Bind(c *echo.Context, target any) error {
	if err := vb.defaultBinder.Bind(c, target); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request body")
	}
	if err := vb.validate.Struct(target); err != nil {
		var validationErrors validator.ValidationErrors
		if errors.As(err, &validationErrors) && len(validationErrors) > 0 {
			first := validationErrors[0]
			return echo.NewHTTPError(http.StatusUnprocessableEntity,
				fmt.Sprintf("field '%s' failed validation: %s", first.Field(), first.Tag()))
		}
		return echo.NewHTTPError(http.StatusUnprocessableEntity, "validation failed")
	}
	return nil
}
