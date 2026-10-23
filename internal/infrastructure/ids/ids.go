// Package ids generates and parses prefixed KSUID identifiers. Their embedded
// timestamps have one-second resolution; ids within a second are randomly ordered.
package ids

import (
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/segmentio/ksuid"
)

// Prefix is the human-readable tag that precedes the KSUID body.
type Prefix string

// The identifier prefixes for every entity that carries a generated id.
const (
	PrefixOrganization Prefix = "org"
	PrefixApplication  Prefix = "app"
	PrefixEndpoint     Prefix = "ep"
	PrefixMessage      Prefix = "msg"
	PrefixAttempt      Prefix = "atmpt"
	PrefixWorker       Prefix = "wkr"
)

// ErrMalformed is returned when a value is not a prefixed KSUID.
var ErrMalformed = eris.New("malformed identifier")

// New returns a freshly generated identifier for the supplied prefix.
func New(p Prefix) string {
	return string(p) + "_" + ksuid.New().String()
}

// NewAt returns an identifier whose embedded timestamp is at.
func NewAt(p Prefix, at time.Time) (string, error) {
	id, err := ksuid.NewRandomWithTime(at)
	if err != nil {
		return "", eris.Wrap(err, "generate ksuid")
	}
	return string(p) + "_" + id.String(), nil
}

// Split separates a prefixed identifier into its prefix and KSUID body.
func Split(id string) (Prefix, ksuid.KSUID, error) {
	prefix, body, found := strings.Cut(id, "_")
	if !found || prefix == "" {
		return "", ksuid.Nil, ErrMalformed
	}
	parsed, err := ksuid.Parse(body)
	if err != nil {
		return "", ksuid.Nil, ErrMalformed
	}
	return Prefix(prefix), parsed, nil
}

// HasKindPrefix reports whether id is tagged with the supplied prefix, without
// validating the body. Use it to check what kind of thing an identifier names
// (an organization, an application) when the id may have been minted
// elsewhere, such as inside a token issued by another system.
func HasKindPrefix(id string, p Prefix) bool {
	return strings.HasPrefix(id, string(p)+"_") && len(id) > len(p)+1
}

// HasPrefix reports whether id carries the supplied prefix and a well-formed
// KSUID body.
func HasPrefix(id string, p Prefix) bool {
	prefix, _, err := Split(id)
	return err == nil && prefix == p
}

// TimeOf returns the second-resolution timestamp embedded in a prefixed KSUID.
// Messages use it as created_at, allowing their partition to be derived from
// the id. Attempts store a separate, higher-resolution timestamp.
func TimeOf(id string) (time.Time, error) {
	_, parsed, err := Split(id)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.Time().UTC(), nil
}
