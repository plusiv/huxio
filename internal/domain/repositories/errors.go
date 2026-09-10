package repositories

import "github.com/rotisserie/eris"

// ErrNotFound is returned by a repository when the requested record does not
// exist. Implementations wrap driver-level "no rows" errors with this sentinel
// so the domain and application layers stay free of infrastructure details.
var ErrNotFound = eris.New("record not found")

// ErrConflict is returned when a write violates a uniqueness constraint.
var ErrConflict = eris.New("record already exists")
