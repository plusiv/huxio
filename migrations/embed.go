// Package migrations embeds the numbered SQL schema files and applies them
// with goose, so the binary carries its own schema. Installing the service is
// then "docker run plus a DSN" with no separate migration step to forget.
package migrations

import "embed"

// Migrations holds the embedded numbered SQL schema files, applied with goose.
//
//go:embed schema/*.sql
var Migrations embed.FS
