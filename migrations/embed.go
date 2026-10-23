// Package migrations embeds numbered SQL migrations and applies them with goose.
package migrations

import "embed"

// Migrations holds the embedded numbered SQL schema files, applied with goose.
//
//go:embed schema/*.sql
var Migrations embed.FS
