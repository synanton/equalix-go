// Package migrations embeds the goose SQL files so the service can
// migrate on startup from a single artifact (no volume mount). The .sql
// files stay the canonical migration source — reviewed as SQL, replayed
// in Java-V order; this package only carries them into the binary.
// External operators who want file-based control pass --migrations-dir
// and the runner reads the same filenames from disk instead.
package migrations

import "embed"

// FS carries 00001–00005 in filename order (goose applies sorted).
//
//go:embed *.sql
var FS embed.FS
