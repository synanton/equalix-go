//go:build differential

package differential

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeWorkload(t *testing.T, lines ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "wl.jsonl")
	if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	p := writeWorkload(t,
		`{"id":"a","tenant":"t1","weight":1,"created_at_offset_ms":0,"submitted_at_offset_ms":0,"payload_bytes":16}`,
		`{"id":"b","tenant":"t2","weight":2,"created_at_offset_ms":5,"submitted_at_offset_ms":10,"payload_bytes":16}`,
	)
	ts, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 2 || ts[1].Tenant != "t2" {
		t.Fatalf("tasks = %+v", ts)
	}
}

func TestLoadRejects(t *testing.T) {
	cases := map[string]string{
		"negative created offset": `{"id":"a","tenant":"t","weight":1,"created_at_offset_ms":-1,"submitted_at_offset_ms":0}`,
		"negative submit offset":  `{"id":"a","tenant":"t","weight":1,"created_at_offset_ms":0,"submitted_at_offset_ms":-1}`,
		"empty id":                `{"id":"","tenant":"t","weight":1,"created_at_offset_ms":0,"submitted_at_offset_ms":0}`,
		"empty tenant":            `{"id":"a","tenant":"","weight":1,"created_at_offset_ms":0,"submitted_at_offset_ms":0}`,
		"zero weight":             `{"id":"a","tenant":"t","weight":0,"created_at_offset_ms":0,"submitted_at_offset_ms":0}`,
		"decreasing submit": `{"id":"a","tenant":"t","weight":1,"created_at_offset_ms":0,"submitted_at_offset_ms":10}` + "\n" +
			`{"id":"b","tenant":"t","weight":1,"created_at_offset_ms":0,"submitted_at_offset_ms":5}`,
		"empty file": ``,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			p := writeWorkload(t, strings.Split(body, "\n")...)
			if _, err := Load(p); err == nil {
				t.Fatal("expected rejection, got nil error")
			}
		})
	}
	// Duplicate IDs reject even when each line is otherwise valid.
	p := writeWorkload(t,
		`{"id":"a","tenant":"t","weight":1,"created_at_offset_ms":0,"submitted_at_offset_ms":0}`,
		`{"id":"a","tenant":"t","weight":1,"created_at_offset_ms":0,"submitted_at_offset_ms":0}`,
	)
	if _, err := Load(p); err == nil {
		t.Fatal("expected duplicate-id rejection")
	}
}
