package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// apiKeyColumnNames reads the column set of apiKeys through PRAGMA.
func apiKeyColumnNames(t *testing.T, conn *sql.DB) map[string]bool {
	t.Helper()
	rows, err := conn.Query(`PRAGMA table_info(apiKeys)`)
	if err != nil {
		t.Fatalf("inspect apiKeys columns: %v", err)
	}
	defer rows.Close()
	var (
		cid, notNull, pk int
		name, colType    string
		dflt             sql.NullString
	)
	found := map[string]bool{}
	for rows.Next() {
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			t.Fatalf("scan apiKeys column: %v", err)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read apiKeys columns: %v", err)
	}
	return found
}

// TestApiKeyBudgetColumnsBackfilled proves a database created without the new
// governance columns gains them on the next startup, so an existing install
// upgrades in place instead of failing at the first query that reads them.
func TestApiKeyBudgetColumnsBackfilled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	conn, err := OpenDatabase(path)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer conn.Close()

	// A legacy apiKeys table: the pre-change shape, no budget columns.
	if _, err := conn.Exec(`CREATE TABLE apiKeys (
		id TEXT PRIMARY KEY,
		key TEXT UNIQUE NOT NULL,
		name TEXT,
		machineId TEXT,
		isActive INTEGER DEFAULT 1,
		createdAt TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("legacy apiKeys setup: %v", err)
	}

	if err := EnsureCoreSchema(conn); err != nil {
		t.Fatalf("EnsureCoreSchema: %v", err)
	}

	found := apiKeyColumnNames(t, conn)
	for _, want := range []string{
		"rateLimitRPM", "rateLimitTPM", "rateLimitConcurrency",
		"tokenQuota", "queueTimeoutMs",
	} {
		if !found[want] {
			t.Errorf("column %s not backfilled onto legacy apiKeys", want)
		}
	}
}

// TestEnsureCoreSchemaIsRerunnable proves running the bootstrap twice is safe:
// the second pass must not error (a duplicate-column ALTER would) and must not
// lose the data written between the two runs.
func TestEnsureCoreSchemaIsRerunnable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rerun.sqlite")
	conn, err := OpenDatabase(path)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer conn.Close()

	if err := EnsureCoreSchema(conn); err != nil {
		t.Fatalf("first EnsureCoreSchema: %v", err)
	}

	r := NewRepo(conn)
	if err := r.CreateApiKey("re1", "sk-rerun-abcdefghijklmnop", "rerun", "m"); err != nil {
		t.Fatalf("CreateApiKey: %v", err)
	}
	if err := r.SetApiKeyBudget("re1", 4242, 9090); err != nil {
		t.Fatalf("SetApiKeyBudget: %v", err)
	}

	// Second bootstrap, as a restart would do.
	if err := EnsureCoreSchema(conn); err != nil {
		t.Fatalf("second EnsureCoreSchema (must be a no-op): %v", err)
	}

	got, err := r.GetApiKeyByID("re1")
	if err != nil || got == nil {
		t.Fatalf("key lost across re-run: %v", err)
	}
	if got.TokenQuota == nil || *got.TokenQuota != 4242 {
		t.Errorf("tokenQuota after re-run = %v, want 4242", got.TokenQuota)
	}
	if got.QueueTimeoutMs == nil || *got.QueueTimeoutMs != 9090 {
		t.Errorf("queueTimeoutMs after re-run = %v, want 9090", got.QueueTimeoutMs)
	}
}

// TestSchemaDatabaseFileIsNotLeftBehind is a small hygiene guard: the file the
// test opened must be the one we inspect, so a stale shared temp file cannot
// make the column assertions pass for the wrong reason.
func TestSchemaDatabaseFileIsNotLeftBehind(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hygiene.sqlite")
	conn, err := OpenDatabase(path)
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	defer conn.Close()
	if err := EnsureCoreSchema(conn); err != nil {
		t.Fatalf("EnsureCoreSchema: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected the database file to exist at %s: %v", path, err)
	}
}
