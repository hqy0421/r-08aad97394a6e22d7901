package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/burnside-project/burnside-go/manifest"
)

// queryTimeout is the per-statement budget for DuckDB invocations. Generous
// for a single-user laptop tool; tighten if abuse becomes a concern.
const queryTimeout = 15 * time.Second

func (s *server) query(ctx context.Context, args json.RawMessage) map[string]any {
	var a struct {
		SQL               string `json:"sql"`
		Limit             int    `json:"limit"`
		MinPublishedEpoch int64  `json:"min_published_epoch"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return errorContent(fmt.Errorf("invalid arguments: %w", err))
	}
	if a.Limit <= 0 || a.Limit > 1000 {
		a.Limit = 100
	}
	if err := validateSelect(a.SQL); err != nil {
		return errorContent(err)
	}

	m, err := s.sink.ReadManifest(ctx)
	if err != nil {
		return errorContent(fmt.Errorf("read manifest: %w (run `pg-cdc init` first?)", err))
	}

	// Enforce the requested minimum published epoch against every table the
	// statement references. Run this before the DuckDB readiness probe: the
	// check is a cheap manifest read, and a caller that asked for a watermark
	// we can't serve should hear why regardless of whether DuckDB is present.
	if a.MinPublishedEpoch > 0 {
		if gate := s.gateQueryTables(ctx, m, a.SQL, a.MinPublishedEpoch); !gate.OK {
			return textContent(gate.Message)
		}
	}

	if err := s.duckdbReady(); err != nil {
		return errorContent(err)
	}

	setup := buildViewDDL(m, s.cfg.Storage.Path)
	stmt := setup + strings.TrimRight(strings.TrimSpace(a.SQL), ";")

	rows, raw, err := runDuckDB(ctx, stmt)
	if err != nil {
		return errorContent(err)
	}
	return formatResult(rows, raw, a.Limit)
}

// gateQueryTables checks the minimum published epoch for every active table
// whose name appears in the SQL text. Name matching is intentionally simple
// (substring on the qualified name and its bare table component): the goal is
// to fail closed on the common case, not to parse SQL.
func (s *server) gateQueryTables(ctx context.Context, m *manifest.Manifest, sql string, min int64) gateResult {
	lower := strings.ToLower(sql)
	var offending []string
	var messages []string
	names := make([]string, 0, len(m.ActiveTables()))
	for name := range m.ActiveTables() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !referencesTable(lower, name) {
			continue
		}
		gate := s.checkMinPublished(ctx, m, name, min)
		if gate.OK {
			continue
		}
		offending = append(offending, name)
		messages = append(messages, gate.Message)
	}
	if len(offending) == 0 {
		return gateResult{OK: true}
	}
	if len(offending) == 1 {
		return gateResult{OK: false, Message: messages[0]}
	}
	return gateResult{
		OK: false,
		Message: fmt.Sprintf(
			"%d table(s) referenced by the query are below min_published_epoch=%d: %s.\n\n%s",
			len(offending), min, strings.Join(offending, ", "), strings.Join(messages, "\n")),
	}
}

// referencesTable reports whether the lowercased SQL plausibly references the
// given qualified table name. It matches the fully-qualified form and the bare
// table name bounded by non-identifier characters.
func referencesTable(lowerSQL, qualified string) bool {
	q := strings.ToLower(qualified)
	if strings.Contains(lowerSQL, q) {
		return true
	}
	bare := q
	if idx := strings.LastIndexByte(q, '.'); idx >= 0 {
		bare = q[idx+1:]
	}
	for _, field := range identifierFields(lowerSQL) {
		if field == bare {
			return true
		}
	}
	return false
}

// identifierFields splits SQL into identifier-ish tokens so a bare table name
// like "orders" matches "FROM orders" but not "FROM backorders".
func identifierFields(sql string) []string {
	return strings.FieldsFunc(sql, func(r rune) bool {
		return !(r == '_' || r == '.' ||
			(r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'))
	})
}

func (s *server) recentChanges(ctx context.Context, args json.RawMessage) map[string]any {
	var a struct {
		Name              string `json:"name"`
		Limit             int    `json:"limit"`
		MinPublishedEpoch int64  `json:"min_published_epoch"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return errorContent(fmt.Errorf("invalid arguments: %w", err))
	}
	if a.Name == "" {
		return errorContent(fmt.Errorf("name is required"))
	}
	if a.Limit <= 0 || a.Limit > 200 {
		a.Limit = 20
	}

	m, err := s.sink.ReadManifest(ctx)
	if err != nil {
		return errorContent(fmt.Errorf("read manifest: %w", err))
	}
	t, ok := m.Tables[a.Name]
	if !ok {
		return errorContent(fmt.Errorf("table not found in manifest: %s", a.Name))
	}

	if a.MinPublishedEpoch > 0 {
		if gate := s.checkMinPublished(ctx, m, a.Name, a.MinPublishedEpoch); !gate.OK {
			return textContent(gate.Message)
		}
	}

	if err := s.duckdbReady(); err != nil {
		return errorContent(err)
	}

	deltaGlob := filepath.Join(s.cfg.Storage.Path, t.Path, "deltas", "*.parquet")
	stmt := fmt.Sprintf("SELECT * FROM read_parquet(%s, union_by_name=true) LIMIT %d",
		quoteSQL(deltaGlob), a.Limit)

	rows, raw, err := runDuckDB(ctx, stmt)
	if err != nil {
		// Most common failure: no deltas yet — tell the user plainly.
		if strings.Contains(err.Error(), "No files found") || strings.Contains(err.Error(), "IO Error") {
			return textContent(fmt.Sprintf("no streaming changes recorded yet for %s (deltas dir empty)", a.Name))
		}
		return errorContent(err)
	}
	return formatResult(rows, raw, a.Limit)
}

// duckdbReady gates the query tools on (a) filesystem sink and (b) duckdb on PATH.
// Both are open-core scaffold limitations; the commercial edition handles S3/GCS
// and may embed DuckDB.
func (s *server) duckdbReady() error {
	if s.cfg.Storage.StorageType() != "filesystem" {
		return fmt.Errorf("query tools currently support only filesystem storage in open core (configured: %s)", s.cfg.Storage.StorageType())
	}
	if _, err := exec.LookPath("duckdb"); err != nil {
		return errors.New("duckdb CLI not found on PATH — install with `brew install duckdb` (macOS) or see duckdb.org/docs/installation/ (other platforms) to enable the query tools")
	}
	return nil
}

// validateSelect rejects anything that isn't a read-only SELECT or WITH … SELECT.
// This is belt-and-suspenders — DuckDB is also reading Parquet files via globs,
// so there's no live database to mutate, but we still keep the surface tight.
func validateSelect(sql string) error {
	s := strings.TrimSpace(strings.ToLower(sql))
	if s == "" {
		return fmt.Errorf("sql is required")
	}
	s = strings.TrimRight(s, ";")
	if !strings.HasPrefix(s, "select") && !strings.HasPrefix(s, "with") {
		return fmt.Errorf("only SELECT (or WITH … SELECT) statements are allowed")
	}
	for _, kw := range []string{"insert ", "update ", "delete ", "drop ", "alter ", "create ", "attach ", "copy ", "pragma ", "load "} {
		if strings.Contains(s, kw) {
			return fmt.Errorf("query may not contain: %s", strings.TrimSpace(kw))
		}
	}
	return nil
}

// buildViewDDL emits CREATE VIEW statements that map manifest tables to the
// Parquet files on disk. A table named "public.orders" becomes "public.orders"
// in DuckDB — same name as in Postgres, so SQL generated by an LLM looks natural.
func buildViewDDL(m *manifest.Manifest, basePath string) string {
	var b strings.Builder
	seen := map[string]bool{}
	for name, t := range m.ActiveTables() {
		schema, table := splitQualified(name)
		if schema != "main" && !seen[schema] {
			fmt.Fprintf(&b, "CREATE SCHEMA IF NOT EXISTS %s; ", quoteIdent(schema))
			seen[schema] = true
		}
		// Glob both base/ and deltas/ — the LLM doesn't need to know about epochs.
		glob := filepath.Join(basePath, t.Path, "**", "*.parquet")
		fmt.Fprintf(&b, "CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM read_parquet(%s, union_by_name=true); ",
			quoteIdent(schema), quoteIdent(table), quoteSQL(glob))
	}
	return b.String()
}

func splitQualified(name string) (schema, table string) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "main", parts[0]
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteSQL(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// runDuckDB executes the given SQL via the DuckDB CLI in JSON mode. Returns
// the parsed row array on success, or the raw stdout if it isn't a JSON array
// (e.g. for statements that return no rowset).
func runDuckDB(ctx context.Context, sql string) (rows []map[string]any, raw string, err error) {
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// No -readonly: it requires an explicit DB file, and we use in-memory.
	// Read-only semantics are enforced by validateSelect upstream.
	cmd := exec.CommandContext(qctx, "duckdb", "-json", "-c", sql)
	stdout, runErr := cmd.Output()
	if runErr != nil {
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			return nil, "", fmt.Errorf("duckdb: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, "", fmt.Errorf("run duckdb: %w", runErr)
	}
	out := strings.TrimSpace(string(stdout))
	if out == "" {
		return nil, "", nil
	}
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		// Not a JSON array — return as text (rare with -json, but tolerate).
		return nil, out, nil
	}
	return rows, "", nil
}

func formatResult(rows []map[string]any, raw string, limit int) map[string]any {
	if raw != "" {
		return textContent(raw)
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	if len(rows) == 0 {
		return textContent("0 rows")
	}
	body, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return errorContent(fmt.Errorf("marshal result: %w", err))
	}
	return textContent(fmt.Sprintf("%d row(s):\n\n%s", len(rows), body))
}
