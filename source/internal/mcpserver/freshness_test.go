package mcpserver

import (
	"strings"
	"testing"
)

// newGatedServer builds a server whose public.orders table has published
// epoch 3 and the given files in storage. It reuses newTestServer's manifest
// shape but overrides epochs and seeds the fake file listing.
func newGatedServer(t *testing.T, published int64, files []string) *server {
	t.Helper()
	s := newTestServer(t.TempDir())
	ord := s.sink.(*fakeSink).m.Tables["public.orders"]
	ord.LatestDeltaEpoch = published
	s.sink.(*fakeSink).m.Tables["public.orders"] = ord
	s.sink.(*fakeSink).files = files
	return s
}

// TestGate_ReceivedNotPublished verifies the exact scenario the operator hit:
// files for newer epochs have landed, but the manifest watermark has not moved.
// A read gated on the newer watermark must fail with an explanation that names
// the received-but-unpublished cause — not silently serve stale data.
func TestGate_ReceivedNotPublished(t *testing.T) {
	s := newGatedServer(t, 3, []string{
		"public.orders/base/base_000000.parquet",
		"public.orders/deltas/epoch=000003.parquet",
		"public.orders/deltas/epoch=000004.parquet",
	})
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"recent_changes","arguments":{"name":"public.orders","min_published_epoch":4}}}`)
	text, _ := resultText(t, resp)
	if !strings.Contains(text, "published epoch 3") || !strings.Contains(text, "at least 4") {
		t.Errorf("expected published/shortfall numbers, got: %s", text)
	}
	if !strings.Contains(text, "landed in storage but the manifest has not committed") {
		t.Errorf("expected received-not-published explanation, got: %s", text)
	}
}

func TestGate_Satisfied(t *testing.T) {
	s := newGatedServer(t, 5, []string{
		"public.orders/base/base_000000.parquet",
		"public.orders/deltas/epoch=000005.parquet",
	})
	// The gate passes at 5; the query proceeds to the DuckDB-ready check,
	// which will fail on a missing CLI or missing files. Either way the gate
	// must not be the thing that blocks it.
	gate := s.checkMinPublished(t.Context(), s.sink.(*fakeSink).m, "public.orders", 5)
	if !gate.OK {
		t.Errorf("gate should pass at published epoch 5, got: %s", gate.Message)
	}
	gate = s.checkMinPublished(t.Context(), s.sink.(*fakeSink).m, "public.orders", 6)
	if gate.OK {
		t.Errorf("gate should fail at min 6")
	}
}

func TestGate_QueryReferencesTable(t *testing.T) {
	s := newGatedServer(t, 2, []string{
		"public.orders/base/base_000000.parquet",
		"public.orders/deltas/epoch=000002.parquet",
	})
	// A statement that references orders and asks for a newer watermark is
	// rejected by the query gate before DuckDB is consulted.
	gate := s.gateQueryTables(t.Context(), s.sink.(*fakeSink).m,
		"SELECT * FROM public.orders WHERE id > 0", 3)
	if gate.OK {
		t.Fatal("gate should have blocked the query")
	}
	if !strings.Contains(gate.Message, "public.orders") {
		t.Errorf("message should name the offending table, got: %s", gate.Message)
	}

	// A statement that does not reference the table is unaffected.
	gate = s.gateQueryTables(t.Context(), s.sink.(*fakeSink).m, "SELECT 1", 3)
	if !gate.OK {
		t.Errorf("gate should not block an unrelated query, got: %s", gate.Message)
	}
}

func TestReferencesTable(t *testing.T) {
	cases := []struct {
		sql  string
		name string
		want bool
	}{
		{"select * from public.orders", "public.orders", true},
		{"select * from orders", "public.orders", true},
		{"select * from backorders", "public.orders", false},
		{"select 1", "public.orders", false},
		{"select o.id from public.orders o join public.customers c on 1=1", "public.customers", true},
	}
	for _, c := range cases {
		if got := referencesTable(strings.ToLower(c.sql), c.name); got != c.want {
			t.Errorf("referencesTable(%q, %q) = %t, want %t", c.sql, c.name, got, c.want)
		}
	}
}

func TestGetFreshness_AllTables(t *testing.T) {
	s := newGatedServer(t, 3, []string{
		"public.orders/base/base_000000.parquet",
		"public.orders/deltas/epoch=000003.parquet",
		"public.orders/deltas/epoch=000004.parquet",
	})
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_freshness"}}`)
	text, isErr := resultText(t, resp)
	if isErr {
		t.Fatalf("unexpected error: %s", text)
	}
	if !strings.Contains(text, "received-not-published") {
		t.Errorf("expected received-not-published state, got: %s", text)
	}
	if !strings.Contains(text, "max published=3") || !strings.Contains(text, "max received=4") {
		t.Errorf("expected boundary summary line, got: %s", text)
	}
}

func TestGetFreshness_SingleTableBelowMin(t *testing.T) {
	s := newGatedServer(t, 3, []string{
		"public.orders/base/base_000000.parquet",
		"public.orders/deltas/epoch=000003.parquet",
	})
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_freshness","arguments":{"name":"public.orders","min_published_epoch":9}}}`)
	text, isErr := resultText(t, resp)
	if isErr {
		t.Fatalf("unexpected error: %s", text)
	}
	if !strings.Contains(text, "Below min_published_epoch=9") || !strings.Contains(text, "short by 6") {
		t.Errorf("expected shortfall explanation, got: %s", text)
	}
}
