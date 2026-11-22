package freshness

import (
	"context"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"
)

// fakeSink returns a fixed file listing. It implements only the Lister method
// freshness uses.
type fakeSink struct {
	files []string
}

func (f *fakeSink) ListFiles(context.Context, string) ([]string, error) { return f.files, nil }

func table(base, latest int64) manifest.Table {
	return manifest.Table{Status: "active", Path: "public.orders", BaseEpoch: base, LatestDeltaEpoch: latest}
}

func TestTable_InSync(t *testing.T) {
	sink := &fakeSink{files: []string{
		"public.orders/base/base_000003.parquet",
		"public.orders/deltas/epoch=000003.parquet",
	}}
	b, err := Table(context.Background(), sink, "public.orders", table(3, 3))
	if err != nil {
		t.Fatal(err)
	}
	if b.ReceivedEpoch != 3 || b.PublishedEpoch != 3 || b.CompactedEpoch != 3 {
		t.Errorf("boundaries = %d/%d/%d, want 3/3/3", b.ReceivedEpoch, b.PublishedEpoch, b.CompactedEpoch)
	}
	if b.State != StateInSync {
		t.Errorf("state = %s, want %s", b.State, StateInSync)
	}
}

func TestTable_ReceivedNotPublished(t *testing.T) {
	// Epochs 4 and 5 landed in storage, but the manifest still says 3.
	sink := &fakeSink{files: []string{
		"public.orders/base/base_000003.parquet",
		"public.orders/deltas/epoch=000004.parquet",
		"public.orders/deltas/epoch=000005.parquet",
	}}
	b, err := Table(context.Background(), sink, "public.orders", table(3, 3))
	if err != nil {
		t.Fatal(err)
	}
	if b.ReceivedEpoch != 5 {
		t.Errorf("received = %d, want 5", b.ReceivedEpoch)
	}
	if b.PublishedEpoch != 3 {
		t.Errorf("published = %d, want 3", b.PublishedEpoch)
	}
	if b.PendingEpochs != 2 || b.PendingDeltaFiles != 2 {
		t.Errorf("pending epochs/files = %d/%d, want 2/2", b.PendingEpochs, b.PendingDeltaFiles)
	}
	if b.State != StateReceivedNotPublished {
		t.Errorf("state = %s, want %s", b.State, StateReceivedNotPublished)
	}
}

func TestTable_PublishedNotCompacted(t *testing.T) {
	sink := &fakeSink{files: []string{
		"public.orders/base/base_000001.parquet",
		"public.orders/deltas/epoch=000002.parquet",
		"public.orders/deltas/epoch=000003.parquet",
	}}
	b, err := Table(context.Background(), sink, "public.orders", table(1, 3))
	if err != nil {
		t.Fatal(err)
	}
	if b.State != StatePublishedNotCompacted {
		t.Errorf("state = %s, want %s", b.State, StatePublishedNotCompacted)
	}
	if b.UncompactedEpochs != 2 {
		t.Errorf("uncompacted = %d, want 2", b.UncompactedEpochs)
	}
}

func TestTable_PublishedMissing(t *testing.T) {
	// Manifest claims epoch 5 but no delta file exists for it.
	sink := &fakeSink{files: []string{
		"public.orders/base/base_000002.parquet",
		"public.orders/deltas/epoch=000004.parquet",
	}}
	b, err := Table(context.Background(), sink, "public.orders", table(2, 5))
	if err != nil {
		t.Fatal(err)
	}
	if b.State != StatePublishedMissing {
		t.Errorf("state = %s, want %s", b.State, StatePublishedMissing)
	}
}

func TestTable_BaseMissing(t *testing.T) {
	sink := &fakeSink{files: []string{
		"public.orders/deltas/epoch=000001.parquet",
	}}
	b, err := Table(context.Background(), sink, "public.orders", table(0, 1))
	if err != nil {
		t.Fatal(err)
	}
	if b.BaseFilePresent {
		t.Errorf("BaseFilePresent = true, want false")
	}
	if b.State != StateBaseMissing {
		t.Errorf("state = %s, want %s", b.State, StateBaseMissing)
	}
}

func TestReport_BelowMin(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			"public.orders":    table(0, 5),
			"public.customers": {Status: "active", Path: "public.customers", BaseEpoch: 0, LatestDeltaEpoch: 2},
			"public.skipme":    {Status: "excluded"},
		},
	}
	sink := &fakeSink{files: []string{
		"public.orders/base/base_000000.parquet",
		"public.orders/deltas/epoch=000005.parquet",
		"public.customers/base/base_000000.parquet",
		"public.customers/deltas/epoch=000002.parquet",
	}}
	r, err := ComputeFromManifest(context.Background(), sink, m)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Tables) != 2 {
		t.Fatalf("tables = %d, want 2 (excluded tables skipped)", len(r.Tables))
	}
	below := r.BelowMin(4)
	if len(below) != 1 || below[0].Table != "public.customers" {
		t.Errorf("BelowMin(4) = %+v, want only public.customers", below)
	}
	if r.MaxPublishedEpoch != 5 {
		t.Errorf("max published = %d, want 5", r.MaxPublishedEpoch)
	}
}

func TestParseBaseEpoch(t *testing.T) {
	cases := []struct {
		name string
		want int64
		ok   bool
	}{
		{"base_000042.parquet", 42, true},
		{"base_000000.parquet", 0, true},
		{"epoch=000005.parquet", 0, false},
		{"base_abc.parquet", 0, false},
	}
	for _, c := range cases {
		got, ok := parseBaseEpoch(c.name)
		if got != c.want || ok != c.ok {
			t.Errorf("parseBaseEpoch(%q) = %d,%t want %d,%t", c.name, got, ok, c.want, c.ok)
		}
	}
}
