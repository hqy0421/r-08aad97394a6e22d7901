package internal

import (
	"bytes"
	"strings"
	"testing"

	"github.com/burnside-project/pg-cdc-core/internal/freshness"
)

func TestPrintBoundaries_FlagsReceivedNotPublished(t *testing.T) {
	r := &freshness.Report{
		Tables: []freshness.Boundary{
			{Table: "public.orders", ReceivedEpoch: 5, PublishedEpoch: 3, CompactedEpoch: 0,
				PendingEpochs: 2, State: freshness.StateReceivedNotPublished},
			{Table: "public.customers", ReceivedEpoch: 1, PublishedEpoch: 1, CompactedEpoch: 1,
				State: freshness.StateInSync},
		},
		InSync: 1, ReceivedNotPublished: 1,
		MaxReceivedEpoch: 5, MaxPublishedEpoch: 3,
	}
	var buf bytes.Buffer
	printBoundaries(&buf, r)
	out := buf.String()

	for _, want := range []string{"received-not-published", "public.orders", "landed, manifest not yet committed"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output, got:\n%s", want, out)
		}
	}
	// Healthy tables are summarized in the count line, not listed row by row.
	if strings.Contains(out, "public.customers") {
		t.Errorf("in-sync table should not be listed per-row, got:\n%s", out)
	}
}

func TestPrintBoundaries_AllInSyncOmitsTable(t *testing.T) {
	r := &freshness.Report{
		Tables: []freshness.Boundary{
			{Table: "public.orders", ReceivedEpoch: 2, PublishedEpoch: 2, CompactedEpoch: 2, BaseFilePresent: true, State: freshness.StateInSync},
		},
		InSync: 1, MaxReceivedEpoch: 2, MaxPublishedEpoch: 2,
	}
	var buf bytes.Buffer
	printBoundaries(&buf, r)
	if !strings.Contains(buf.String(), "1 in sync") {
		t.Errorf("expected in-sync summary, got:\n%s", buf.String())
	}
}
