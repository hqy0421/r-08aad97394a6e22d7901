package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/freshness"
)

// gateResult describes whether a read may proceed against the requested
// minimum published epoch, and if not, why — in operator terms.
type gateResult struct {
	OK      bool
	Message string
}

// checkMinPublished verifies that the table's published watermark has reached
// the caller's requested minimum. The published epoch — not the received epoch
// — is what read services see, so a gate must be evaluated against it: a flush
// that landed files but did not commit the manifest leaves published behind
// even though the bytes are in storage.
func (s *server) checkMinPublished(ctx context.Context, m *manifest.Manifest, tableName string, min int64) gateResult {
	if min <= 0 {
		return gateResult{OK: true}
	}
	t, ok := m.Tables[tableName]
	if !ok || t.Status != "active" {
		return gateResult{
			OK:      false,
			Message: fmt.Sprintf("table %q is not an active table in the manifest", tableName),
		}
	}
	b, err := freshness.Table(ctx, s.sink, tableName, t)
	if err != nil {
		return gateResult{
			OK:      false,
			Message: fmt.Sprintf("could not verify published epoch for %s: %v", tableName, err),
		}
	}
	if b.PublishedEpoch < min {
		return gateResult{OK: false, Message: gateMessage(b, min)}
	}
	return gateResult{OK: true}
}

// gateMessage explains a shortfall between the requested minimum and what the
// manifest currently publishes, distinguishing the two causes an operator can
// act on differently.
func gateMessage(b freshness.Boundary, min int64) string {
	base := fmt.Sprintf(
		"%s has published epoch %d but at least %d was requested (short by %d).",
		b.Table, b.PublishedEpoch, min, min-b.PublishedEpoch)

	switch b.State {
	case freshness.StateReceivedNotPublished:
		return base + fmt.Sprintf(
			" Epochs %d–%d have landed in storage but the manifest has not committed them yet, so read services cannot see them until the next successful flush.",
			b.PublishedEpoch+1, b.ReceivedEpoch)
	case freshness.StatePublishedMissing:
		return base + " The manifest advertises an epoch with no matching files in storage — the deployment may be inconsistent; run `pg-cdc status`."
	case freshness.StateBaseMissing:
		return base + fmt.Sprintf(
			" No base file exists for base epoch %d; run `pg-cdc init` or `pg-cdc compact` to rebuild it.", b.CompactedEpoch)
	default:
		return base + " Wait for the streamer to publish further epochs, then retry."
	}
}

// getFreshness reports per-table received / published / compacted boundaries.
// This is the discoverable surface for the gate: a caller reads this first to
// learn what published epoch it can safely demand.
func (s *server) getFreshness(ctx context.Context, args json.RawMessage) map[string]any {
	var a struct {
		Name              string `json:"name"`
		MinPublishedEpoch int64  `json:"min_published_epoch"`
	}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &a); err != nil {
			return errorContent(fmt.Errorf("invalid arguments: %w", err))
		}
	}

	m, err := s.sink.ReadManifest(ctx)
	if err != nil {
		return errorContent(fmt.Errorf("read manifest: %w (run `pg-cdc init` first?)", err))
	}

	report, err := freshness.ComputeFromManifest(ctx, s.sink, m)
	if err != nil {
		return errorContent(err)
	}

	if a.Name != "" {
		b, ok := report.Find(a.Name)
		if !ok {
			return errorContent(fmt.Errorf("table not found among active tables: %s", a.Name))
		}
		return textContent(renderSingleBoundary(b, a.MinPublishedEpoch))
	}
	return textContent(renderReport(report, a.MinPublishedEpoch))
}

func renderReport(r *freshness.Report, min int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Visibility boundaries across %d active tables\n", len(r.Tables))
	fmt.Fprintf(&b, "max published=%d · max received=%d · in-sync=%d · received-not-published=%d · published-not-compacted=%d",
		r.MaxPublishedEpoch, r.MaxReceivedEpoch, r.InSync, r.ReceivedNotPublished, r.PublishedNotCompacted)
	if r.Anomalous > 0 {
		fmt.Fprintf(&b, " · anomalous=%d", r.Anomalous)
	}
	b.WriteString("\n")

	if min > 0 {
		short := len(r.BelowMin(min))
		if short == 0 {
			fmt.Fprintf(&b, "\nAll tables meet min_published_epoch=%d.\n", min)
		} else {
			fmt.Fprintf(&b, "\n%d of %d tables are below min_published_epoch=%d.\n", short, len(r.Tables), min)
		}
	}

	b.WriteString("\ntable · received / published / compacted · state\n")
	rows := make([]freshness.Boundary, len(r.Tables))
	copy(rows, r.Tables)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Table < rows[j].Table })
	for _, t := range rows {
		fmt.Fprintf(&b, "- %s · %d / %d / %d · %s", t.Table, t.ReceivedEpoch, t.PublishedEpoch, t.CompactedEpoch, t.State)
		if min > 0 && t.PublishedEpoch < min {
			b.WriteString(" · BELOW MIN")
		}
		b.WriteString("\n")
	}
	return b.String()
}

func renderSingleBoundary(b freshness.Boundary, min int64) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s\n\n", b.Table)
	fmt.Fprintf(&sb, "  received epoch:  %d (%d delta file(s) in storage)\n", b.ReceivedEpoch, b.DeltaFiles)
	fmt.Fprintf(&sb, "  published epoch: %d (manifest watermark read services see)\n", b.PublishedEpoch)
	fmt.Fprintf(&sb, "  compacted epoch: %d (folded into base)\n", b.CompactedEpoch)
	fmt.Fprintf(&sb, "  base file:       present=%t\n", b.BaseFilePresent)
	fmt.Fprintf(&sb, "  state:           %s\n", b.State)
	if b.PendingEpochs > 0 {
		fmt.Fprintf(&sb, "  pending:         %d epoch(s) received but not published (%d delta file(s))\n",
			b.PendingEpochs, b.PendingDeltaFiles)
	}
	if b.UncompactedEpochs > 0 {
		fmt.Fprintf(&sb, "  uncompacted:     %d epoch(s) published but not folded into a base\n", b.UncompactedEpochs)
	}
	if min > 0 {
		if b.PublishedEpoch >= min {
			fmt.Fprintf(&sb, "\nMeets min_published_epoch=%d.\n", min)
		} else {
			fmt.Fprintf(&sb, "\nBelow min_published_epoch=%d (short by %d).\n", min, min-b.PublishedEpoch)
			fmt.Fprintf(&sb, "  %s\n", gateMessage(b, min))
		}
	}
	return sb.String()
}
