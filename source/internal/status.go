package internal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/freshness"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Status shows replication health: slot status, lag, LSN, epochs, tables, and
// the per-table received / published / compacted boundaries.
//
// The three boundaries are reported separately because they can legitimately
// differ: a flush can upload delta files (received) and crash before the
// manifest is committed (published), leaving read services — which key on the
// manifest watermark — behind while the data is already on disk.
func Status(ctx context.Context, cfg config.Config, source ports.Source, sink ports.Sink) error {
	defer source.Close()

	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}

	// Query slot status
	var slotActive bool
	var confirmedLSN, restartLSN string
	err = source.QueryRow(ctx,
		`SELECT active, confirmed_flush_lsn::text, restart_lsn::text
		 FROM pg_replication_slots WHERE slot_name = $1`, slotName).
		Scan(&slotActive, &confirmedLSN, &restartLSN)

	fmt.Println("pg-cdc status")
	fmt.Println()

	if err != nil {
		fmt.Printf("  Slot: %s (not found)\n", slotName)
	} else {
		activeStr := "inactive"
		if slotActive {
			activeStr = "streaming"
		}
		fmt.Printf("  Slot:          %s (%s)\n", slotName, activeStr)
		fmt.Printf("  Confirmed LSN: %s\n", confirmedLSN)
		fmt.Printf("  Restart LSN:   %s\n", restartLSN)
	}

	fmt.Printf("  Init LSN:      %s\n", m.Source.InitLSN)
	fmt.Printf("  Source:         %s\n", m.Source.Name)
	fmt.Println()

	var active, excluded int
	for _, t := range m.Tables {
		if t.Status == "active" {
			active++
		} else {
			excluded++
		}
	}

	// Compute visibility boundaries for every active table.
	report, ferr := freshness.ComputeFromManifest(ctx, sink, m)

	fmt.Printf("  Tables:         %d active, %d excluded\n", active, excluded)
	if ferr == nil {
		fmt.Printf("  Published epoch: %d (manifest watermark read services see)\n", report.MaxPublishedEpoch)
		fmt.Printf("  Received epoch:  %d (highest epoch physically in storage)\n", report.MaxReceivedEpoch)
	} else {
		fmt.Printf("  Boundaries:     unavailable (%v)\n", ferr)
	}
	fmt.Printf("  Storage:        %s (%s)\n", cfg.Storage.Path, sink.Backend())

	if !m.Compaction.LastCompactedAt.IsZero() {
		fmt.Printf("  Last compacted: %s\n", m.Compaction.LastCompactedAt.Format("2006-01-02 15:04:05"))
	}

	if len(m.Roles) > 0 {
		fmt.Printf("  Role profiles:  %d\n", len(m.Roles))
	}

	if ferr != nil {
		return nil
	}

	printBoundaries(os.Stdout, report)
	return nil
}

// printBoundaries prints a per-table summary of the received / published /
// compacted boundaries, calling out tables where they diverge.
func printBoundaries(w io.Writer, r *freshness.Report) {
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  Visibility:     %d in sync, %d received-not-published, %d published-not-compacted",
		r.InSync, r.ReceivedNotPublished, r.PublishedNotCompacted)
	if r.Anomalous > 0 {
		fmt.Fprintf(w, ", %d anomalous", r.Anomalous)
	}
	fmt.Fprintln(w)

	// Only list tables whose boundaries aren't fully collapsed — the summary
	// line already covers the healthy ones.
	var divergent []freshness.Boundary
	for _, b := range r.Tables {
		if b.State != freshness.StateInSync {
			divergent = append(divergent, b)
		}
	}
	if len(divergent) == 0 {
		return
	}
	sort.Slice(divergent, func(i, j int) bool { return divergent[i].Table < divergent[j].Table })

	fmt.Fprintln(w)
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "  TABLE\tRECEIVED\tPUBLISHED\tCOMPACTED\tSTATE\tNOTE")
	for _, b := range divergent {
		fmt.Fprintf(tw, "  %s\t%d\t%d\t%d\t%s\t%s\n",
			b.Table, b.ReceivedEpoch, b.PublishedEpoch, b.CompactedEpoch, b.State, boundaryNote(b))
	}
	_ = tw.Flush()
}

// boundaryNote explains a non-in-sync state in operator terms.
func boundaryNote(b freshness.Boundary) string {
	switch b.State {
	case freshness.StateReceivedNotPublished:
		return fmt.Sprintf("%d epoch(s) landed, manifest not yet committed; readers will lag until the next flush commits", b.PendingEpochs)
	case freshness.StatePublishedNotCompacted:
		return fmt.Sprintf("%d epoch(s) not yet folded into a base", b.UncompactedEpochs)
	case freshness.StatePublishedMissing:
		return "manifest advertises an epoch with no matching files in storage"
	case freshness.StateBaseMissing:
		return fmt.Sprintf("no base file for base epoch %d", b.CompactedEpoch)
	default:
		return ""
	}
}
