// Package freshness computes per-table visibility boundaries for a pg-cdc
// deployment. All three boundaries are derived from storage alone, so they can
// be inspected without a connection to the source database.
//
//   - received  — the highest epoch whose file has physically landed in storage
//     (delta Parquet for a flush, or a compacted base file). A flush can
//     upload deltas and crash before the manifest write; those epochs are
//     received but not yet published.
//   - published — the highest epoch recorded in the manifest
//     (`latest_delta_epoch`). This is the watermark read services key on:
//     anything above it is not visible to them yet.
//   - compacted — the highest epoch folded into a base snapshot
//     (`base_epoch`).
//
// The distinction matters operationally: `received > published` means a flush
// landed but has not been committed to the manifest, so a reader that trusts
// the manifest watermark is legitimately stale while `pg-cdc status` (which
// previously printed a single "latest epoch") looks healthy.
package freshness

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
)

// State classifies a table's boundary relationships.
type State string

const (
	// StateInSync means received == published == compacted.
	StateInSync State = "in-sync"
	// StateReceivedNotPublished means files for newer epochs have landed in
	// storage but the manifest watermark has not advanced past them yet.
	StateReceivedNotPublished State = "received-not-published"
	// StatePublishedNotCompacted means the manifest exposes epochs that have
	// not yet been folded into a base snapshot.
	StatePublishedNotCompacted State = "published-not-compacted"
	// StatePublishedMissing means the manifest claims a published epoch with
	// no matching files in storage.
	StatePublishedMissing State = "published-but-missing"
	// StateBaseMissing means the base file for the manifest's base epoch is
	// absent from storage.
	StateBaseMissing State = "base-missing"
)

// Boundary holds the three visibility boundaries for one table.
type Boundary struct {
	Table             string `json:"table"`
	ReceivedEpoch     int64  `json:"received_epoch"`
	PublishedEpoch    int64  `json:"published_epoch"`
	CompactedEpoch    int64  `json:"compacted_epoch"`
	PendingEpochs     int64  `json:"pending_epochs"`     // received - published
	UncompactedEpochs int64  `json:"uncompacted_epochs"` // published - compacted
	DeltaFiles        int    `json:"delta_files"`
	PendingDeltaFiles int    `json:"pending_delta_files"` // delta files above the published watermark
	BaseFilePresent   bool   `json:"base_file_present"`
	State             State  `json:"state"`
}

// Lister lists objects under a prefix. It is the subset of ports.Sink the
// boundary computation needs; keeping it narrow decouples this package from the
// full sink surface and makes it trivial to fake in tests.
type Lister interface {
	ListFiles(ctx context.Context, prefix string) ([]string, error)
}

// Report is the boundary set for every active table in a manifest.
type Report struct {
	Tables                []Boundary `json:"tables"`
	InSync                int        `json:"in_sync"`
	ReceivedNotPublished  int        `json:"received_not_published"`
	PublishedNotCompacted int        `json:"published_not_compacted"`
	Anomalous             int        `json:"anomalous"`
	MaxReceivedEpoch      int64      `json:"max_received_epoch"`
	MaxPublishedEpoch     int64      `json:"max_published_epoch"`
}

// ComputeFromManifest computes boundaries from an already-read manifest,
// avoiding a second manifest fetch for callers that already have it.
func ComputeFromManifest(ctx context.Context, sink Lister, m *manifest.Manifest) (*Report, error) {
	r := &Report{}
	names := make([]string, 0, len(m.Tables))
	for name, t := range m.Tables {
		if t.Status == "active" {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	for _, name := range names {
		b, err := Table(ctx, sink, name, m.Tables[name])
		if err != nil {
			return nil, fmt.Errorf("compute boundaries for %s: %w", name, err)
		}
		r.Tables = append(r.Tables, b)
		switch b.State {
		case StateInSync:
			r.InSync++
		case StateReceivedNotPublished:
			r.ReceivedNotPublished++
		case StatePublishedNotCompacted:
			r.PublishedNotCompacted++
		default:
			r.Anomalous++
		}
		r.MaxReceivedEpoch = max(r.MaxReceivedEpoch, b.ReceivedEpoch)
		r.MaxPublishedEpoch = max(r.MaxPublishedEpoch, b.PublishedEpoch)
	}
	return r, nil
}

// Table computes the boundaries for a single table by listing its delta and
// base objects. It performs no manifest read.
func Table(ctx context.Context, sink Lister, name string, t manifest.Table) (Boundary, error) {
	b := Boundary{
		Table:          name,
		PublishedEpoch: t.LatestDeltaEpoch,
		CompactedEpoch: t.BaseEpoch,
	}

	// A lower publish watermark than base would be self-contradictory; clamp
	// defensively so the reported relationship is never nonsensical.
	if b.PublishedEpoch < b.CompactedEpoch {
		b.PublishedEpoch = b.CompactedEpoch
	}

	deltaFiles, err := sink.ListFiles(ctx, path.Join(t.Path, "deltas")+"/")
	if err != nil {
		return b, fmt.Errorf("list deltas: %w", err)
	}
	var maxDelta int64
	for _, f := range deltaFiles {
		n, err := epoch.ParseFromFilename(path.Base(f))
		if err != nil {
			continue
		}
		b.DeltaFiles++
		if n > b.PublishedEpoch {
			b.PendingDeltaFiles++
		}
		maxDelta = max(maxDelta, n)
	}

	baseFiles, err := sink.ListFiles(ctx, path.Join(t.Path, "base")+"/")
	if err != nil {
		return b, fmt.Errorf("list base: %w", err)
	}
	var maxBase int64 = -1
	for _, f := range baseFiles {
		n, ok := parseBaseEpoch(path.Base(f))
		if !ok {
			continue
		}
		maxBase = max(maxBase, n)
		if n == t.BaseEpoch {
			b.BaseFilePresent = true
		}
	}

	// Received is the highest epoch for which *anything* is on disk. A base
	// file newer than the manifest's base epoch counts as received too: compact
	// uploads the base before committing the manifest, same as a flush.
	b.ReceivedEpoch = max(maxDelta, b.CompactedEpoch, maxBase)
	if b.ReceivedEpoch > b.PublishedEpoch {
		b.PendingEpochs = b.ReceivedEpoch - b.PublishedEpoch
	}
	if b.PublishedEpoch > b.CompactedEpoch {
		b.UncompactedEpochs = b.PublishedEpoch - b.CompactedEpoch
	}
	b.State = classify(b)
	return b, nil
}

// Find returns the boundary for a table name, if present.
func (r *Report) Find(name string) (Boundary, bool) {
	for _, b := range r.Tables {
		if b.Table == name {
			return b, true
		}
	}
	return Boundary{}, false
}

// BelowMin returns every table whose published epoch is under min. An empty
// result means every active table is at or beyond the requested version.
func (r *Report) BelowMin(min int64) []Boundary {
	var out []Boundary
	for _, b := range r.Tables {
		if b.PublishedEpoch < min {
			out = append(out, b)
		}
	}
	return out
}

func classify(b Boundary) State {
	switch {
	case !b.BaseFilePresent:
		return StateBaseMissing
	case b.PublishedEpoch > b.ReceivedEpoch:
		return StatePublishedMissing
	case b.ReceivedEpoch > b.PublishedEpoch:
		return StateReceivedNotPublished
	case b.PublishedEpoch > b.CompactedEpoch:
		return StatePublishedNotCompacted
	default:
		return StateInSync
	}
}

// parseBaseEpoch extracts the epoch from a "base_000042.parquet" filename.
func parseBaseEpoch(name string) (int64, bool) {
	if !strings.HasPrefix(name, "base_") || !strings.HasSuffix(name, ".parquet") {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "base_"), ".parquet"), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
