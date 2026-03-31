# Merging Gaia and Hipparcos

## Why two catalogs?

ESA's **Gaia** mission (launched 2013, Data Release 3 in 2022) surveyed roughly 1.8 billion sources with sub-milliarcsecond astrometry — an unprecedented map of the Milky Way. But Gaia's CCDs saturate on the brightest stars. For sources brighter than about G ~ 3–6, the astrometric solutions are often incomplete, degraded, or missing entirely. These are exactly the stars that people recognise when they look up at the night sky: Sirius, Vega, Betelgeuse, the Southern Cross.

**Hipparcos** (ESA, 1989–1993; New Reduction by van Leeuwen 2007) was purpose-built for bright-star astrometry. Its catalog of ~118,000 stars covers the naked-eye sky reliably, including many objects where Gaia struggles. The trade-off is that Hipparcos is shallow — it has nothing to say about the vast majority of the galaxy.

For a complete 3D star map you need both: Gaia for depth and completeness, Hipparcos for the bright-star anchors. But roughly 100,000 stars appear in both catalogs, and the pipeline must produce exactly **one canonical row** per physical star.

## Why deduplication is hard

Three factors make this non-trivial:

### No shared identifier

Gaia and Hipparcos use independent numbering systems. There is no column in either catalog that says "this is the same star." The only link is ESA's `hipparcos2_best_neighbour` crossmatch table — a positional match between the two catalogs published as part of Gaia DR3. The pipeline downloads it via a TAP query against the Gaia Archive (`gaia_to_hip/download.py`) and converts it into `gaia_hip_map.parquet`, which maps `gaia_source_id` to `hip_source_id` along with `number_of_neighbours` and `angular_distance`.

### Scale

The full Gaia catalog has ~1.5 billion rows. You cannot load two copies to compare — the merger must stream Gaia batch-by-batch while holding only the small HIP table (~118K rows) and crossmatch table in memory. This constraint shapes the entire merge architecture.

### Binaries and multiples

Hipparcos often saw a binary system as a single point of light, while Gaia resolves the individual components. The crossmatch may link one HIP entry to multiple Gaia sources (`number_of_neighbours > 1`), and the Hipparcos solution type field (`Sn`) indicates whether the astrometric fit was a standard five-parameter single-star model or something more complex (acceleration solution, orbital fit, or component solution). Non-standard solutions produce unreliable positions and parallaxes.

These problems concentrate on the brightest, most recognisable stars. Alpha Centauri, Sirius, and Procyon all require special handling because of binarity — which is why the pipeline has both automated vetoes and a manual override system.

## Quality scoring and winner selection

When a Gaia row and a Hipparcos row are linked by the crossmatch table and no manual override applies, the merger must choose one winner. The decision tree in `_choose_matched_winner` (`merge/pipeline.py`) applies these rules in order:

### 1. Neighbour veto

If `number_of_neighbours > 1` in the crossmatch entry, the Hipparcos measurement is ambiguous (Gaia resolves what Hipparcos saw as one source into multiple candidates). **Gaia wins automatically.**

### 2. Hipparcos multiplicity veto

If the Hipparcos solution type `Sn != 5` (anything other than the standard five-parameter single-star model), the Hipparcos astrometry is suspect — it may be a photocentre, an acceleration solution, or a component solution for a multiple system. **Gaia wins automatically.**

### 3. Bright-star gate with margin

For the remaining pairs, Hipparcos can only beat Gaia by a significant margin that depends on the star's apparent brightness:

| Apparent G magnitude | Margin | What it means |
|---|---|---|
| G < 3.5 (very bright) | 1.0x | Hip wins if strictly better — Gaia is often problematic here |
| 3.5 ≤ G < 6 (bright) | 0.6x | Hip score must be < 60% of Gaia's score |
| G ≥ 6 (normal) | 0.5x | Hip score must be < 50% of Gaia's score |

Apparent G comes from `phot_g_mean_mag` when available. If absent, it is estimated from `mag_abs + 5 * log10(r_pc / 10)`. If neither is available, the strictest margin (0.5x) applies by default.

### 4. Tie-break

If neither side wins after the margin comparison, **Gaia wins**.

### The quality metric

The comparison metric is `astrometry_quality`, which both catalog pipelines compute as **fractional parallax error** — σ/π for Hipparcos (`e_Plx / Plx`), and `parallax_error / max(parallax, ε)` for Gaia DR3 (or the analogous Bailer-Jones interval width). Lower is better. These values are directly comparable across catalogs.

For Gaia rows where the primary astrometric solution fails quality tests, the pipeline falls back through multiple tiers, each carrying a sentinel quality value that sorts correctly against real measurements:

- **Tier A** (primary): actual fractional error, typically < 1.0
- **Tier B** (weak catalog fallback): `10.0`
- **Tier C** (photometric distance from GSP-Phot M_G and A_G): `20.0`
- **Tier D** (synthetic prior from a fixed M_G = 4.0): `50.0`

## Manual overrides

Some stars cannot be handled by automated scoring. The override system lets you correct or supplement catalog data where automation is wrong or incomplete. Overrides are defined in YAML files (under `overrides/data/`), built into `overrides.parquet` by `fis-pipeline overrides build`, and applied by the merger with **highest precedence** — they always outrank automatic winner selection.

### Actions

- **`add`** — Insert a star that exists in neither catalog.
- **`replace`** — Substitute curated astrometry/photometry for a star that does exist. The original identity `(source, source_id)` is preserved; only the payload changes.
- **`drop`** — Remove a star from the merged output entirely.

### Pair-aware resolution

Override YAML targets a single row by `(source, source_id)`. When the target is one side of a crossmatch-linked pair, the merger resolves the **entire pair** via the crossmatch table — the partner from the other catalog is automatically suppressed. Override authors do not need to supply the partner's ID.

When Gaia data is magnitude-limited, a crossmatch partner may be absent from the loaded data. The merger handles this gracefully: if the target exists but the partner doesn't, the override applies normally and there's nothing to suppress. If the target is absent but the partner exists, the override still resolves the pair and suppresses the partner. If neither side exists, the merger emits a warning and records the override as `override_no_effect`.

### Packaged overrides

The pipeline ships with overrides for cases that illustrate the range of problems:

**The Sun** (`sun.yaml`) — An `add` override. The Sun is not in either catalog; it is inserted at the ICRS origin with IAU 2015 nominal solar parameters (T_eff = 5772 K, M_V = 4.83).

**Alpha Centauri A and B** (`alpha_cen.yaml`) — `replace` overrides targeting HIP 71683 and 71681. The per-component catalog parallaxes are replaced with the system-level binary parallax from Kervella et al. (2016/2017), keeping both components at a consistent distance.

**Proxima Centauri** (`alpha_cen.yaml`) — A `replace` override targeting HIP 70890. Proxima is missing from the `hipparcos2_best_neighbour` crossmatch table (likely due to its extreme proper motion of ~3.8 arcsec/yr), so the merger cannot deduplicate automatically. The override provides Gaia DR3 astrometry via SIMBAD and flags it as a variable flare star.

**Sirius B** (`binaries.yaml`) — A `replace` override targeting the Gaia DR3 source ID for Sirius B. The white dwarf companion has weak catalog photometry; the override substitutes HST-resolved values (Barstow et al. 2005).

**Procyon B** (`binaries.yaml`) — An `add` override. This white dwarf companion is absent from Gaia DR3 entirely; resolved photometry and temperature come from Provencal et al. (2002, HST/STIS).

### Override identifiers

Override YAML files can include an `identifiers` block that populates the identifiers sidecar for that star. This is how manual stars (e.g. the Sun → "Sol", Alpha Cen A → "Rigil Kentaurus") receive their display names. The identifiers pipeline reads these blocks and merges them into `identifiers_map.parquet` alongside Vizier-derived entries.

## Canonical identity

Each merged output row is identified by the compound key `(source, source_id)`. This is the single canonical identity used throughout the pipeline and by all sidecars.

- **Matched pairs** (Gaia + Hipparcos linked by crossmatch): always `source = "hip"`, `source_id = <hip_number>`, regardless of which catalog's astrometry won. The Hipparcos identifier is preferred because human-facing designations (HD, Bayer/Flamsteed, proper names) are keyed by HIP number in the identifiers pipeline.
- **Gaia-only** (unmatched): `source = "gaia"`, `source_id = <gaia_dr3_id>`.
- **Hipparcos-only** (unmatched, no crossmatch partner): `source = "hip"`, `source_id = <hip_number>`.
- **Manual adds**: `source = "manual"`, `source_id = <manual_id>` (e.g. `"sun"`). String-valued manual IDs cannot collide with numeric catalog IDs.
- **Replaced rows**: canonical identity is the **original target's** `(source, source_id)`. The override swaps the payload, not the identity.

Cross-catalog identifiers (e.g. the Gaia DR3 source ID for a Hipparcos star) are stored in the **identifiers sidecar** (`identifiers_map.parquet`), not on the dense table.

## Streaming merge architecture

The merger (`run_merge` in `merge/pipeline.py`) processes ~1.5 billion Gaia rows without materialising the full catalog in memory.

### In-memory lookup tables (loaded once)

- HIP stars (~118K rows, keyed by `source_id`)
- Crossmatch table (`gaia_hip_map.parquet`, bidirectional dicts)
- Overrides (~tens of rows)

### Streaming pass (one Gaia file at a time)

For each Gaia batch file:

1. Read the batch. Split rows into "special" (crossmatched or override-targeted, identified via a pre-built set of Gaia IDs) and "unmatched".
2. Unmatched Gaia rows are written directly to their HEALPix output directories.
3. Special rows are processed individually:
   - If an override applies to this row or its crossmatch partner, apply the override (replace/drop) and mark the pair resolved.
   - If it's a matched pair with no override, run the quality scoring decision tree, emit the winner under the HIP identity, and mark the HIP row resolved.
   - If the HIP partner is missing or already resolved, emit as unmatched Gaia.

### Post-streaming flush

After all Gaia batches are processed:

1. Flush remaining unresolved HIP rows (those with no Gaia partner in the loaded data, or unmatched).
2. Emit `add` override rows.
3. Check for unapplied `replace`/`drop` overrides where neither side was present (log warnings).

### HEALPix sharding

The merged output is partitioned by HEALPix pixel, computed from the winning row's `(ra_deg, dec_deg)`. Output layout:

```
{output_dir}/healpix/{pixel}/
  ├── 000001_gaia_batch1.parquet
  ├── 000002_gaia_special_batch1.parquet
  ├── 000003_hip_flush.parquet
  └── ...
```

HEALPix order is configurable via the project TOML (`[merge] healpix_order`).

### State tracking

The merger maintains only small fixed-size state across the streaming pass:

- A set of resolved HIP `source_id`s
- A set of processed override IDs
- A per-pixel write sequence counter
- The decisions list (bounded by crossmatch table size + override count)

No per-row state is accumulated for the ~1.5 billion unmatched Gaia rows.

## Outputs

The merger produces three artifacts:

### HEALPix-partitioned merged Parquet

One directory per HEALPix pixel under `{output_dir}/healpix/{pixel}/`, each containing one or more Parquet files with `OUTPUT_COLS`:

- `source`, `source_id` — canonical identity
- `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` — Sun-centred ICRS Cartesian coordinates (parsecs)
- `ra_deg`, `dec_deg`, `r_pc` — spherical coordinates
- `mag_abs` — absolute magnitude
- `teff` — effective temperature (K)
- `quality_flags` — packed uint16 encoding distance source, Teff source, photometry source, and status bits
- `astrometry_quality`, `photometry_quality` — numeric quality indicators

### Merge report (`merge_report.json`)

Aggregate counts only — a small fixed-size document regardless of catalog size. Includes counts by category (unmatched Gaia, unmatched HIP, matched-pair winners by catalog, override actions by type), HEALPix parameters, and the full input file manifest.

### Merge decisions sidecar (`merge_decisions.parquet`)

One row per matched-pair decision or override action. Records the Gaia and Hipparcos source IDs, winner catalog, quality scores, tie-break reason, override metadata, and diagnostic auxiliary columns (`gaia_ruwe`, `gaia_phot_g_mean_mag`, `hip_solution_type`, etc.). Bounded by the crossmatch table size (~100K rows) plus override count (~tens of rows), not by the full catalog size.

## Validation

### Runtime checks

- **Row count consistency**: after the merge, assert that `rows_emitted_total` equals the sum of unmatched Gaia + unmatched HIP + matched pairs scored + override replaces + override adds.
- **Drop override payload validation**: at load time, assert that each `drop` override carries no non-audit payload fields.
- **No-effect override warnings**: if a `replace` or `drop` override targets a row where neither the target nor its crossmatch partner exists in the loaded data, emit a warning and record it in the decisions sidecar.

### Guaranteed by construction

- **No duplicate canonical IDs**: Gaia source_ids are unique within Gaia, HIP source_ids within HIP, matched pairs always use HIP identity with exactly one winner, and manual namespace IDs are strings that cannot collide with numeric catalog IDs.
- **All matched pairs resolved**: the streaming pass processes every Gaia row, and the HIP flush processes every remaining HIP row.

## Known limitations and potential improvements

### RUWE not used in winner selection

The Gaia Renormalised Unit Weight Error (`ruwe`) is carried in the decisions sidecar for diagnostics but does not influence winner selection. A future version could use RUWE > 1.4 as a soft veto or quality penalty for the Gaia side of matched pairs, since high RUWE indicates a poor single-star astrometric fit.

### Photometry not used in scoring

Winner selection is based purely on astrometric quality. The `photometry_quality` column is carried through to the output but does not affect the winner decision. For use cases where photometric accuracy matters (e.g. colour-magnitude diagrams), incorporating photometric quality into the scoring could be valuable.

### Magnitude-limited subsets

When running on small Gaia subsets (e.g. with `[gaia] mag_limit` in the project file), many crossmatch entries reference Gaia source IDs that are absent from the loaded data. The merger handles this correctly — effectively-unmatched HIP rows are kept, and overrides targeting absent rows still resolve via the crossmatch — but the merge report does not currently flag the fraction of crossmatch entries that were exercised versus skipped.

### Within-shard ordering

Shards are written in arrival order within each HEALPix pixel. Downstream consumers that need spatial or magnitude ordering must sort the data themselves.

### Hard-coded scoring thresholds

The bright-star margin thresholds (1.0x, 0.6x, 0.5x) and magnitude boundaries (3.5, 6.0) are constants in `merge/pipeline.py`. They could potentially be tuned empirically by analysing matched-pair quality distributions from the decisions sidecar, or made configurable via the project TOML.
