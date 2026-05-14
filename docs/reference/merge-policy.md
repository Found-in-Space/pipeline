# Mergers specification

## Purpose

Define how Gaia, Hipparcos, and **manual overrides** are merged into one canonical dataset for downstream processing (Stage 00 onward).

The merger resolves duplicates using a cross-match table and quality rules, applies **highest-precedence manual corrections**, then emits Parquet compatible with Stage 00 and Stage 01. **Morton codes and octree construction are not part of this data pipeline**; they belong in a downstream step that can scan **2D spatial shards** (e.g. HEALPix) instead of the full catalog.

## Scope

This specification covers:

- when merge occurs relative to per-catalog pipelines and spatial sharding
- required inputs and outputs (including manual overrides)
- duplicate-resolution rules and override precedence
- schema contract for the **dense** merged star table and **merge decisions** sidecar

This specification does not cover:

- reverse/global lookup indices (implementation detail of serving)
- catalog-specific scientific quality policy beyond the tie-break and scoring contract

---

## Stage placement

### Per-catalog preprocessing

1. **Gaia pipeline** — ingest, astrometry/photometry, coordinates → per-batch or staging Parquet.
2. **Hipparcos pipeline** — same role for Hipparcos-only rows.
3. **Overrides pipeline** — normalize the manual overrides table (versioned artifact; see below).

### Merge (pre–Stage 00)

Merge runs **after** those three inputs exist and **before** Stage 00:

`gaia parquet set + hip parquet + crossmatch + manual_overrides → merged canonical parquet → Stage 00 → Stage 01`

### HEALPix-sharded output (mandatory)

The merged output **must** be partitioned by HEALPix pixel. At ~1.5 billion rows a single monolithic file is not viable — it cannot be written, read, or consumed by downstream stages. HEALPix sharding is not optional.

The output layout is **one directory per HEALPix pixel**, each containing one or more Parquet files:

```
data/processed/merged/healpix/{pixel}/
  ├── 0001.parquet
  ├── 0002.parquet   # if multiple sources contribute to this pixel
  └── ...
```

Rationale and constraints:

- **Octree alignment**: HEALPix pixels map cleanly onto 3D octree view cones. Downstream octree builders read a single HEALPix directory to process all stars in that sky region, without scanning the full catalog.
- **Append-friendly**: A single Gaia input batch may contain rows for more than one HEALPix pixel, and conversely multiple input batches may contribute to the same pixel. Writing into directories (not single files) means the merger can append without assuming one-to-one input-to-output mapping.
- **Gaia input is already HEALPix-grouped**: Gaia download batches are organized so each HEALPix pixel appears in exactly one input file, but each input file may bundle several pixels. The merger streams through these batches and routes rows to the correct output directory by computing the HEALPix pixel from the winning row's (`ra_deg`, `dec_deg`).
- **HIP and override rows**: These are small enough to hold in memory. After processing all Gaia batches, unmatched HIP rows and `add` override rows are routed to their respective HEALPix directories based on their own sky positions.
- **Morton encoding**, if used at all, is a concern of the downstream indexer, not of the merge output schema.
- HEALPix level for the merged output may differ from the level used for Gaia download batching; the level must be documented in the merge report.

### HEALPix pixel assignment and crossmatch edge cases

The output HEALPix pixel for a merged row is determined by the **winning row's sky position** (`ra_deg`, `dec_deg`). For matched pairs near HEALPix pixel boundaries, the Gaia and Hipparcos positions may fall in different pixels due to slightly different astrometric solutions. This does not affect de-duplication or override resolution — those operate on IDs from the crossmatch table, not on positions. The winning row is placed in whichever pixel its coordinates fall in; the losing row is simply suppressed.

### Rationale summary

- Merge requires cross-catalog joins that Stage 00 file-local transforms do not perform.
- Quality metrics for Gaia vs Hip winner selection are available from catalog preprocessing.
- Merging before Stage 00 avoids duplicating heavy work on rows that will be dropped or replaced.
- Sparse identifiers (HD, Bayer, proper names) are kept in the **identifiers sidecar** (produced separately) so the dense table does not carry mostly-null columns across billions of rows.

---

## Inputs

Required:

- Gaia source parquet set (or equivalent table), after Gaia pipeline preprocessing
- Hipparcos source parquet (or equivalent table), after Hipparcos pipeline preprocessing
- Cross-match table mapping Gaia `source_id` and Hipparcos `HIP` (or equivalent) for matched pairs
- **Manual overrides** table (versioned): explicit `add`, `replace`, and optional `drop` / suppress actions (see below)

Optional:

- A provisional mapping sidecar derived from Gaia catalog rows (when Gaia input includes HIP identifiers) to reduce re-scan cost. This artifact is advisory and can be superseded by merge-time crossmatch/override resolution.

---

## File layout and naming conventions

Use a fixed split between raw catalogs and processed artifacts:

- Raw catalog downloads: `data/catalogs/`
- Processed artifacts used by merger and downstream stages: `data/processed/`

Within `data/processed/`, current naming policy is deterministic and source-aware:

- Gaia stars: `data/processed/gaia/*.parquet` (one file per Gaia input, wildcard-friendly)
- Gaia/Hip mapping: `data/processed/gaia_hip_map.parquet` (top level, intentionally **not** under `gaia/`)
- Hipparcos processed stars: `data/processed/hip_stars.parquet`
- Identifier mapping sidecar: `data/processed/identifiers_map.parquet`
- Manual overrides (merger input): `data/processed/overrides.parquet` — single wide table: `OUTPUT_COLS` plus `override_id`, `action`, `override_reason`, `override_policy_version`. Non-drop rows use `DIST_SRC_OVERRIDE` in `quality_flags` (with `FLAG_DIST_VALID`); `astrometry_quality` / `photometry_quality` are `0.0` placeholders. `drop` rows null payload columns in `OUTPUT_COLS` except `source` / `source_id` for targeting.

Rationale:

- Avoid mixing raw downloads with derived artifacts.
- Keep Gaia multi-file outputs discoverable via wildcard without pulling in non-Gaia files.
- Keep single-file products deterministic for stable pipeline wiring and easier auditing.

---

## Manual overrides

Overrides correct or supplement catalog data where automation is wrong or incomplete, for example:

- **Missing objects** — e.g. the Sun, or other bodies not represented as normal catalog rows.
- **Poor binary / composite solutions** — e.g. Hipparcos parameters for some resolved binaries; curated astrometry/photometry replaces the catalog row.

Open override candidates:

- TODO: create a catalogue override record for HIP 90910. It currently appears in
  `stars.octree` as a K5 star with absolute magnitude `-12.56` at level 2, so
  the source distance/parallax is likely unreasonable and should be corrected
  before octree enrichment.

Each override row should carry at least:

- `override_id` (stable string or integer)
- `action`: `add` | `replace` | `drop` (or equivalent)
- `override_reason` / category (e.g. `missing_star`, `binary_quality`, `manual_astrometry`)
- `override_policy_version` (for reproducibility)
- Target key to match existing rows: `source` + `source_id` (crossmatch partner lookup is handled by the merger; see "Pair-aware override resolution")
- For `add` / `replace`: fields required by the merged schema (coordinates, `mag_abs`, etc.) or pointers to a curated row definition

Override YAMLs may also include an **`identifiers`** block that populates the identifiers sidecar for that star. This is how manual stars (e.g. the Sun) receive labels:

```yaml
identifiers:
  proper_name: Sun
```

Supported identifier fields: `gaia_source_id`, `hip_id`, `hd`, `bayer`, `flamsteed`, `constellation`, `proper_name`. Only fields with a value need be listed. The identifiers pipeline reads these blocks and merges them into `identifiers_map.parquet` alongside Vizier-derived entries.

### Override target key

Override targeting uses the compound star key:

- `source`
- `source_id`

where `source` identifies the namespace/catalog (for example `gaia`, `hip`, or `manual`) and `source_id` is only unique within that namespace.

Action-specific minimums:

- `drop`: provide only target identity (`source`, `source_id`) plus audit metadata (`override_id`, `override_reason`, `override_policy_version`, `action`). No astrometry/photometry payload is required.
- `replace`: provide target identity plus replacement payload fields required by merged output.
- `add`: provide new identity (or enough information to mint canonical identity) plus payload fields required by merged output.

### Precedence

**Manual overrides outrank automatic merge decisions:**

1. `drop` / suppress — remove or exclude the targeted catalog row(s) from the merged dense output (or mark excluded per policy).
2. `replace` — substitute curated astrometry/photometry (and identity links) for the matched object; automatic Gaia-vs-Hip scoring does not apply to that conflict.
3. `add` — insert a row with a canonical `source_id` that may not exist in either catalog (namespace rules below).
4. Otherwise — apply **M1–M3** for unmatched and matched pairs.

For `drop`, matching is by exact compound key (`source`, `source_id`), and the resolved row is removed from dense output.

### Pair-aware override resolution

Override YAML targets a **single** row by (`source`, `source_id`). The merger resolves overrides at the **matched-pair** level using the crossmatch table (`gaia_hip_map.parquet`):

- If a `replace` or `drop` override targets one side of a crossmatch-linked pair, the **entire pair** is resolved by the override. The partner row from the other catalog is suppressed — it does not appear as an unmatched row or compete in quality scoring.
- Override authors do **not** need to supply the partner's ID. The crossmatch map already contains the relationship, and requiring manual partner lookups would be error-prone and redundant.
- The merge decisions sidecar (M5) records both the override action and the suppressed partner, e.g.: "HIP 71683 replaced by `manual.alpha_cen_a.replace.v1`; Gaia counterpart suppressed via crossmatch."
- For `add` overrides (which target `source=manual`), no crossmatch lookup applies — the row is inserted directly.

### Magnitude-limited Gaia subsets and missing crossmatch partners

Gaia staging data may be filtered with `[gaia] mag_limit` in the project file, so a Gaia `source_id` present in the crossmatch table may have **no corresponding row** in the actual Gaia batch files. The merger must handle this gracefully in both override and non-override paths.

**Non-override path (matched pairs with a missing side):**

- If the crossmatch links Gaia X ↔ HIP Y but Gaia X has no row in the Gaia data, the pair is treated as having only one present side. HIP Y becomes an **effectively unmatched** Hipparcos row (kept per M1). No quality scoring or winner selection occurs — there is no competitor.
- The converse (HIP side missing) is unlikely in practice but follows the same logic: the present Gaia row is kept as unmatched.

**Override path — target present, partner missing:**

- Override targets `source=hip, source_id=Y`. The crossmatch maps Y to Gaia X, but Gaia X was excluded by mag-limit. The override applies normally to the HIP row. There is no Gaia partner to suppress — this is not an error.

**Override path — target missing, partner present:**

- Override targets `source=gaia, source_id=X`, but X was excluded by mag-limit. The crossmatch maps X to HIP Y, which **does** exist. The merger must still resolve the pair: the override payload replaces or drops as specified, and HIP Y is suppressed. The override is not silently skipped just because its literal target catalog row is absent.

**Override path — neither side present:**

- Override targets a row where both the target and any crossmatch partner are absent from the loaded data. The merger emits a **warning** (the override has no effect) and records this in the merge decisions sidecar. This is not a fatal error — it can happen legitimately with aggressive magnitude filtering.

**Implementation requirement:** For every `replace` or `drop` override, the merger must look up the crossmatch table to find a partner on **either** side, then check which (if any) of the two rows actually exist in the loaded Gaia and HIP data. The override resolves whichever rows are present; missing rows are noted but do not block the override.

---

## Canonical identity

Each merged output row is identified by the compound key `(source, source_id)`. This pair is the single canonical identity used by sidecars and forward lookups.

### Identity rules

- **Matched pairs** (Gaia + Hipparcos linked by crossmatch): always `source = "hip"`, `source_id = <hip_number>`. The Hipparcos identifier is preferred because all human-facing identifiers (HD, Bayer, Flamsteed, proper names) are keyed by HIP number in the identifiers pipeline. The winning catalog's astrometry/photometry is used for the row's physical data, but the identity is always Hipparcos.
- **Gaia-only** (unmatched): `source = "gaia"`, `source_id = <gaia_dr3_id>`.
- **Hipparcos-only** (unmatched, no crossmatch partner): `source = "hip"`, `source_id = <hip_number>`.
- **Manual adds**: `source = "manual"`, `source_id = <manual_id>` (e.g. `"sun"`). String-valued manual IDs cannot collide with numeric catalog IDs.
- **Replaced rows**: canonical identity is always the **original target's** `(source, source_id)`. The override swaps the payload (coordinates, photometry, etc.) but does not remap identity.

### No separate ID columns

The dense merged table does **not** carry `gaia_source_id`, `hip_source_id`, or `catalog_source` as additional columns. The compound key `(source, source_id)` is the only identity on each row. Cross-catalog identifiers (e.g. the Gaia DR3 source ID for a Hipparcos star) are stored in the **identifiers sidecar**, not on the dense table. Data provenance (which catalog supplied the winning astrometry/photometry) is recorded in the **merge decisions** sidecar (M5).

---

## Duplicate resolution

### Definitions

- **Matched pair**: one Gaia row and one Hipparcos row linked by cross-match, where **both rows exist** in the loaded data. A crossmatch link where one side was excluded (e.g. by Gaia magnitude filtering) does not form a matched pair — the present row is treated as unmatched.
- **Unmatched Gaia**: Gaia row not present in any matched pair (either no crossmatch link, or the HIP partner is missing from the data).
- **Unmatched Hipparcos**: Hipparcos row not present in any matched pair (either no crossmatch link, or the Gaia partner is missing from the data).

### Rules

#### M1. Keep all unmatched rows

- Keep all unmatched Gaia rows (unless removed by a `drop` override).
- Keep all unmatched Hipparcos rows (unless removed by a `drop` override).

#### M2. Exactly one winner per matched pair

For each matched pair **not** resolved by a `replace` or `drop` override, compute quality scores and select exactly one winner.

#### M3. Deterministic tie-break

If quality scores tie, apply deterministic tie-break in this order:

1. Prefer Gaia.
2. Lower canonical `source_id` (under a fixed ordering).

#### M4. No duplicate canonical IDs

Merged dense output must not contain duplicate canonical `source_id`.

#### M5. Auditable decision record

Persist provenance for:

- Each **matched pair** decision: the Gaia and Hipparcos identifiers involved, winner catalog, score components (or aggregate), tie-break reason if applicable.
- Each **override**: `override_id`, `action`, target key `(source, source_id)`, `override_reason`, `override_policy_version`.

This sidecar is bounded by the crossmatch table (~100K rows) plus overrides (~tens of rows). **Unmatched rows are not recorded per-row** — they are counted in the aggregate merge report only. At ~1.5 billion stars, any per-row accumulation outside the matched-pair and override sets would exhaust memory.

Recommend a dedicated **merge decisions** sidecar (Parquet) rather than widening the main table.

---

## Quality scoring contract

### Guarded `astrometry_quality` comparison with vetoes and bright-star gate

Both the Gaia and Hipparcos pipelines produce `astrometry_quality` as a **fractional parallax error** (σ/π): `e_Plx / Plx` for Hipparcos (`hipparcos/astrometry.py`), `parallax_error / max(parallax, ε)` for Gaia DR3 (`gaia/astrometry.py`). For Bailer-Jones distances the analogous half-interval width is used. These values are directly comparable across catalogs: **lower is better**.

Gaia fallback tiers already carry sentinel quality values that sort correctly against real measurements:

- Tier A (primary): actual fractional error (typically < 1.0)
- Tier B (weak catalog fallback): `10.0`
- Tier C (photometric distance): `20.0`
- Tier D (synthetic prior): `50.0`

#### Decision tree

Winner selection for matched pairs (M2) applies these rules in order:

1. **Neighbour veto**: If `number_of_neighbours > 1` in the crossmatch table, the match is ambiguous (Gaia resolves what Hipparcos saw as one source into multiple candidates). Gaia wins automatically. Recorded as `tie_break_reason = "neighbour_veto"`.

2. **Hipparcos multiplicity veto**: If the Hipparcos solution type `Sn ≠ 5` (anything other than the standard 5-parameter single-star solution), the Hipparcos astrometry is suspect — it may be a photocentre, an acceleration solution, or a component solution for a multiple system. Gaia wins automatically. Recorded as `tie_break_reason = "hip_multiplicity"`.

3. **Bright-star gate with margin**: Hipparcos can only beat Gaia by a **significant margin** that depends on apparent brightness:

   | Apparent G magnitude | Margin | Meaning |
   |---|---|---|
   | G < 3.5 (very bright) | 1.0× | Hip wins if strictly better (Gaia often incomplete/problematic) |
   | 3.5 ≤ G < 6 (bright) | 0.6× | Hip score must be < 60% of Gaia's score |
   | G ≥ 6 (normal) | 0.5× | Hip score must be < 50% of Gaia's score |

   Apparent G is taken from `phot_g_mean_mag` when available; otherwise estimated from `mag_abs + 5*log10(r_pc / 10)`. If neither is available, the normal (strictest) margin applies.

4. **Tie-break**: If neither side wins after margin comparison, Gaia wins. Recorded as `tie_break_reason = "gaia_margin"` when Hip was better but did not meet the margin threshold.

#### Auxiliary columns for diagnostics

Both catalog pipelines carry auxiliary columns alongside the core `OUTPUT_COLS`:

- **Gaia**: `ruwe` (Renormalised Unit Weight Error), `phot_g_mean_mag` (apparent G-band magnitude)
- **Hipparcos**: `Sn` (new-reduction solution type), `Hpmag` (apparent Hipparcos magnitude)
- **Crossmatch**: `number_of_neighbours`, `angular_distance`

These auxiliary columns are **not** included in the merged dense output — they are inputs to the decision logic and appear in the **merge decisions sidecar** for diagnostic analysis:

| Decision column | Source | Purpose |
|---|---|---|
| `number_of_neighbours` | Crossmatch | Match ambiguity (veto if > 1) |
| `angular_distance_arcsec` | Crossmatch | Match quality diagnostic |
| `gaia_ruwe` | Gaia processed | RUWE > 1.4 flags suspect single-star astrometry |
| `gaia_phot_g_mean_mag` | Gaia processed | Apparent magnitude for bright-star gate |
| `hip_solution_type` | Hip processed | Hipparcos Sn (veto if ≠ 5) |
| `hip_apparent_mag` | Hip processed | Apparent Hpmag diagnostic |

The merger must expose:

- deterministic numeric score per candidate in the decisions sidecar
- all diagnostic auxiliary columns per matched pair
- reproducible output for the same input tables

#### Auxiliary-column tolerance

Catalog staging outputs are a **superset** of the core `OUTPUT_COLS` schema.
The merger validates that required columns are present with a subset check. If
optional auxiliary columns are missing, the merger defaults them to safe values
(NaN / None) that bypass vetoes and apply the strictest margin.

---

## Output schema contract

### Dense merged table (all stars)

Must satisfy Stage 00 requirements:

- `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc`
- `mag_abs`
- optional `teff`

Must include identity:

- `source` (catalog namespace: `"gaia"`, `"hip"`, or `"manual"`)
- `source_id` (canonical identifier within that namespace)

Must include lightweight spatial helpers needed for 2D sharding:

- `ra_deg`
- `dec_deg`
- `r_pc`

Must include quality and provenance metadata:

- `quality_flags`
- `astrometry_quality`
- `photometry_quality`

**Do not** carry cross-catalog IDs, HD, Bayer, or other rare designations on the dense table. Those belong in the **identifiers sidecar** produced by the identifiers pipeline, joinable via `(source, source_id)`.

Extra columns needed for Stage 00 must survive unchanged where Stage 00 uses `SELECT *` on shards.

### Within-shard ordering contract

Within-shard ordering (e.g. `r_pc` ascending for radial octree traversal) is **deferred to a downstream step**. At low HEALPix orders the densest shards can contain tens of millions of rows, making a full-shard sort incompatible with the streaming merge architecture. The merger emits rows in arrival order within each shard file; downstream consumers are responsible for any ordering they require.

### Sidecars

The merger produces the **merge decisions** sidecar (M5), keyed by matched pair or override.

Designation and identifier metadata is handled by the **identifiers pipeline** (`data/processed/identifiers_map.parquet`), which is a wide table keyed by the compound key `(source, source_id)`. It includes cross-catalog identifiers (e.g. the Gaia DR3 source ID for Hipparcos stars, obtained via the crossmatch table), catalog designations (`hd`, `bayer`, `flamsteed`, `constellation`), and proper names. Manual override stars that define an `identifiers` block in their YAML are included in the same map. Downstream consumers join the identifiers map via `(source, source_id)` on the dense merged table.

---

## Validation requirements

At ~1.5 billion rows, post-hoc full-catalog scans are not practical. Validations must either be checked incrementally during the streaming pass, verified by counter arithmetic on small state, or guaranteed by algorithm construction.

### Runtime validations (implemented or required)

1. **Row count consistency** — assert at the end of the merge that `rows_emitted_total == unmatched_gaia + unmatched_hip + matched_pairs_scored + override_replace_applied + override_add_applied`. All counters are maintained during the streaming pass; this is a self-consistency check, not a comparison against an external reference count.
2. **Drop override payload validation** — at override load time (~tens of rows), assert that each `drop` override carries no non-audit payload fields (geometry/photometry columns must be null). Cheap because it operates on the override table, not on the catalog.
3. **Emit merge summary counts** by category, winner catalog, and override action type (the merge report).
4. **Warn on no-effect overrides** — if any `replace` or `drop` override targets a row where neither the target nor its crossmatch partner exists in the loaded data, emit a warning and record the override in the decisions sidecar as `override_no_effect`.

### By construction (no runtime check needed)

5. **No duplicate canonical `(source, source_id)`** — guaranteed by the algorithm: Gaia source_ids are unique within Gaia (catalog guarantee), HIP source_ids are unique within HIP, matched pairs always use the HIP identity and emit exactly one winner, and `manual` namespace IDs are string-valued and cannot collide with numeric catalog IDs.
6. **All matched pairs resolved** — the streaming pass processes every Gaia row encountered in the batch files, and the HIP flush processes every remaining HIP row. Under magnitude-limited Gaia subsets, many crossmatch entries will reference absent Gaia rows; these are not matched pairs (see "Magnitude-limited Gaia subsets") and require no resolution.
7. **Non-null geometry/magnitude** — upstream pipeline responsibility. The Gaia and Hipparcos pipelines validate their own output; the merger does not re-check per-row data quality on 1.5B rows.

---

## Streaming merge architecture

The merger must process ~1.5 billion Gaia rows without materialising the full catalog in memory.

**In-memory lookup tables** (small, loaded once):

- HIP stars (~118K rows)
- Crossmatch table (`gaia_hip_map.parquet`)
- Overrides (~tens of rows)

**Streaming pass** (one Gaia batch file at a time):

1. Load lookup sets: which HIP `source_id`s are in the crossmatch, which are override targets, and which Gaia `source_id`s are in the crossmatch or targeted by overrides.
2. For each Gaia batch file:
   - Read the batch into memory (each batch is a manageable size).
   - For each Gaia row: check if it is part of a matched pair (crossmatch lookup) or targeted by an override.
     - **Matched pair, no override**: compare `astrometry_quality` with the HIP partner. Emit the winner's physical data under the HIP identity (`source = "hip"`, `source_id = <hip_number>`). Mark the HIP row as resolved.
     - **Override target**: apply the override. Mark the pair (if any) as resolved.
     - **Unmatched Gaia**: emit as-is.
   - Compute HEALPix pixel from the winning row's (`ra_deg`, `dec_deg`) and write to the appropriate output directory.
3. After all Gaia batches: flush remaining unresolved HIP rows (unmatched) and `add` override rows into their respective HEALPix directories.

**Tracking state across batches:** The merger maintains a set of resolved HIP `source_id`s (and resolved override IDs) across the streaming pass. Since the Gaia download guarantees each HEALPix pixel appears in only one input file, a given Gaia `source_id` should appear in at most one batch. The crossmatch and override lookups are dict/set operations against the in-memory tables.

---

## Operational outputs

Merger run must produce:

- **HEALPix-partitioned merged Parquet** — one directory per HEALPix pixel under `data/processed/merged/healpix/{pixel}/`, each containing one or more Parquet files. This is the input to downstream octree/index builds.
- **Merge report JSON** — **aggregate counts only**, not per-row data. Must include: counts by category (unmatched Gaia, unmatched HIP, matched-pair winners by catalog, override actions by type), HEALPix order and nside, and full input file manifest (Gaia directory, HIP path, crossmatch path, overrides path). This must remain a small fixed-size document regardless of catalog size. Do not accumulate per-row state for the ~1.5B unmatched Gaia rows.
- **Decision / override audit sidecar** (Parquet) — per M5, recording every **matched-pair decision** and **override action** including suppressed partners. This sidecar is bounded by the crossmatch table size (~100K rows) plus the override count (~tens of rows), not by the full catalog size. Unmatched rows (which make up the vast majority of the 1.5B Gaia catalog) are **not** recorded individually — they are accounted for only in the aggregate report counts.

---

## Resolved decisions

The following were resolved during merger development:

1. **Matched-pair winner policy** — Gaia wins by default. Hipparcos is only eligible under controlled conditions: standard single-star solution (Sn=5), unambiguous crossmatch (single neighbour), and a magnitude-dependent margin requirement. See "Quality scoring contract / Decision tree" above for the full rules.

2. **Canonical identity for `replace` actions** — Replacement rows always retain the original target's `source` / `source_id` as canonical identity. The override swaps the payload only; identity is never remapped.

3. **Override pair resolution** — Overrides target a single (`source`, `source_id`) key. When the target is one side of a crossmatch-linked pair, the merger resolves the entire pair via the crossmatch table; the partner is suppressed. Override authors do not supply partner IDs. When Gaia data is magnitude-limited, the merger handles missing crossmatch partners gracefully — an override still applies if either side of the pair exists in the data. See "Pair-aware override resolution" and "Magnitude-limited Gaia subsets" above.

---

## Non-normative notes

- A "merge plan now, apply later" strategy adds pipeline complexity with little benefit; current recommendation is direct merge after catalog preprocessing and before Stage 00.
- Full reverse lookup indices remain serving-layer artifacts.
- HEALPix level for **serving shards** may differ from HEALPix used only for **Gaia download batching**; document the level used for merged output.
- Mapping sidecars derived from Gaia catalog input can provide convenient Gaia↔HIP hints, but merge outputs remain the canonical source of truth for identity after overrides and winner selection.
