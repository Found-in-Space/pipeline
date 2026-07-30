# Output Contract

The publication output of this repository is a single Found in Space view of
stars, written as HEALPix-sharded Parquet under `[merge].output_dir`. It is
designed for downstream spatial/index builds without requiring those downstream
systems to understand Gaia, Hipparcos, crossmatch, or override policy.

## Dense Star Schema

Dense staged and merged star tables use `OUTPUT_COLS` from
`foundinspace.pipeline.constants`:

| Column | Meaning |
| --- | --- |
| `source` | Canonical namespace: `gaia`, `hip`, or `manual`. |
| `source_id` | Identifier within `source`. |
| `x_icrs_pc`, `y_icrs_pc`, `z_icrs_pc` | Sun-centered Cartesian ICRS position in parsecs. |
| `ra_deg`, `dec_deg`, `r_pc` | Sky position and distance at the canonical epoch. |
| `mag_abs` | Absolute magnitude for downstream rendering. |
| `teff` | Effective temperature in kelvin. |
| `quality_flags` | Packed provenance and validity bits. |
| `astrometry_quality` | Lower-is-better distance/astrometry comparison score. |
| `photometry_quality` | Magnitude uncertainty estimate. |

Staged catalog outputs may carry auxiliary decision columns. Gaia carries
`ruwe` and `phot_g_mean_mag`; Hipparcos carries `Sn` and `Hpmag`. The merged
dense shards keep only the canonical output columns.

## Merge Inputs

The merge consumes:

- staged Gaia Parquet files;
- staged Hipparcos Parquet;
- Gaia/Hipparcos crossmatch Parquet;
- processed override Parquet.

The default Gaia/Hipparcos map is Gaia DR3 `hipparcos2_best_neighbour`.
The current merge can also consume an explicitly supplied, pre-resolved
one-to-one map. The policy-neutral pairing evidence published by `../catalogs`
is not such a map: it records possible pairs and deliberately retains
many-to-one evidence. Pipeline acceptance policy must resolve that evidence
before merge. That resolution stage is not yet implemented; see
[Gaia–Hipparcos pairing resolution](pairing-resolution.md).

## Canonical Identity

Merged rows are keyed by `(source, source_id)`:

- matched Gaia/Hipparcos pairs use Hipparcos identity: `source = "hip"`;
- unmatched Gaia rows use `source = "gaia"`;
- unmatched Hipparcos rows use `source = "hip"`;
- manual additions use `source = "manual"`;
- replacements keep the original target identity and replace the physical
  payload only.

The dense table does not carry extra Gaia ID, HIP ID, HD, Bayer, Flamsteed,
constellation, or proper-name columns. Sparse identifiers live in the identifier
sidecar and join by `(source, source_id)`.

## Deduplication Policy

Unmatched rows are kept unless a `drop` override removes them. Matched pairs
emit exactly one dense row unless an override resolves the pair.

Manual overrides have highest precedence:

- `drop` removes the targeted row or crossmatch-linked pair;
- `replace` substitutes curated physical data and suppresses the partner row;
- `add` inserts a manual row.

The packaged Python runtime contains no hidden override rows. The standard
controlled-run profiles explicitly acquire Found in Space stellar overrides
release `20260730.2` from Zenodo using Version DOI
`10.5281/zenodo.21703732`, verify the pinned archive SHA-256 and publication
checksum manifest, and select both catalog YAML files. Projects may choose a
different published version or an explicit empty source list, but every source
choice and content hash remains visible in the project and command provenance.
Review targets and Gaia–Hipparcos pairing candidates remain evidence inputs
until a published override release selects them.

The Sun is not a core catalog row or pipeline override. The coordinate origin
is part of the frame contract, while a visible or interactive Sun is an
optional session object. Published solar reference values live in their own
catalog publication and are not merged automatically. See
`docs/sun-reference-policy.md`.

Distance exceptions researched outside the automatic policy are tracked in
`tools/curation/distance_resolution_v1/distance-resolution-v1.csv`.
Its 81 rows are deliberately non-executable: each records the exact Gaia/HIP
identity, current fallback, reason for research, selected distance and
uncertainty, durable reference, and any remaining caveat without changing a
build. `resolved` rows are ready for explicit override review; `provisional`
rows preserve the best current evidence but are not ready for automatic
promotion.
`tools.curation.distance_resolution_v1.research_cli` collects SIMBAD
measurements and bibcodes, Gaia DR3 astrometry and Bailer-Jones intervals, and
StarHorse photo-astrometric intervals into `downloads/distance-resolution-v1/`.
Promotion always requires a reviewed, versioned YAML `replace` override.

The 48 reviewed distance replacements are published together with the original
three Alpha Centauri overrides in the cumulative Zenodo release. Their
repository-side materialization is generated and checked against the tracker
and preflight donor payloads by
`tools.curation.distance_resolution_v1.build_overrides_cli`; runtime builds
consume the published catalog snapshot rather than that curation workspace.

For automatic matched-pair decisions, Gaia wins by default. Hipparcos can win
only when the crossmatch is unambiguous, the Hipparcos solution type is the
standard single-star solution, and its astrometry score beats Gaia by the
brightness-dependent margin used by the merge policy. The decision sidecar
records the winner, scores, vetoes, tie-break reason, and relevant auxiliary
columns.

Matched output construction selects fields independently:

- the existing brightness-, topology-, and solution-aware winner policy
  chooses the astrometry donor;
- the valid staged render magnitude with the lower finite
  `photometry_quality` is selected independently, with Gaia winning ties;
- Gaia measured/derived effective temperature is preferred, then Hipparcos
  color-derived temperature, then a default.

When astrometry and photometry have different donors, the photometry donor's
implied apparent render magnitude is preserved and its `mag_abs` is rebased to
the selected distance. The packed quality flags are composed from the
astrometry, photometry, and temperature donors rather than copied wholesale.
The decision sidecar records all three donors and whether magnitude rebasing
occurred. The brightness-dependent astrometry margin still normally uses raw
Gaia G; pair acceptance remains separate from output field construction.

## HEALPix Shards And Reports

Merged dense output is partitioned by HEALPix pixel:

```text
[merge].output_dir/
  healpix/{pixel}/0001.parquet
  merge_report.json
  merge_decisions.parquet
  merge_quality_report.json
  merge_quality_issues.parquet
```

The HEALPix pixel is computed from the winning row's `ra_deg` and `dec_deg`.
Within-shard row order is not part of this contract.

`merge_report.json` contains aggregate counts and the input manifest for the
run. `merge_decisions.parquet` records matched-pair and override decisions only;
unmatched Gaia rows are counted in aggregate rather than recorded per-row.

`merge quality-report` writes post-merge audit outputs for suspicious rows and
decisions. It records radial-distance disagreement, full Cartesian separation,
and the apparent render magnitude reconstructed from each staged candidate.
These reports are publication evidence, not replacement inputs or a
pair-acceptance decision.

## Sidecars

Identifier data is deliberately sparse and stays out of the dense table. The
identifier pipeline writes a separate sidecar, and `merge quality-report` uses it
for labels in audit output.

Optional Gaia enrichment sidecars are emitted by the merge when Gaia
carry-through fields were present in staged Gaia data. Those fields follow the
matched Gaia counterpart even if Hipparcos or an override wins the dense
physical row. Override drops, unmatched Hipparcos rows, and manual-only
additions do not emit Gaia enrichment.
