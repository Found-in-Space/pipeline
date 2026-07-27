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

For automatic matched-pair decisions, Gaia wins by default. Hipparcos can win
only when the crossmatch is unambiguous, the Hipparcos solution type is the
standard single-star solution, and its astrometry score beats Gaia by the
brightness-dependent margin used by the merge policy. The decision sidecar
records the winner, scores, vetoes, tie-break reason, and relevant auxiliary
columns.

Winner selection currently copies the complete staged Gaia or Hipparcos
physical payload. It does not select astrometry and render photometry from
different donors. The brightness-dependent margin normally uses raw Gaia G,
not the apparent magnitude reconstructed from both candidate render records.
Those limitations must be addressed before policy-neutral local pair evidence
is accepted automatically.

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
decisions. The current audit uses fractional radial-distance disagreement and
does not populate its declared apparent-magnitude comparison fields. It
therefore does not yet validate the proposed visual pairing policy. These
reports are publication evidence, not replacement inputs.

## Sidecars

Identifier data is deliberately sparse and stays out of the dense table. The
identifier pipeline writes a separate sidecar, and `merge quality-report` uses it
for labels in audit output.

Optional Gaia enrichment sidecars are emitted by the merge when Gaia
carry-through fields were present in staged Gaia data. Those fields follow the
matched Gaia counterpart even if Hipparcos or an override wins the dense
physical row. Override drops, unmatched Hipparcos rows, and manual-only
additions do not emit Gaia enrichment.
