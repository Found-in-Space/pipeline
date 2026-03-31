# Gaia All-Sky Download

Design notes and code recipes for downloading Gaia DR3 star data with
Bailer-Jones distances. This documents the strategy worked out manually and
is intended as a spec for a future `fis-pipeline gaia download` CLI command.

All queries can be run directly in the
[Gaia Archive](https://gea.esac.esa.int/archive/) web interface (ADQL tab).

## Column reference

All columns come from `gaiadr3.gaia_source` (`g`) or
`external.gaiaedr3_distance` (`d`). The pipeline accesses missing columns
via `.get()` with NaN defaults, so downloads without the optional columns
still work — but the pipeline falls back to weaker estimates.

| Column | Source | Pipeline use |
|--------|--------|--------------|
| `source_id` | `g` | Primary key; HEALPix tile via `source_id / 2^53` |
| `ra`, `dec` | `g` | Sky position → coordinate propagation to J2016.0 |
| `parallax`, `parallax_error` | `g` | Tier A (DR3) and Tier B distance; quality scoring |
| `pmra`, `pmdec` | `g` | Proper motion for J2016.0 propagation |
| `phot_g_mean_mag` | `g` | Apparent magnitude; Tier C/D photometric distance; auxiliary output |
| `phot_bp_mean_mag`, `phot_rp_mean_mag` | `g` | Combined → `bp_rp` colour for Teff cascade |
| `ruwe` | `g` | Astrometric quality indicator (passed through as auxiliary output) |
| `mg_gspphot` | `g` | GSP-Phot absolute magnitude — preferred `mag_abs`; Tier C distance |
| `ag_gspphot` | `g` | GSP-Phot extinction — extinction-corrected `mag_abs`; Tier C distance |
| `mg_gspphot_upper`, `mg_gspphot_lower` | `g` | GSP-Phot M_G confidence bounds → `photometry_quality` |
| `teff_esphs` | `g` | Teff cascade priority 1 (ESP-HS) |
| `teff_gspspec` | `g` | Teff cascade priority 2 (GSP-Spec) |
| `teff_espucd` | `g` | Teff cascade priority 3 (ESP-UCD) |
| `teff_gspphot` | `g` | Teff cascade priority 4 (GSP-Phot) |
| `r_med_geo`, `r_lo_geo`, `r_hi_geo` | `d` | Bailer-Jones geometric distance + confidence interval |
| `r_med_photogeo`, `r_lo_photogeo`, `r_hi_photogeo` | `d` | Bailer-Jones photogeometric distance + confidence interval |

Without the GSP-Phot columns, every star uses the simple distance-modulus
path for `mag_abs` and gets NaN `photometry_quality`. Without the Teff
columns, every star falls through to the BP-RP colour estimate or the 5800 K
default.

## Magnitude-limited download (small dataset)

With an apparent-magnitude cap the result set is small enough for a single
query — no partitioning or batch planning needed.

| Limit  | Rows / Stars | Size  | Auth required? |
|--------|--------------|-------|----------------|
| G ≤ 9  |    175,485   |  16MB | No             |
| G ≤ 12 |  3,062,324   | 277Mb | Yes            |
| G ≤ 15 | 36,635,159   | 3.2Gb | Yes            |


The Gaia Archive public API caps query results at ~3 million rows. G ≤ 12
exceeds this, so a registered account is required (free; login via the
archive UI or `Gaia.login()` in astroquery). G ≤ 9 fits comfortably within
the anonymous limit.

```sql
SELECT
  g.source_id,
  g.ra,
  g.dec,
  g.parallax,
  g.parallax_error,
  g.pmra,
  g.pmdec,
  g.phot_g_mean_mag,
  g.phot_bp_mean_mag,
  g.phot_rp_mean_mag,
  g.ruwe,
  g.mg_gspphot,
  g.ag_gspphot,
  g.mg_gspphot_upper,
  g.mg_gspphot_lower,
  g.teff_esphs,
  g.teff_gspspec,
  g.teff_espucd,
  g.teff_gspphot,
  d.r_med_geo,
  d.r_lo_geo,
  d.r_hi_geo,
  d.r_med_photogeo,
  d.r_lo_photogeo,
  d.r_hi_photogeo
FROM gaiadr3.gaia_source AS g
JOIN external.gaiaedr3_distance AS d
  ON d.source_id = g.source_id
WHERE
  g.astrometric_params_solved IN (31, 95)
  AND (
    d.r_med_photogeo IS NOT NULL
    OR d.r_med_geo IS NOT NULL
  )
  AND g.phot_g_mean_mag <= 9.0
```

Adjust the magnitude limit to taste. The result can be downloaded as VOTable
(gzipped) directly from the archive UI, or via `astroquery`:

```python
from astroquery.gaia import Gaia

job = Gaia.launch_job_async(query, output_format="votable_gzip")
result = job.get_results()
result.write("gaia_bright.vot.gz", format="votable", overwrite=True)
```

## Full-sky download (batched)

Without a magnitude limit the query returns ~1.3 billion rows — far too large
for a single TAP job. The data must be partitioned.

**Partitioning key:** HEALPix level 3, extracted from `source_id` via integer
division (`source_id / 2^53`). Level 3 gives 768 tiles. Star counts vary
wildly across the sky (galactic plane vs. poles), so tiles are grouped into
batches that stay under a row cap.

**Batch target:** ~55 million rows per batch — a compromise between the number
of async jobs, output file size, and Gaia archive limits.

### Output files

- `gaia-l3-all-sky-count.csv` — L3 tile counts + manual download-status column.
- `gaia_batch_plan.csv` — batch assignment for each `hp3` tile (optional intermediate).
- `gaia_batch_<label>.vot.gz` — one VOTable per download batch.

### 1) Build all-sky L3 count table

Use the same filters as the star download query, but aggregate to counts per
tile:

```sql
SELECT
  (g.source_id / 9007199254740992) AS hp3,
  COUNT(*) AS n
FROM gaiadr3.gaia_source AS g
JOIN external.gaiaedr3_distance AS d
  ON d.source_id = g.source_id
WHERE
  g.astrometric_params_solved IN (31, 95)
  AND (
    d.r_med_photogeo IS NOT NULL
    OR d.r_med_geo IS NOT NULL
  )
GROUP BY 1
ORDER BY n DESC;
```

Export the result to CSV and add a manual status column:

```csv
hp3,n,downloaded
451,"1,234,567",
...
```

Notes:

- `9007199254740992 = 2^53`, used to extract the L3 partition from Gaia `source_id`.
- `downloaded` is a manual marker (`b1`, `b2`, ...), blank for pending tiles.

### 2) Normalise counts from CSV

```python
import numpy as np
import pandas as pd

df = pd.read_csv("gaia-l3-all-sky-count.csv")
df.columns = [c.strip() for c in df.columns]
df["count"] = pd.to_numeric(
    df["n"].astype(str).str.replace(r"[,\s]+", "", regex=True),
    errors="coerce",
)
df["to_download"] = np.where(df["downloaded"].isna(), df["count"], 0)
```

### 3) Plan batches under the row limit

Star counts per L3 tile are very uneven, so a simple sequential split wastes
capacity. Instead we use a repeated "best subset under limit" (dynamic
programming) packing — each batch is filled as close to 55 M as possible
before moving to the next.

```python
from typing import List, Tuple

def best_subset_under_limit(values: List[int], limit: int) -> Tuple[List[int], int]:
    reachable = {0: []}
    for i, v in enumerate(values):
        nxt = dict(reachable)
        for s, idxs in reachable.items():
            t = s + int(v)
            if t <= limit and t not in nxt:
                nxt[t] = idxs + [i]
        reachable = nxt
    best_sum = max(reachable.keys())
    return reachable[best_sum], best_sum


def batch_under_limit(values: List[int], limit: int = 55_000_000) -> List[List[int]]:
    remaining = list(enumerate(values))  # (original_idx, count)
    batches: List[List[int]] = []

    while remaining:
        local_values = [v for _, v in remaining]
        chosen_local, total = best_subset_under_limit(local_values, limit)

        if not chosen_local:
            max_i = max(range(len(local_values)), key=lambda i: local_values[i])
            chosen_local = [max_i]
            total = local_values[max_i]

        chosen_set = set(chosen_local)
        batch_orig_indices = [remaining[i][0] for i in chosen_local]
        batches.append(batch_orig_indices)
        remaining = [x for i, x in enumerate(remaining) if i not in chosen_set]

        print(f"batch {len(batches)} rows={total:,} waste={limit-total:,}")

    return batches
```

Run planning:

```python
pending = (
    df[df["downloaded"].isna()][["hp3", "count"]]
    .sort_values("count", ascending=True)
    .reset_index(drop=True)
)

batches = batch_under_limit(pending["count"].tolist(), limit=55_000_000)
```

Assign labels and save a plan:

```python
pending["batch"] = ""
for i, member_indices in enumerate(batches, start=1):
    pending.loc[member_indices, "batch"] = f"b{i}"

pending.to_csv("gaia_batch_plan.csv", index=False)
```

### 4) Build per-batch Gaia query

For one batch, collect `hp3` values and inject into `IN (...)`:

```python
batch_label = "b1"
levels = pending.loc[pending["batch"] == batch_label, "hp3"].astype(int).tolist()
formatted_levels = ",".join(str(h) for h in levels)

QUERY_TEMPLATE = """
SELECT
  g.source_id,
  g.ra,
  g.dec,
  g.parallax,
  g.parallax_error,
  g.pmra,
  g.pmdec,
  g.phot_g_mean_mag,
  g.phot_bp_mean_mag,
  g.phot_rp_mean_mag,
  g.ruwe,
  g.mg_gspphot,
  g.ag_gspphot,
  g.mg_gspphot_upper,
  g.mg_gspphot_lower,
  g.teff_esphs,
  g.teff_gspspec,
  g.teff_espucd,
  g.teff_gspphot,
  d.r_med_geo,
  d.r_lo_geo,
  d.r_hi_geo,
  d.r_med_photogeo,
  d.r_lo_photogeo,
  d.r_hi_photogeo
FROM gaiadr3.gaia_source AS g
JOIN external.gaiaedr3_distance AS d
  ON d.source_id = g.source_id
WHERE
  g.astrometric_params_solved IN (31, 95)
  AND (
    d.r_med_photogeo IS NOT NULL
    OR d.r_med_geo IS NOT NULL
  )
  AND g.source_id / 9007199254740992 IN ({formatted_levels})
"""

query = QUERY_TEMPLATE.format(formatted_levels=formatted_levels)
```

### 5) Submit async Gaia job and save output

```python
from astroquery.gaia import Gaia

# Gaia.login(user="your_username", password="your_password")
job = Gaia.launch_job_async(query, output_format="votable_gzip", background=True)
print("job id:", job.jobid)
```

When complete:

```python
result = job.get_results()
result.write(f"gaia_batch_{batch_label}.vot.gz", format="votable", overwrite=True)
```

Repeat for each batch label.

### 6) Mark completed batches

After a batch file is downloaded and validated, update `downloaded` for those
`hp3` rows (e.g. to `b1`) so re-planning skips them.

### 7) Verification

To verify a count query still matches the canonical CSV:

1. Re-run the all-sky count query.
2. Normalise both sides (`n` as integer, no commas).
3. Join on `hp3`.
4. Assert zero differences.

If differences occur, check for changed Gaia release/table names, changed
filter predicates, or integer division/casting behaviour in the TAP service.
