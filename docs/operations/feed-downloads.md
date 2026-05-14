# Feed Downloads (Gaia + Hipparcos)

This is the operational recipe for the download side of the star feed pipeline.
Use the repo commands where they exist; the Gaia all-sky batching notes remain a
manual recipe until Gaia download automation is added.

It covers:

- Hipparcos: one catalog download.
- Gaia: all-sky HEALPix-L3 counting, batch planning under a row cap, and per-batch async downloads.
- Identifiers: HIP/HD, Bayer/Flamsteed, and proper-name source catalogs used to build a wide sidecar.

## Prerequisites

- Python 3.13+.
- Project dependencies installed with `uv sync`.
- Gaia Archive account credentials for async TAP jobs.

## Output files

Recommended outputs:

- `hipparcos2.ecsv` (Hipparcos raw catalog).
- `hip_hd.ecsv` (HIP→HD from `I/239/hip_main`).
- `iv27a_catalog.ecsv` (`HIP`, `HD`, `Bayer`, `Fl`, `Cst` from `IV/27A/catalog`).
- `iv27a_proper_names.ecsv` (`HD`→proper names from `IV/27A/table3`).
- `identifiers_map.parquet` (wide sidecar keyed by `(source, source_id)`).
- `gaia-l3-all-sky-count.csv` (L3 tile counts + manual status).
- `gaia_batch_plan.csv` (optional: batch assignment for each `hp3`).
- `gaia_batch_<label>.vot.gz` (one file per Gaia download batch).
- `gaia_hipparcos2_best_neighbour.ecsv` (Gaia archive cross-match: Gaia `source_id` ↔ Hipparcos HIP).

## Gaia↔Hipparcos cross-match (`hipparcos2_best_neighbour`)

The main Gaia star feed query (above) does **not** include Hipparcos identifiers. The merger instead uses a small sidecar Parquet (`gaia_hip_map.parquet`) built from the Gaia archive table `gaiadr3.hipparcos2_best_neighbour` (~100k rows).

**In this repo:** run `fis-pipeline gaia-to-hip build`. Defaults: ECSV under `data/catalogs/`, Parquet at `data/processed/gaia_hip_map.parquet`.

**Standalone TAP recipe** (equivalent to `gaia-to-hip download`):

```python
from astroquery.gaia import Gaia

QUERY = """
SELECT
  source_id,
  original_ext_source_id,
  angular_distance,
  number_of_neighbours
FROM gaiadr3.hipparcos2_best_neighbour
"""

job = Gaia.launch_job(QUERY)
result = job.get_results()
result.write("gaia_hipparcos2_best_neighbour.ecsv", format="ascii.ecsv", overwrite=True)
```

`original_ext_source_id` is the Hipparcos-2 source id (the HIP number). Optional columns are kept in the ECSV for provenance; the pipeline sidecar uses only `source_id` and `original_ext_source_id`.

## Hipparcos workflow

**In this repo:** run `fis-pipeline hip build`.

## 1) Download Hipparcos

```python
from astroquery.vizier import Vizier

v = Vizier(columns=["*"], row_limit=-1)
tables = v.get_catalogs("I/311/hip2")  # Hipparcos New Reduction
hip_table = tables[0]
hip_table.write("hipparcos2.ecsv", format="ascii.ecsv", overwrite=True)
print(f"saved {len(hip_table):,} rows")
```

## 2) Process Hipparcos downstream

Processing is pipeline-specific, but typically:

1. Load Hipparcos.
2. Run astrometry quality selection.
3. Derive photometry + coordinates.


## Gaia workflow

Gaia jobs are partitioned by HEALPix level 3 (`hp3`) derived from `source_id`.

## 1) Build all-sky L3 count table

Use the same filter as your star download query, but aggregate to counts:

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

## 2) Normalize counts from CSV

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

## 3) Plan batches under the row limit

We use a batch limit of 55,000,000 here as a compromise between number of downloads, file size, 
and any limits imposed by the Gaia archive. This can be tuned to fit your use-case.

Use repeated "best subset under limit" (dynamic programming) to pack each batch close to the cap.

```python
from typing import List, Tuple

def best_subset_under_limit(values: List[int], limit: int) -> Tuple[List[int], int]:
    # reachable[sum] = list of chosen local indices producing this sum
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
    # Keep original positions so we can map back to hp3 later.
    remaining = list(enumerate(values))  # (original_idx, count)
    batches: List[List[int]] = []

    while remaining:
        local_values = [v for _, v in remaining]
        chosen_local, total = best_subset_under_limit(local_values, limit)

        # Fallback: if nothing non-empty fits, take the largest single tile.
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

## 4) Build per-batch Gaia query

For one batch, collect `hp3` values and inject into `IN (...)`.

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

## 5) Submit async Gaia job and save output

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

## 6) Mark completed batches

After a batch file is downloaded and validated, update `downloaded` for those `hp3` rows (for example, to `b1`) so re-planning skips them.

## 7) Optional verification: count query reproducibility

To verify your count query still matches your canonical CSV:

1. Re-run the all-sky count query.
2. Normalize both sides (`n` as integer, no commas).
3. Join on `hp3`.
4. Assert zero differences.

If differences occur, check for:

- changed Gaia release/table names,
- changed filter predicates,
- integer division/casting behavior in your TAP service.
