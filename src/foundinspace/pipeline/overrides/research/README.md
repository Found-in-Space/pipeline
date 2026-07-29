# Distance resolution tracker

`distance-resolution-v1.csv` is the tracked work list for the 81 stars whose
pipeline distance remains unsuitable for a release build after the automatic
astrometry policy is applied.

This is not an executable override file. The override loader reads YAML only
from `overrides/data`, so unresolved research rows cannot accidentally change a
build. Each row records the canonical pipeline identity, useful aliases, the
current fallback, and the concrete reason external evidence is required.

Set `status` to `resolved` only after filling `resolved_distance_pc`, its
uncertainty where available, and a durable bibliographic reference. A reviewed
resolved row can then be promoted to a versioned `replace` override in
`overrides/data`.

Run the catalog search with:

```bash
uv run python scripts/research_override_distances.py
```

By default it queries SIMBAD, Gaia DR3 plus the Bailer-Jones EDR3 distance
table, and the StarHorse 2021 VizieR catalog. Results go to
`downloads/distance-resolution-v1/`; they are evidence for review and do not
modify this tracker or the executable overrides.
