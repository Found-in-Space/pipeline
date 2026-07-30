# Distance resolution curation

`distance-resolution-v1.csv` is the tracked work list for the 81 stars whose
pipeline distance remains unsuitable for a release build after the automatic
astrometry policy is applied.

This directory is repository-only curation tooling and is not included in the
installed pipeline package. The CSV is not an executable override file, so
research results cannot accidentally change a build.
Each row records the canonical pipeline identity, useful aliases, the current
fallback, the reason external evidence was required, and the best distance
found so far.

All 81 targets now have a cited distance and uncertainty. The tracker contains
48 `resolved` rows and 33 `provisional` rows:

- `resolved` means the evidence is strong enough for override review. It is not
  automatically executable; promotion still requires an explicit versioned
  `replace` override selected explicitly by a controlled project.
- `provisional` means the row contains the best published or catalogued value
  currently available, but a conflict, component ambiguity, catalog quality
  flag, or membership question makes automatic promotion premature.
- `unresolved` remains a valid status for future additions, but no row in this
  release is unresolved.

The evidence preference is direct geometric, orbital, or maser work; then a
well-established physical system, cluster, or host-galaxy distance; then a
clean photo-astrometric catalog posterior; and finally a flagged or legacy
catalog estimate. The reference and caveat used for each choice are preserved
in the row.

Two systematic failures account for much of the original list. SIMBAD
identifiers recovered exact Gaia DR3 identities for 35 Hipparcos targets missed
by the official best-neighbour crossmatch, and targeted cone searches recovered
another three; only Alnilam lacks a usable Gaia primary. This replaces generic
multi-kpc EDSD fallbacks with object-specific estimates. Nineteen Magellanic
Cloud members instead require their LMC or SMC host distance: Galactic-prior
Gaia estimates for those stars are known foreground solutions and are
deliberately rejected. Specialist literature supplies the strongest values for
several bright variables and multiple systems.

Run the catalog search with:

```bash
uv run python -m tools.curation.distance_resolution_v1.research_cli
```

By default it queries SIMBAD, Gaia DR3 plus the Bailer-Jones EDR3 distance
table, and the StarHorse 2021 VizieR catalog. Results go to
`downloads/distance-resolution-v1/`; they are evidence for review and do not
modify this tracker or the executable overrides.

`distance-resolution-v1-resolved.yaml` is the explicit custom override release
candidate for the 48 `resolved` rows. It is deliberately kept outside
`overrides/data`, so the packaged default remains empty. The YAML
preserves the staged astrometry and temperature donors, rebases absolute
magnitude to keep the current apparent brightness at the reviewed distance, and
embeds the selected uncertainty, reference, notes, donor identities, and input
hashes.

Generate or cross-check it with:

```bash
uv run python -m tools.curation.distance_resolution_v1.build_overrides_cli --force
uv run python -m tools.curation.distance_resolution_v1.build_overrides_cli --check
```

The build requires the matching `official-upper-level-preflight` artifacts in
`downloads/`. A catalog publication should consume this YAML through an
explicit pinned source path and include the YAML, tracker, build report, and
input provenance in the publication directory.
