# Gaia–Hipparcos Pairing Resolution

The pairing-evidence catalog and the pipeline have deliberately different
responsibilities:

- `catalogs` records possible Gaia–Hipparcos pairs and factual measurements.
- `pipeline` decides which evidence pairs become one dense output record and
  which staged fields supply that record.

The policy-neutral `20260630.2` catalog is not a merge-ready crossmatch. Its
`gaia_hip_pairing_evidence.parquet` contains a many-to-one evidence union, while
the current merge accepts only a pre-resolved one-to-one map.

## Current Implementation Status

The current implemented path remains:

1. `gaia-to-hip build` creates the Gaia DR3
   `hipparcos2_best_neighbour` (H2BN) map and may combine explicitly supplied,
   already-resolved supplemental maps.
2. `merge build` validates that the selected map is one-to-one.
3. The merge chooses an astrometry donor for each accepted pair, independently
   selects the best valid staged render photometry and temperature, and retains
   Hipparcos canonical identity.

There is no `pairings resolve` command yet. Do not configure the policy-neutral
evidence union as the accepted `[gaia-to-hip].output_parquet` or as a
supplemental map: the one-to-one validation will reject it, and dropping
conflicting rows without a recorded policy would not be reproducible. The
project-level output is authoritative for merge, identifiers, and quality
reporting; the deprecated merge-only CLI option cannot select another map.

The current astrometry-winner policy still normally uses raw Gaia G for its
brightness-dependent margin. Output construction is field-level: when the
photometry donor differs, its apparent render magnitude is rebased to the
selected astrometric distance and both donors are recorded.

## Intended Resolution Stage

A future resolution stage should run after Gaia and Hipparcos staging, when the
pipeline calculation engine has produced comparable positions, distances,
render magnitudes, and quality measurements.

The proposed initial policy is:

1. Accept every H2BN pair and reserve both endpoints.
2. Consider a local-only evidence pair automatically only when:
   - neither endpoint conflicts with an H2BN reservation;
   - the recorded local topology is isolated and one-to-one;
   - the pipeline-derived radial gap is at most the configured visual threshold,
     provisionally `10 pc`;
   - the absolute difference between the two apparent render magnitudes is at
     most the configured safeguard, provisionally `2 mag`;
   - both staged rows contain the measurements required by the policy.
3. Keep both source rows when the evidence is ambiguous, conflicting, missing
   required measurements, or outside a threshold.
4. Treat pathological-distance correction as a separate recorded reason rather
   than silently broadening the visual-gap rule.

The provisional thresholds are pipeline policy, not catalog classifications.
They must be configurable and recorded in the resolution report so VR testing
can change them without regenerating the pairing-evidence publication.

## Visual Magnitude

The relevant brightness diagnostic is the apparent magnitude implied by the
pipeline render fields:

```text
apparent_render_mag =
    mag_abs + 5 * log10(r_pc / 10)
```

This compares what the dense rows imply for a viewer at Earth. It is not the
same as comparing raw Gaia G with raw Hipparcos Hp: the two source passbands
differ, and the Gaia `mag_abs` cascade may use GSP-Phot or extinction-corrected
photometry.

If astrometry and photometry donors are separated, the selected apparent render
magnitude must be rebased to the selected distance:

```text
selected_mag_abs =
    selected_apparent_render_mag - 5 * log10(selected_r_pc / 10)
```

The decision evidence should record the astrometry donor and photometry donor
separately.

## Exploratory Sizing

The following figures are background sizing results, not catalog products or
release validation counts.

From the controlled 124,207-row evidence union:

- `35,629` pairs have a catalog radial gap of at most `10 pc`;
- `28,068` of those are H2BN pairs;
- `7,561` are additional local-only pairs.

An exploratory Gaia subset calculation then reconstructed the pipeline apparent
render magnitude for those rows:

| Apparent render-magnitude difference | All pairs | Local-only pairs |
| --- | ---: | ---: |
| At most `1 mag` | 33,182 | 5,782 |
| More than `1 mag`, at most `2 mag` | 1,117 | 501 |
| At most `2 mag` | 34,299 | 6,283 |
| More than `2 mag` | 1,330 | 1,278 |

The exploratory subset output was deliberately not added to either repository
or to the pairing-evidence publication. These figures size the future pipeline
work; they are not controlled release evidence.

The `6,283` figure is therefore the corrected exploratory count for local-only
pairs with radial gap at most `10 pc` and pipeline apparent render-magnitude
difference at most `2 mag`. A raw Gaia G versus Hipparcos Hp comparison gives
`6,295`; that is a valid catalog-field calculation but not the visual metric the
pipeline should use.

Applying H2BN endpoint reservation and topology/conflict removal suggests that
roughly `5,000` local-only pairs may be automatically accepted. This is only a
planning estimate. The exact accepted count must come from the implemented
resolution stage using controlled staged inputs.

## Required Outputs

The resolution stage should produce:

- a deterministic accepted one-to-one map;
- a decision Parquet with one row for every input evidence pair, including
  accepted and rejected candidates;
- a JSON report containing the policy version, thresholds, input hashes,
  reason counts, missing-measurement counts, and accepted-map count;
- explicit astrometry, photometry, and temperature donor fields.

The accepted map must become a project-level input shared by merge,
identifiers, and quality reporting. A command-line override used only by
`merge build` is not sufficient for a controlled run.

## Readiness Gaps

Before the evidence catalog can drive a controlled merge, the pipeline still
needs:

- the resolution stage and its tests;
- a single configured accepted-map path used by every consumer;
- render-magnitude pair-acceptance safeguards;
- recorded input hashes and complete decision coverage.

Until those exist, H2BN or another independently resolved one-to-one map remains
the supported merge input.
