# Quality Flags

`quality_flags` is a packed unsigned integer. The bit layout is defined in
`foundinspace.pipeline.constants`.

## Distance Source

Bits 0-3 record the distance or astrometry source, including Gaia DR3 parallax,
Bailer-Jones distances, Hipparcos parallax, photometric fallback, synthetic
prior, and manual override rows.

## Temperature Source

Bits 4-6 record how `teff` was derived, from Gaia temperature products, color
fallbacks, or the default solar-type fallback.

## Status Bits

- `FLAG_DIST_VALID`: finite positive distance is available.
- `FLAG_NEEDS_REVIEW`: a fallback tier was used.
- `FLAG_DIST_PLAUSIBLE`: distance passes broad sanity bounds.

## Photometry Source

Bits 10-11 record whether the apparent magnitude came from Gaia G, Hipparcos Hp,
or another/unknown source.

Use the `qf_*` helpers in `foundinspace.pipeline.constants` when reading these
bits from Python.
