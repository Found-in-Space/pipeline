# Scientific Provenance

The staged catalog pipelines normalize Gaia and Hipparcos into the same dense
physical schema before merge. This document describes the calculations and the
fields they require. The code is authoritative; this page records the intended
scientific contract for auditing publication outputs.

## Source Catalogs

Gaia rows come from Gaia DR3 source data, the external Bailer-Jones Gaia EDR3
distance table, and Gaia DR3 astrophysical parameters. Hipparcos rows come from
the Hipparcos New Reduction catalog. Supporting feeds provide Gaia/Hipparcos
crossmatches, display identifiers, and curated manual overrides.

The canonical epoch is Gaia DR3 J2016.0. Hipparcos positions start at J1991.25
and are propagated to J2016.0 before merge.

## Gaia Calculation Engine

`[gaia].calculation_engine = "dr3_ap_bj_core"` is the current Gaia calculation
contract. The downloader query is generated from this engine, and `gaia build`
fails before processing a batch if any required field is missing.

Required Gaia input columns:

```text
source_id, ra, dec, parallax, parallax_error, pmra, pmdec,
phot_g_mean_mag, phot_bp_mean_mag, phot_rp_mean_mag, ruwe,
r_med_geo, r_lo_geo, r_hi_geo,
r_med_photogeo, r_lo_photogeo, r_hi_photogeo,
mg_gspphot, mg_gspphot_lower, mg_gspphot_upper, ag_gspphot,
teff_esphs, teff_gspspec, teff_espucd, teff_gspphot
```

This is a field-presence contract. Per-row values may still be null or invalid,
and calculation fallbacks handle those values explicitly. Configured Gaia carry
fields are also strict: if the source column is missing from the incoming batch
or stage dataframe, the build fails.

## Distance And Astrometry

Parallax distance is:

```text
distance_pc = 1000 / parallax_mas
```

The Gaia stage selects distance in tiers:

1. Primary quality-tested candidate: Bailer-Jones geometric or
   photogeometric distance. The score is the fractional half-width of the
   16th–84th percentile interval. Intervals must contain their positive median
   in the correct order. A small selection-only penalty favors the less
   model-dependent geometric posterior when widths are close; the reported
   `astrometry_quality` remains the unpenalized fractional interval width.
2. Weak catalog fallback: any positive photogeometric, geometric, or parallax
   distance that failed the primary quality test. Raw `1000/parallax` is in
   this review tier because the available Bailer-Jones products provide a
   positive-distance posterior and account for the Gaia EDR3 parallax zero
   point.
3. Photometric fallback from Gaia apparent G, GSP-Phot absolute `M_G`, and
   extinction `A_G`.
4. Synthetic prior from apparent G, extinction if valid, and fixed `M_G = 4.0`.

Hipparcos uses valid positive `Plx` and `e_Plx`; distance is `1000 / Plx` and
`astrometry_quality` is `e_Plx / Plx`. Rows with fractional parallax error
above `0.2` are retained because Hipparcos has no alternative distance product,
but are explicitly marked for review. Invalid Hipparcos parallax leaves the
distance as null and is removed from the staged output.

`astrometry_quality` is lower-is-better. A raw-parallax fallback retains its
fractional parallax error when available. Other Gaia fallback tiers use finite
sentinel scores: `10.0` for a catalog value without a valid interval, `20.0`
for photometric distance, and `50.0` for a synthetic prior.

## Magnitude And Temperature

Gaia apparent magnitude is Gaia G. Hipparcos apparent magnitude is Hp.

Absolute magnitude uses:

```text
distance_modulus = 5 * log10(distance_pc / 10)
M = apparent_mag - distance_modulus - extinction
```

Gaia absolute magnitude cascade:

1. valid Gaia `mg_gspphot`;
2. extinction-corrected Gaia G using valid `ag_gspphot`;
3. distance-modulus fallback without extinction correction.

Gaia `photometry_quality` is half the GSP-Phot magnitude interval when
`mg_gspphot_lower` and `mg_gspphot_upper` are valid. For magnitudes derived
from apparent Gaia G and distance it is approximately
`2.17 * astrometry_quality`; a GSP-Phot magnitude without a valid interval has
unknown (`NaN`) quality rather than a fabricated uncertainty.
GSP-Phot intervals are accepted only when the finite 16th percentile, median,
and 84th percentile are ordered correctly. A central GSP-Phot magnitude with
finite malformed bounds is rejected so the cascade can use Gaia G instead.
The photometry provenance bits distinguish a GSP-Phot absolute magnitude from
an absolute magnitude derived from Gaia G.

For visual comparisons between staged rows, use the apparent magnitude implied
by the pipeline render fields:

```text
apparent_render_mag = mag_abs + distance_modulus
```

This is the brightness the dense row implies for a viewer at Earth. It is not
generally identical to a raw Gaia G versus Hipparcos Hp comparison: G and Hp are
different passbands, and the Gaia absolute-magnitude cascade may use GSP-Phot
or extinction-corrected photometry. Pair-acceptance and bright-star safeguards
belong to the downstream pipeline policy described in
[Gaia–Hipparcos pairing resolution](pairing-resolution.md), not to the
policy-neutral evidence catalog.

Gaia effective temperature cascade:

1. valid Gaia Teff in priority order: `teff_esphs`, `teff_gspspec`,
   `teff_espucd`, `teff_gspphot`;
2. Gaia BP-RP color-derived Teff;
3. solar-type default `5800 K`.

Gaia does not use a Johnson B-V fallback. Hipparcos Teff uses B-V when present
and falls back to `5800 K`.

The BP-RP fallback first approximates Johnson B-V as:

```text
B-V = 0.77 * BP-RP + 0.20
```

Then both BP-RP-derived and Hipparcos B-V temperatures use the Ballesteros
blackbody approximation:

```text
T = 4600 * (1 / (0.92 * (B-V) + 1.7) + 1 / (0.92 * (B-V) + 0.62))
```

Color-derived temperatures are used only inside the documented approximation
domains: `BP-RP` from `-0.5` through `3.5`, and Johnson `B-V` from `-0.5`
through `2.5`. Finite out-of-domain values use the default rather than being
silently clamped and reported as measured color temperatures.

## Coordinates

Catalog rows are converted to Sun-centered Cartesian ICRS parsec coordinates.
The fast coordinate path propagates RA and Dec to J2016.0 using proper motion,
with no radial velocity applied. Output coordinates are:

```text
x_icrs_pc, y_icrs_pc, z_icrs_pc, ra_deg, dec_deg, r_pc
```

A missing proper-motion component uses a zero-motion fallback so it cannot
erase an otherwise valid position. In particular, a Gaia row already at
J2016.0 retains its catalog position even when motion is unavailable.

## Quality Flags

`quality_flags` is a packed unsigned integer:

- bits 0-3: distance source, including Gaia parallax, Bailer-Jones distances,
  Hipparcos parallax, fallback tiers, synthetic prior, and manual override;
- bits 4-6: temperature source;
- bit 7: finite positive distance exists;
- bit 8: fallback tier needs review;
- bit 9: distance is within broad plausibility bounds;
- bits 10-11: photometry source: Gaia G, Hipparcos Hp, or Gaia GSP-Phot.

Use the `qf_*` helpers in `foundinspace.pipeline.constants` when reading these
bits from Python.
