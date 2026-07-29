# Sun Reference Policy

Status: accepted
Decision date: 2026-07-29

## Context

The demo pipeline added `manual:sun` at `(0, 0, 0)` so early viewers could
render the Sun through the same path as streamed catalog stars. That row mixed
three different concepts:

- the solar origin of the coordinate frame;
- published solar photometric and physical reference values; and
- membership in the external-star catalog and octree.

The first catalogued build is the point at which those concepts become a public
data contract. Gaia and Hipparcos do not omit the Sun because its catalog
distance is uncertain. The Sun is the observing/reference object and is outside
the scope of those external-star catalogs.

An octree row at zero distance is also exceptional. Apparent-magnitude and
near-field rendering need special handling, while different applications may
want a light source, a textured body, a marker, a custom stellar rendering, or
no visible Sun.

## Decision

The core merged stellar catalog and its octree exclude the Sun.

- The origin remains a coordinate-frame contract, not a catalog row.
- The pipeline has no packaged default overrides. Controlled builds select all
  override sources explicitly and record their versions and checksums.
- The reviewed poor-distance override dataset contains catalog corrections
  only. It does not include the Sun.
- Published solar reference values live in their own optional catalog
  publication rather than in pipeline code or a core octree.
- Applications that want a visible or interactive Sun load that publication,
  define their own Sun, or omit it.

The first standalone solar reference publication is Found in Space catalogs
release `20260729.1`. It records the recommended Johnson V absolute magnitude,
nominal effective temperature, citations, and a clearly labelled optional
session-origin placement. It is not a merge-ready pipeline override.

## Provider Direction

SkyKit should treat the Sun as the reference case for composing multiple star
providers:

- a high-throughput external-catalog octree provider;
- a small static provider for published reference objects such as the Sun;
- scenario providers for authored objects, such as the faint star used by the
  Astrophage demo; and
- application or user session providers.

Provider-qualified identity and explicit shadowing rules must prevent accidental
deduplication or replacement across providers. Rendering, picking, labels, and
navigation should work across the composed providers without requiring every
provider to use an octree.

## Consequences

- Future core builds no longer contain `manual:sun`.
- Historical demo octrees that contain the row remain valid historical
  artifacts.
- "Fly to Sun" and similar navigation target the coordinate origin directly;
  they do not require a Sun object to be present.
- Consumers must opt in when they want the published solar reference object.
- Solar reference citations no longer belong to the pipeline's scientific
  input set.
- A session can replace or restyle the Sun without rebuilding or mutating the
  external-star octree.
