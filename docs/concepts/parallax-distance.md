# Parallax and Distance

Parallax is the apparent annual shift of a nearby star against distant
background objects. In the simple high-quality case, distance in parsecs is:

```text
distance_pc = 1000 / parallax_mas
```

Real catalog data is messier. Some parallaxes are noisy, missing, negative, or
poorly suited to simple inversion. The Gaia stage therefore uses a tiered
selection:

1. high-quality Gaia or Bailer-Jones catalog distances
2. weak but positive catalog fallbacks
3. photometric distance fallback
4. synthetic prior fallback

The selected distance source is recorded in `quality_flags`; the comparison
score is recorded in `astrometry_quality`.
