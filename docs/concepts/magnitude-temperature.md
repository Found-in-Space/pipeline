# Magnitude and Temperature

The pipeline keeps an absolute magnitude and effective temperature for each
star because downstream renderers need brightness and color-like quantities.

Gaia rows prefer Gaia astrophysical parameters when they are valid. When they
are missing, the pipeline falls back to color-derived temperature estimates and
finally to a solar-type default. Hipparcos rows use the available Hipparcos
photometry and B-V color fallback.

These choices are not hidden: the temperature and photometry sources are packed
into `quality_flags`, while `photometry_quality` carries an uncertainty estimate
where one can be derived.
