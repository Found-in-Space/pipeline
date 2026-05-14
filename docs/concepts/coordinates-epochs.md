# Coordinates and Epochs

Catalog sources do not always describe positions at the same epoch. The
pipeline normalizes positions to the Gaia DR3 reference epoch, J2016.0, then
writes both sky coordinates and Sun-centered Cartesian ICRS coordinates.

The fast coordinate path uses NumPy and applies proper motion without radial
velocity. Tests compare the fast path with the Astropy implementation for the
covered cases.

Downstream octree builders can derive their own spatial indexes from the
canonical Cartesian columns.
