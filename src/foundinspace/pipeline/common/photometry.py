"""Photometric conversions for rendering: Teff ↔ color indices, Teff → RGB."""

import numpy as np

from foundinspace.pipeline.constants import TEFF_MAX_K, TEFF_MIN_K

TEFF_LOG8_MIN_K = 2000.0
TEFF_LOG8_MAX_K = 50000.0
TEFF_LOG8_SENTINEL = 255


def teff_to_rgb(teff_k: float | np.ndarray) -> tuple[float, float, float] | np.ndarray:
    """
    Convert effective temperature (K) to sRGB values (0–255).

    Uses Tanner Helland's algorithm (based on Mitchell Charity's CIE 1964
    blackbody data). Valid for 1000–40000 K; values outside are clamped.

    Returns:
        (r, g, b) for scalar input, or (n, 3) array for array input.
    """
    t = (
        np.atleast_1d(np.clip(np.asarray(teff_k, dtype=float), TEFF_MIN_K, TEFF_MAX_K))
        / 100.0
    )

    r = np.empty_like(t)
    g = np.empty_like(t)
    b = np.empty_like(t)

    # Red: <= 6600 K → 255, else power law
    mask_r_low = t <= 66
    r[mask_r_low] = 255
    r[~mask_r_low] = np.clip(
        329.698727446 * (t[~mask_r_low] - 60) ** -0.1332047592, 0, 255
    )

    # Green: <= 6600 K → log, else power law
    mask_g_low = t <= 66
    g[mask_g_low] = np.clip(
        99.4708025861 * np.log(t[mask_g_low]) - 161.1195681661, 0, 255
    )
    g[~mask_g_low] = np.clip(
        288.1221695283 * (t[~mask_g_low] - 60) ** -0.0755148492, 0, 255
    )

    # Blue: >= 6600 K → 255, <= 1900 K → 0, else log
    mask_b_high = t >= 66
    mask_b_low = t <= 19
    mask_b_mid = ~mask_b_high & ~mask_b_low
    b[mask_b_high] = 255
    b[mask_b_low] = 0
    b[mask_b_mid] = np.clip(
        138.5177312231 * np.log(t[mask_b_mid] - 10) - 305.0447927307, 0, 255
    )

    out = np.column_stack([r, g, b])
    if np.isscalar(teff_k):
        return tuple(float(x) for x in out[0])
    return out


def teff_to_hex(teff_k: float | np.ndarray) -> str | np.ndarray:
    """
    Convert effective temperature (K) to hex color string.

    Returns:
        "#rrggbb" for scalar input, or array of hex strings for array input.
    """
    rgb = teff_to_rgb(teff_k)
    if np.isscalar(teff_k):
        r, g, b = rgb
        return f"#{int(r):02x}{int(g):02x}{int(b):02x}"
    return np.array(
        [f"#{int(r):02x}{int(g):02x}{int(b):02x}" for r, g, b in rgb.astype(int)]
    )


def encode_teff_log8(teff_k: float | np.ndarray) -> int | np.ndarray:
    """Encode effective temperature (K) to the viewer/octree 8-bit log scale.

    Valid temperatures in the viewer payload are represented on a logarithmic
    scale from 2000 K to 50000 K. Invalid, non-finite, or out-of-range values
    use byte 255 as the viewer's "unknown/default" sentinel.
    """
    t = np.atleast_1d(np.asarray(teff_k, dtype=float))
    valid = np.isfinite(t) & (t >= TEFF_LOG8_MIN_K) & (t <= TEFF_LOG8_MAX_K)
    finite_t = np.where(np.isfinite(t), t, TEFF_LOG8_MIN_K)
    log_t = np.log10(np.clip(finite_t, TEFF_LOG8_MIN_K, TEFF_LOG8_MAX_K))
    log_lo = np.log10(TEFF_LOG8_MIN_K)
    log_hi = np.log10(TEFF_LOG8_MAX_K)
    encoded = np.floor(255.0 * (log_t - log_lo) / (log_hi - log_lo)).astype(np.uint8)
    out = np.where(valid, np.clip(encoded, 0, 254), TEFF_LOG8_SENTINEL).astype(np.uint8)
    if np.isscalar(teff_k):
        return int(out[0])
    return out


def bv_to_teff(bv: float | np.ndarray) -> float | np.ndarray:
    """
    Convert Johnson B–V color index to effective temperature (K).

    Ballesteros 2012 (EPL 97, 34008), blackbody-based:
    T = 4600 * (1/(0.92*(B-V)+1.7) + 1/(0.92*(B-V)+0.62))

    B–V outside [-0.5, 2.5] is clamped to avoid singularities.
    """
    bv = np.atleast_1d(np.clip(np.asarray(bv, dtype=float), -0.5, 2.5))
    teff = 4600 * (1 / (0.92 * bv + 1.7) + 1 / (0.92 * bv + 0.62))
    teff = np.clip(teff, TEFF_MIN_K, TEFF_MAX_K)
    return float(teff[0]) if teff.size == 1 else teff


def bp_rp_to_teff(bp_rp: float | np.ndarray) -> float | np.ndarray:
    """
    Convert Gaia BP–RP color index to effective temperature (K).

    Uses approximate conversion BP-RP → B-V (Gaia DR3 relation) then
    Ballesteros 2012 B-V → Teff. BP-RP outside [-0.5, 3.5] is clamped.
    """
    bp_rp = np.atleast_1d(np.clip(np.asarray(bp_rp, dtype=float), -0.5, 3.5))
    # Gaia BP-RP to Johnson B-V (approximate, from Gaia documentation)
    bv = 0.77 * bp_rp + 0.20
    return bv_to_teff(bv)
