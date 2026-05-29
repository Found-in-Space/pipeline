from __future__ import annotations

from dataclasses import dataclass

DEFAULT_GAIA_CALCULATION_ENGINE = "dr3_ap_bj_core"


@dataclass(frozen=True, slots=True)
class GaiaCalculationEngine:
    name: str
    gaia_source_columns: tuple[str, ...]
    distance_columns: tuple[str, ...]
    astrophysical_parameter_columns: tuple[str, ...]

    @property
    def required_input_columns(self) -> tuple[str, ...]:
        return (
            *self.gaia_source_columns,
            *self.distance_columns,
            *self.astrophysical_parameter_columns,
        )

    @property
    def core_output_names(self) -> tuple[str, ...]:
        return self.required_input_columns

    @property
    def requires_distance_table(self) -> bool:
        return bool(self.distance_columns)

    @property
    def requires_astrophysical_parameters(self) -> bool:
        return bool(self.astrophysical_parameter_columns)

    def validate_input_columns(
        self, columns: set[str] | list[str] | tuple[str, ...]
    ) -> None:
        available = set(columns)
        missing = [
            column for column in self.required_input_columns if column not in available
        ]
        if missing:
            joined = ", ".join(missing)
            raise ValueError(
                f"Gaia calculation engine {self.name!r} missing required input "
                f"column(s): {joined}"
            )


DR3_AP_BJ_CORE_ENGINE = GaiaCalculationEngine(
    name=DEFAULT_GAIA_CALCULATION_ENGINE,
    gaia_source_columns=(
        "source_id",
        "ra",
        "dec",
        "parallax",
        "parallax_error",
        "pmra",
        "pmdec",
        "phot_g_mean_mag",
        "phot_bp_mean_mag",
        "phot_rp_mean_mag",
        "ruwe",
    ),
    distance_columns=(
        "r_med_geo",
        "r_lo_geo",
        "r_hi_geo",
        "r_med_photogeo",
        "r_lo_photogeo",
        "r_hi_photogeo",
    ),
    astrophysical_parameter_columns=(
        "mg_gspphot",
        "mg_gspphot_lower",
        "mg_gspphot_upper",
        "ag_gspphot",
        "teff_esphs",
        "teff_gspspec",
        "teff_espucd",
        "teff_gspphot",
    ),
)

GAIA_CALCULATION_ENGINES = (DR3_AP_BJ_CORE_ENGINE.name,)
_ENGINES_BY_NAME = {DR3_AP_BJ_CORE_ENGINE.name: DR3_AP_BJ_CORE_ENGINE}


def get_gaia_calculation_engine(name: str | None = None) -> GaiaCalculationEngine:
    engine_name = (name or DEFAULT_GAIA_CALCULATION_ENGINE).strip().lower()
    try:
        return _ENGINES_BY_NAME[engine_name]
    except KeyError as exc:
        raise ValueError(
            f"Unknown Gaia calculation engine {name!r}; expected one of "
            f"{', '.join(GAIA_CALCULATION_ENGINES)}"
        ) from exc
