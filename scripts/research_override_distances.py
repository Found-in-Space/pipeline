#!/usr/bin/env python3
"""Search external catalogs for the tracked unresolved stellar distances."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from foundinspace.pipeline.overrides.distance_research import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_TRACKER_PATH,
    SUPPORTED_SERVICES,
    run_distance_research,
)


def _service_list(value: str) -> tuple[str, ...]:
    services = tuple(item.strip().lower() for item in value.split(",") if item.strip())
    unknown = sorted(set(services) - set(SUPPORTED_SERVICES))
    if not services or unknown:
        raise argparse.ArgumentTypeError(
            "services must be a comma-separated subset of "
            + ",".join(SUPPORTED_SERVICES)
        )
    return services


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect SIMBAD, Gaia/Bailer-Jones, and StarHorse evidence for "
            "the 81-star distance-resolution tracker."
        )
    )
    parser.add_argument("--tracker", type=Path, default=DEFAULT_TRACKER_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--services",
        type=_service_list,
        default=SUPPORTED_SERVICES,
        help="comma-separated subset of simbad,gaia,starhorse",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-query services even when cached evidence CSVs exist",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="query only the first N tracker rows (use a separate output directory)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = run_distance_research(
        args.tracker,
        args.output_dir,
        services=args.services,
        force=args.force,
        limit=args.limit,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
