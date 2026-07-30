#!/usr/bin/env python3
"""Build or check the reviewed distance-resolution override YAML."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from tools.curation.distance_resolution_v1.promotion import (
    DEFAULT_PREFLIGHT_DIR,
    DEFAULT_RESOLVED_OVERRIDE_PATH,
    DEFAULT_TRACKER_PATH,
    build_resolved_distance_override_document,
    validate_resolved_override_against_tracker,
    write_resolved_distance_override_yaml,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create the 48-row reviewed distance override YAML from the "
            "81-star tracker and staged preflight data."
        )
    )
    parser.add_argument("--tracker", type=Path, default=DEFAULT_TRACKER_PATH)
    parser.add_argument("--preflight-dir", type=Path, default=DEFAULT_PREFLIGHT_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_RESOLVED_OVERRIDE_PATH)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify that the existing YAML equals a fresh deterministic build",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing output YAML",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.check:
        expected, build_report = build_resolved_distance_override_document(
            tracker_path=args.tracker,
            preflight_dir=args.preflight_dir,
        )
        actual = yaml.safe_load(args.output.read_text(encoding="utf-8"))
        if actual != expected:
            raise SystemExit(f"{args.output} differs from a fresh deterministic build")
        report = {
            **build_report,
            **validate_resolved_override_against_tracker(
                args.output,
                tracker_path=args.tracker,
            ),
            "deterministic_rebuild_match": True,
        }
    else:
        report = write_resolved_distance_override_yaml(
            args.output,
            tracker_path=args.tracker,
            preflight_dir=args.preflight_dir,
            overwrite=args.force,
        )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
