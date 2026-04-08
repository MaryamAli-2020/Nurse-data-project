"""Command-line entry points for the INRC-II data foundation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the INRC-II canonical data foundation.")
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path.cwd(),
        help="Project root containing the INRC-II XML dataset folders.",
    )
    args = parser.parse_args()
    summary = run_pipeline(args.project_root)
    print(json.dumps(summary["validation"], indent=2))
    print(summary["database_path"])


if __name__ == "__main__":
    main()
