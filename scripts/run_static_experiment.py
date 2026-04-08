"""Run a static rostering experiment on a SchedulingBenchmarks case or INRC weekly projection."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from inrc2_data.experiments import run_static_experiment


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a static rostering experiment.")
    parser.add_argument("--algorithm", choices=("greedy", "ga"), default="greedy")
    parser.add_argument("--variant", default="baseline")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--bundle-id")
    parser.add_argument("--bundle-path", type=Path)
    parser.add_argument("--instance-code")
    parser.add_argument("--case-code")
    parser.add_argument("--local-search-iterations", type=int, default=0)
    parser.add_argument("--no-db-persist", action="store_true")
    args = parser.parse_args()

    result = run_static_experiment(
        PROJECT_ROOT,
        algorithm_name=args.algorithm,
        variant_name=args.variant,
        seed=args.seed,
        bundle_id=args.bundle_id,
        bundle_path=args.bundle_path,
        instance_code=args.instance_code,
        case_code=args.case_code,
        local_search_iterations=args.local_search_iterations,
        persist_to_db=not args.no_db_persist,
    )
    print(json.dumps(
        {
            "solver_run_id": result["solver_run_id"],
            "instance_code": result["instance_code"],
            "dataset_family": result["dataset_family"],
            "status": result["evaluation"]["status"],
            "hard_violation_count": result["evaluation"]["hard_violation_count"],
            "soft_penalty": result["evaluation"]["soft_penalty"],
            "objective_value": result["evaluation"]["objective_value"],
            "runtime_sec": result["runtime_sec"],
            "report_path": result["report_path"],
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
