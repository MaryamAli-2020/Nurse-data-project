"""Run the first executable methodology suite: greedy, GA, and memetic-style variants."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from inrc2_data.experiments import run_multistage_experiment, run_static_experiment


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the implemented research methodology suite.")
    parser.add_argument("--mode", choices=("static", "multistage"), default="static")
    parser.add_argument("--case-code")
    parser.add_argument("--instance-code")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--no-db-persist", action="store_true")
    args = parser.parse_args()

    outputs = []
    if args.mode == "static":
        if not args.case_code and not args.instance_code:
            parser.error("Static mode requires --case-code or --instance-code.")
        outputs.append(
            run_static_experiment(
                PROJECT_ROOT,
                algorithm_name="greedy",
                variant_name="constructive_baseline",
                seed=args.seed,
                case_code=args.case_code,
                instance_code=args.instance_code,
                persist_to_db=not args.no_db_persist,
            )
        )
        outputs.append(
            run_static_experiment(
                PROJECT_ROOT,
                algorithm_name="ga",
                variant_name="plain_ga",
                seed=args.seed + 1,
                case_code=args.case_code,
                instance_code=args.instance_code,
                persist_to_db=not args.no_db_persist,
            )
        )
        outputs.append(
            run_static_experiment(
                PROJECT_ROOT,
                algorithm_name="ga",
                variant_name="memetic_ga",
                seed=args.seed + 2,
                case_code=args.case_code,
                instance_code=args.instance_code,
                local_search_iterations=20,
                persist_to_db=not args.no_db_persist,
            )
        )
    else:
        if not args.instance_code:
            parser.error("Multistage mode requires --instance-code.")
        outputs.append(
            run_multistage_experiment(
                PROJECT_ROOT,
                instance_code=args.instance_code,
                algorithm_name="greedy",
                variant_name="constructive_rollout",
                seed=args.seed,
                persist_to_db=not args.no_db_persist,
            )
        )
        outputs.append(
            run_multistage_experiment(
                PROJECT_ROOT,
                instance_code=args.instance_code,
                algorithm_name="ga",
                variant_name="ga_rollout",
                seed=args.seed + 1,
                persist_to_db=not args.no_db_persist,
            )
        )
        outputs.append(
            run_multistage_experiment(
                PROJECT_ROOT,
                instance_code=args.instance_code,
                algorithm_name="ga",
                variant_name="memetic_rollout",
                seed=args.seed + 2,
                local_search_iterations=12,
                persist_to_db=not args.no_db_persist,
            )
        )

    print(json.dumps(
        [
            {
                "solver_run_id": row["solver_run_id"],
                "variant_name": row["variant_name"],
                "status": row["evaluation"]["status"],
                "objective_value": row["evaluation"]["objective_value"],
                "runtime_sec": row["runtime_sec"],
                "report_path": row["report_path"],
            }
            for row in outputs
        ],
        indent=2,
    ))


if __name__ == "__main__":
    main()
