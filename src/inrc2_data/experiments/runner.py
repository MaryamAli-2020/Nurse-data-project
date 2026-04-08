"""Top-level static and multistage experiment runners."""

from __future__ import annotations

import json
import sqlite3
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

from ..constants import DATASET_FAMILY_INRC_II
from ..utils import stable_id
from .evaluation import prepare_static_bundle
from .io import (
    enrich_static_bundle,
    find_static_bundle_path,
    get_multistage_instance_record,
    load_static_bundle,
    persist_multistage_run,
    persist_static_run,
    resolve_database_path,
)
from .solvers import StaticSolverConfig, solve_static_bundle


def run_static_experiment(
    project_root: Path,
    *,
    algorithm_name: str,
    variant_name: str,
    seed: int,
    bundle_id: str | None = None,
    bundle_path: Path | None = None,
    instance_code: str | None = None,
    case_code: str | None = None,
    local_search_iterations: int = 0,
    persist_to_db: bool = True,
    output_dir: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    db_path = resolve_database_path(project_root, explicit_path=db_path)
    if bundle_path is None:
        bundle_path = find_static_bundle_path(
            project_root,
            bundle_id=bundle_id,
            instance_code=instance_code,
            case_code=case_code,
        )
    raw_bundle = load_static_bundle(bundle_path)
    enriched_bundle = enrich_static_bundle(raw_bundle, db_path)
    prepared = prepare_static_bundle(enriched_bundle)
    config = StaticSolverConfig(seed=seed, local_search_iterations=local_search_iterations)

    started_at = time.perf_counter()
    solver_payload = solve_static_bundle(
        prepared,
        algorithm_name=algorithm_name,
        variant_name=variant_name,
        seed=seed,
        config=config,
    )
    runtime_sec = time.perf_counter() - started_at

    solver_run_id = stable_id(
        "solver_run",
        "static",
        algorithm_name,
        variant_name,
        enriched_bundle["bundle_id"],
        seed,
        int(time.time() * 1000),
    )
    result = {
        "solver_run_id": solver_run_id,
        "algorithm_name": algorithm_name,
        "variant_name": variant_name,
        "seed": seed,
        "runtime_sec": runtime_sec,
        "bundle_id": enriched_bundle["bundle_id"],
        "bundle_path": str(bundle_path),
        "instance_code": enriched_bundle["instance_code"],
        "dataset_family": enriched_bundle["dataset_family"],
        "assignments": solver_payload["assignments"],
        "evaluation": solver_payload["evaluation"],
    }

    output_dir = output_dir or (project_root / "data" / "processed" / "experiment_runs" / "static")
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{_safe_name(solver_run_id)}.json"
    report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    result["report_path"] = str(report_path)

    if persist_to_db:
        persist_static_run(db_path, enriched_bundle, result, report_path)
    return result


def run_multistage_experiment(
    project_root: Path,
    *,
    instance_code: str,
    algorithm_name: str,
    variant_name: str,
    seed: int,
    local_search_iterations: int = 0,
    persist_to_db: bool = True,
    output_dir: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    db_path = resolve_database_path(project_root, explicit_path=db_path)
    instance_record = get_multistage_instance_record(db_path, instance_code=instance_code)
    config = StaticSolverConfig(seed=seed, local_search_iterations=local_search_iterations)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    stage_results: list[dict[str, Any]] = []
    started_at = time.perf_counter()
    try:
        current_history_rows = deepcopy(instance_record["history_rows"])
        for stage_row in instance_record["stage_rows"]:
            bundle = _build_inrc_stage_bundle(
                conn,
                scenario_id=instance_record["scenario_id"],
                week_id=stage_row["week_id"],
                history_rows=current_history_rows,
                instance_code=instance_record["instance_code"],
                stage_index=stage_row["stage_index"],
            )
            prepared = prepare_static_bundle(
                bundle,
                treat_open_end_as_incomplete=stage_row["stage_index"] < len(instance_record["stage_rows"]) - 1,
            )
            stage_payload = solve_static_bundle(
                prepared,
                algorithm_name=algorithm_name,
                variant_name=variant_name,
                seed=seed + stage_row["stage_index"],
                config=config,
            )
            stage_assignments = _materialize_stage_assignments(bundle, stage_payload["assignments"])
            stage_results.append(
                {
                    "stage_index": stage_row["stage_index"],
                    "week_id": stage_row["week_id"],
                    "week_code": stage_row["week_code"],
                    "assignments": stage_assignments,
                    "evaluation": stage_payload["evaluation"],
                }
            )
            current_history_rows = _advance_history_rows(bundle, stage_payload["assignments"], current_history_rows)
    finally:
        conn.close()

    runtime_sec = time.perf_counter() - started_at
    evaluation = _aggregate_multistage_evaluations(stage_results)
    solver_run_id = stable_id(
        "solver_run",
        "multistage",
        algorithm_name,
        variant_name,
        instance_record["instance_code"],
        seed,
        int(time.time() * 1000),
    )
    result = {
        "solver_run_id": solver_run_id,
        "algorithm_name": algorithm_name,
        "variant_name": variant_name,
        "seed": seed,
        "runtime_sec": runtime_sec,
        "instance_code": instance_record["instance_code"],
        "instance_id": instance_record["instance_id"],
        "stage_results": stage_results,
        "assignments": [
            assignment
            for stage_row in stage_results
            for assignment in stage_row["assignments"]
        ],
        "evaluation": evaluation,
    }

    output_dir = output_dir or (project_root / "data" / "processed" / "experiment_runs" / "multistage")
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{_safe_name(solver_run_id)}.json"
    report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    result["report_path"] = str(report_path)

    if persist_to_db:
        persist_multistage_run(db_path, instance_record, result, report_path)
    return result


def _build_inrc_stage_bundle(
    conn: sqlite3.Connection,
    *,
    scenario_id: str,
    week_id: str,
    history_rows: list[dict[str, Any]],
    instance_code: str,
    stage_index: int,
) -> dict[str, Any]:
    nurse_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT n.nurse_id, n.nurse_code, c.contract_code
            FROM nurse AS n
            JOIN contract AS c ON c.contract_id = n.contract_id
            WHERE n.scenario_id = ?
            ORDER BY n.nurse_code
            """,
            (scenario_id,),
        ).fetchall()
    ]
    shift_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                shift_type_id,
                shift_code,
                min_consecutive_shift_assignments,
                max_consecutive_shift_assignments
            FROM shift_type
            WHERE scenario_id = ?
            ORDER BY shift_code
            """,
            (scenario_id,),
        ).fetchall()
    ]
    skill_rows = [
        dict(row)
        for row in conn.execute(
            "SELECT skill_id, skill_code FROM skill WHERE scenario_id = ? ORDER BY skill_code",
            (scenario_id,),
        ).fetchall()
    ]
    day_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT day_id, day_index, day_name, day_short_name, is_weekend
            FROM day
            WHERE week_id = ?
            ORDER BY day_index
            """,
            (week_id,),
        ).fetchall()
    ]
    week_row = conn.execute(
        "SELECT week_code, scenario_id FROM week WHERE week_id = ?",
        (week_id,),
    ).fetchone()
    contracts = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                contract_code,
                min_total_assignments,
                max_total_assignments,
                min_consecutive_work_days,
                max_consecutive_work_days,
                min_consecutive_days_off,
                max_consecutive_days_off,
                max_working_weekends,
                complete_weekend_required
            FROM contract
            WHERE scenario_id = ?
            ORDER BY contract_code
            """,
            (scenario_id,),
        ).fetchall()
    ]
    nurse_index = {row["nurse_id"]: index for index, row in enumerate(nurse_rows)}
    shift_index = {row["shift_type_id"]: index for index, row in enumerate(shift_rows)}
    skill_index = {row["skill_id"]: index for index, row in enumerate(skill_rows)}

    history_by_nurse_id = {row["nurse_id"]: row for row in history_rows}
    initial_history = []
    for nurse_row in nurse_rows:
        history_row = history_by_nurse_id[nurse_row["nurse_id"]]
        initial_history.append(
            {
                "staff_index": nurse_index[nurse_row["nurse_id"]],
                "nurse_id": nurse_row["nurse_id"],
                "last_shift_index": (
                    None
                    if history_row["last_shift_type_id"] is None
                    else shift_index[history_row["last_shift_type_id"]]
                ),
                "consecutive_same_shift_count": history_row["consecutive_same_shift_count"],
                "consecutive_work_days_count": history_row["consecutive_work_days_count"],
                "consecutive_days_off_count": history_row["consecutive_days_off_count"],
                "total_worked_shifts_so_far": history_row["total_worked_shifts_so_far"],
                "total_working_weekends_so_far": history_row["total_working_weekends_so_far"],
            }
        )

    bundle = {
        "bundle_id": stable_id("static_bundle", "multistage_stage", instance_code, stage_index),
        "dataset_family": DATASET_FAMILY_INRC_II,
        "problem_mode": "STATIC",
        "instance_id": None,
        "instance_code": f"{instance_code}|stage{stage_index}",
        "native_case_code": None,
        "counts": {
            "staff": len(nurse_rows),
            "skills": len(skill_rows),
            "shift_types": len(shift_rows),
            "days": len(day_rows),
            "coverage_rows": 0,
            "request_rows": 0,
            "fixed_assignments": 0,
        },
        "days": [
            {
                "day_id": row["day_id"],
                "day_index": row["day_index"],
                "day_name": row["day_name"],
                "day_short_name": row["day_short_name"],
                "is_weekend": row["is_weekend"],
            }
            for row in day_rows
        ],
        "staff": [
            {
                "staff_index": nurse_index[row["nurse_id"]],
                "staff_code": row["nurse_code"],
                "contract_code": row["contract_code"],
                "nurse_id": row["nurse_id"],
            }
            for row in nurse_rows
        ],
        "shifts": [
            {
                "shift_index": shift_index[row["shift_type_id"]],
                "shift_code": row["shift_code"],
                "min_consecutive_assignments": row["min_consecutive_shift_assignments"],
                "max_consecutive_assignments": row["max_consecutive_shift_assignments"],
                "shift_type_id": row["shift_type_id"],
            }
            for row in shift_rows
        ],
        "skills": [
            {
                "skill_index": skill_index[row["skill_id"]],
                "skill_code": row["skill_code"],
                "skill_id": row["skill_id"],
            }
            for row in skill_rows
        ],
        "contracts": contracts,
        "constraints": {
            "eligibility_edges": [
                {
                    "staff_index": nurse_index[row["nurse_id"]],
                    "skill_index": skill_index[row["skill_id"]],
                }
                for row in conn.execute(
                    """
                    SELECT nurse_id, skill_id
                    FROM nurse_skill
                    WHERE nurse_id IN (SELECT nurse_id FROM nurse WHERE scenario_id = ?)
                    ORDER BY nurse_id, skill_id
                    """,
                    (scenario_id,),
                ).fetchall()
            ],
            "forbidden_successions": [
                {
                    "prev_shift_index": shift_index[row["prev_shift_type_id"]],
                    "next_shift_index": shift_index[row["next_shift_type_id"]],
                    "is_forbidden": row["is_forbidden"],
                }
                for row in conn.execute(
                    """
                    SELECT prev_shift_type_id, next_shift_type_id, is_forbidden
                    FROM forbidden_shift_succession
                    WHERE scenario_id = ?
                    ORDER BY prev_shift_type_id, next_shift_type_id
                    """,
                    (scenario_id,),
                ).fetchall()
            ],
            "coverage_rows": [
                {
                    "day_index": row["day_index"],
                    "shift_index": shift_index[row["shift_type_id"]],
                    "skill_index": skill_index[row["skill_id"]],
                    "min_required": row["min_required"],
                    "optimal_required": row["optimal_required"],
                }
                for row in conn.execute(
                    """
                    SELECT d.day_index, cr.shift_type_id, cr.skill_id, cr.min_required, cr.optimal_required
                    FROM coverage_requirement AS cr
                    JOIN day AS d ON d.day_id = cr.day_id
                    WHERE cr.week_id = ?
                    ORDER BY d.day_index, cr.shift_type_id, cr.skill_id
                    """,
                    (week_id,),
                ).fetchall()
            ],
            "request_rows": [
                {
                    "staff_index": nurse_index[row["nurse_id"]],
                    "day_index": row["day_index"],
                    "request_type": row["request_type"],
                    "shift_index": (
                        None
                        if row["requested_off_shift_type_id"] is None
                        else shift_index[row["requested_off_shift_type_id"]]
                    ),
                }
                for row in conn.execute(
                    """
                    SELECT nr.nurse_id, d.day_index, nr.request_type, nr.requested_off_shift_type_id
                    FROM nurse_request AS nr
                    JOIN day AS d ON d.day_id = nr.day_id
                    WHERE nr.week_id = ?
                    ORDER BY nr.nurse_id, d.day_index
                    """,
                    (week_id,),
                ).fetchall()
            ],
            "fixed_assignments": [],
            "employee_shift_limits": [],
            "initial_history": initial_history,
        },
        "objective_weights": {},
        "metadata": {
            "dataset_id": None,
            "scenario_id": week_row["scenario_id"],
            "week_id": week_id,
            "history_id": stable_id("derived_history", instance_code, stage_index),
        },
    }
    bundle["counts"]["coverage_rows"] = len(bundle["constraints"]["coverage_rows"])
    bundle["counts"]["request_rows"] = len(bundle["constraints"]["request_rows"])
    return bundle


def _advance_history_rows(
    bundle: dict[str, Any],
    assignments: list[dict[str, Any]],
    current_history_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    shift_code_by_index = {
        row["shift_index"]: row["shift_code"]
        for row in bundle["shifts"]
    }
    shift_id_by_index = {
        row["shift_index"]: row["shift_type_id"]
        for row in bundle["shifts"]
    }
    assignments_by_staff_day = {
        (row["staff_index"], row["day_index"]): row
        for row in assignments
    }
    next_history_rows: list[dict[str, Any]] = []
    for staff_row in bundle["staff"]:
        staff_index = staff_row["staff_index"]
        previous_row = next(
            row for row in current_history_rows
            if row["nurse_id"] == staff_row["nurse_id"]
        )
        ordered_shift_codes = []
        for day_row in sorted(bundle["days"], key=lambda row: row["day_index"]):
            assignment = assignments_by_staff_day.get((staff_index, day_row["day_index"]))
            ordered_shift_codes.append(
                None if assignment is None else shift_code_by_index[assignment["shift_index"]]
            )

        worked_days = sum(1 for value in ordered_shift_codes if value is not None)
        worked_weekend = int(any(
            ordered_shift_codes[day_row["day_index"]] is not None
            for day_row in bundle["days"]
            if day_row["is_weekend"]
        ))
        trailing_days_off = 0
        for shift_code in reversed(ordered_shift_codes):
            if shift_code is None:
                trailing_days_off += 1
            else:
                break

        if trailing_days_off == len(ordered_shift_codes):
            last_shift_type_id = None
            consecutive_same_shift_count = 0
            consecutive_work_days_count = 0
            consecutive_days_off_count = previous_row["consecutive_days_off_count"] + len(ordered_shift_codes)
        elif trailing_days_off > 0:
            last_shift_type_id = None
            consecutive_same_shift_count = 0
            consecutive_work_days_count = 0
            consecutive_days_off_count = trailing_days_off
        else:
            trailing_shift_code = ordered_shift_codes[-1]
            assert trailing_shift_code is not None
            same_shift_run = 0
            for shift_code in reversed(ordered_shift_codes):
                if shift_code == trailing_shift_code:
                    same_shift_run += 1
                else:
                    break
            work_day_run = 0
            for shift_code in reversed(ordered_shift_codes):
                if shift_code is not None:
                    work_day_run += 1
                else:
                    break
            last_shift_type_id = shift_id_by_index[next(
                index for index, code in shift_code_by_index.items() if code == trailing_shift_code
            )]
            consecutive_same_shift_count = same_shift_run
            consecutive_work_days_count = work_day_run
            consecutive_days_off_count = 0

        next_history_rows.append(
            {
                "nurse_id": staff_row["nurse_id"],
                "nurse_code": staff_row["staff_code"],
                "last_shift_type_id": last_shift_type_id,
                "consecutive_same_shift_count": consecutive_same_shift_count,
                "consecutive_work_days_count": consecutive_work_days_count,
                "consecutive_days_off_count": consecutive_days_off_count,
                "total_worked_shifts_so_far": previous_row["total_worked_shifts_so_far"] + worked_days,
                "total_working_weekends_so_far": previous_row["total_working_weekends_so_far"] + worked_weekend,
            }
        )
    return next_history_rows


def _materialize_stage_assignments(
    bundle: dict[str, Any],
    assignments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    day_id_by_index = {row["day_index"]: row["day_id"] for row in bundle["days"]}
    nurse_id_by_index = {row["staff_index"]: row["nurse_id"] for row in bundle["staff"]}
    shift_id_by_index = {row["shift_index"]: row["shift_type_id"] for row in bundle["shifts"]}
    skill_id_by_index = {row["skill_index"]: row["skill_id"] for row in bundle["skills"]}
    stage_rows = []
    for assignment in assignments:
        stage_rows.append(
            {
                "staff_index": assignment["staff_index"],
                "day_index": assignment["day_index"],
                "shift_index": assignment["shift_index"],
                "skill_index": assignment["skill_index"],
                "day_id": day_id_by_index[assignment["day_index"]],
                "nurse_id": nurse_id_by_index[assignment["staff_index"]],
                "shift_type_id": shift_id_by_index[assignment["shift_index"]],
                "skill_id": skill_id_by_index[assignment["skill_index"]],
            }
        )
    return stage_rows


def _aggregate_multistage_evaluations(stage_results: list[dict[str, Any]]) -> dict[str, Any]:
    aggregated: dict[tuple[str, str, bool], dict[str, Any]] = {}
    final_only_codes = {"S_TOTAL_ASSIGNMENTS_MAX", "S_WORKING_WEEKENDS_MAX"}
    for stage_result in stage_results[:-1]:
        for row in stage_result["evaluation"]["constraint_results"]:
            if row["constraint_code"] in final_only_codes:
                continue
            key = (row["constraint_code"], row["evaluation_scope"], row["is_hard"])
            target = aggregated.setdefault(
                key,
                {
                    "constraint_code": row["constraint_code"],
                    "violation_count": 0,
                    "penalty_cost": 0.0,
                    "is_hard": row["is_hard"],
                    "evaluation_scope": "MULTISTAGE_ROLLOUT",
                    "week_id": None,
                },
            )
            target["violation_count"] += row["violation_count"]
            target["penalty_cost"] += row["penalty_cost"]

    final_stage = stage_results[-1]
    for row in final_stage["evaluation"]["constraint_results"]:
        key = (row["constraint_code"], row["evaluation_scope"], row["is_hard"])
        target = aggregated.setdefault(
            key,
            {
                "constraint_code": row["constraint_code"],
                "violation_count": 0,
                "penalty_cost": 0.0,
                "is_hard": row["is_hard"],
                "evaluation_scope": "MULTISTAGE_ROLLOUT",
                "week_id": final_stage["week_id"],
            },
        )
        target["violation_count"] += row["violation_count"]
        target["penalty_cost"] += row["penalty_cost"]

    constraint_results = list(aggregated.values())
    hard_violation_count = sum(row["violation_count"] for row in constraint_results if row["is_hard"])
    soft_penalty = sum(row["penalty_cost"] for row in constraint_results if not row["is_hard"])
    return {
        "status": "FEASIBLE" if hard_violation_count == 0 else "INFEASIBLE",
        "hard_violation_count": hard_violation_count,
        "soft_penalty": soft_penalty,
        "objective_value": hard_violation_count * 1_000_000.0 + soft_penalty,
        "constraint_results": constraint_results,
        "metrics": {
            "stage_count": len(stage_results),
            "assignment_count": sum(len(stage_result["assignments"]) for stage_result in stage_results),
        },
    }


def _safe_name(value: str) -> str:
    return "".join(character if character.isalnum() or character in "._-" else "_" for character in value)
