"""Solver-friendly derived feature bundles for static and multi-stage roster instances."""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from .constants import DATASET_FAMILY_INRC_II, DATASET_FAMILY_SCHEDULING_BENCHMARKS
from .models import StaticInstanceBundle
from .scheduling_benchmarks import derive_forbidden_successions
from .utils import stable_id


def build_feature_bundles(db_path: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    multistage_dir = output_dir / "multistage"
    static_dir = output_dir / "static"
    multistage_dir.mkdir(parents=True, exist_ok=True)
    static_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    manifest: dict[str, Any] = {"multistage": {}, "static": {}}

    multistage_instances = list(
        conn.execute(
            """
            SELECT instance_id, instance_code, scenario_id
            FROM instance
            WHERE dataset_family = ? AND problem_mode = 'MULTISTAGE'
            ORDER BY instance_id
            """,
            (DATASET_FAMILY_INRC_II,),
        ).fetchall()
    )
    for instance_row in multistage_instances:
        bundle = _build_multistage_instance_bundle(
            conn,
            instance_row["instance_id"],
            instance_row["instance_code"],
            instance_row["scenario_id"],
        )
        file_name = _safe_file_name(instance_row["instance_id"]) + ".json"
        target_path = multistage_dir / file_name
        target_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
        manifest["multistage"][instance_row["instance_id"]] = str(target_path)

    static_cases = list(
        conn.execute(
            """
            SELECT i.instance_id, i.instance_code, i.native_case_code
            FROM instance AS i
            WHERE i.dataset_family = ? AND i.problem_mode = 'STATIC'
            ORDER BY i.native_case_code
            """,
            (DATASET_FAMILY_SCHEDULING_BENCHMARKS,),
        ).fetchall()
    )
    for case_row in static_cases:
        bundle = _build_scheduling_case_bundle(
            conn,
            case_row["instance_id"],
            case_row["instance_code"],
            case_row["native_case_code"],
        )
        target_path = static_dir / (_safe_file_name(bundle.bundle_id) + ".json")
        target_path.write_text(json.dumps(bundle.to_dict(), indent=2), encoding="utf-8")
        manifest["static"][bundle.bundle_id] = str(target_path)

    inrc_static_projections = list(
        conn.execute(
            """
            SELECT
                s.scenario_id,
                s.dataset_id,
                w.week_id,
                w.week_code,
                hs.history_id,
                hs.history_variant_code
            FROM scenario AS s
            JOIN week AS w ON w.scenario_id = s.scenario_id
            JOIN history_snapshot AS hs
                ON hs.scenario_id = s.scenario_id
               AND hs.snapshot_type = 'INITIAL_SOURCE'
            ORDER BY s.scenario_id, CAST(hs.history_variant_code AS INTEGER), w.week_index
            """
        ).fetchall()
    )
    for projection_row in inrc_static_projections:
        bundle = _build_inrc_static_bundle(
            conn,
            projection_row["scenario_id"],
            projection_row["dataset_id"],
            projection_row["week_id"],
            projection_row["week_code"],
            projection_row["history_id"],
            projection_row["history_variant_code"],
        )
        target_path = static_dir / (_safe_file_name(bundle.bundle_id) + ".json")
        target_path.write_text(json.dumps(bundle.to_dict(), indent=2), encoding="utf-8")
        manifest["static"][bundle.bundle_id] = str(target_path)

    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    conn.close()
    return manifest


def _safe_file_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _build_multistage_instance_bundle(
    conn: sqlite3.Connection,
    instance_id: str,
    instance_code: str,
    scenario_id: str,
) -> dict[str, Any]:
    nurses = list(
        conn.execute(
            "SELECT nurse_id, nurse_code FROM nurse WHERE scenario_id = ? ORDER BY nurse_code",
            (scenario_id,),
        ).fetchall()
    )
    skills = list(
        conn.execute(
            "SELECT skill_id, skill_code FROM skill WHERE scenario_id = ? ORDER BY skill_code",
            (scenario_id,),
        ).fetchall()
    )
    shifts = list(
        conn.execute(
            "SELECT shift_type_id, shift_code FROM shift_type WHERE scenario_id = ? ORDER BY shift_code",
            (scenario_id,),
        ).fetchall()
    )
    days = list(
        conn.execute(
            """
            SELECT stage_index, global_day_index, week_id, day_id, day_index, day_name, day_short_name, is_weekend
            FROM instance_day_view
            WHERE instance_id = ?
            ORDER BY stage_index, day_index
            """,
            (instance_id,),
        ).fetchall()
    )
    initial_history = list(
        conn.execute(
            """
            SELECT
                n.nurse_id,
                n.nurse_code,
                nhs.last_shift_type_id,
                nhs.consecutive_same_shift_count,
                nhs.consecutive_work_days_count,
                nhs.consecutive_days_off_count,
                nhs.total_worked_shifts_so_far,
                nhs.total_working_weekends_so_far
            FROM history_snapshot AS hs
            JOIN nurse_history_state AS nhs ON nhs.history_id = hs.history_id
            JOIN nurse AS n ON n.nurse_id = nhs.nurse_id
            WHERE hs.instance_id = ?
              AND hs.snapshot_type = 'INSTANCE_INITIAL'
              AND hs.week_index_before_solve = 0
            ORDER BY n.nurse_code
            """,
            (instance_id,),
        ).fetchall()
    )

    nurse_index = {row["nurse_id"]: index for index, row in enumerate(nurses)}
    skill_index = {row["skill_id"]: index for index, row in enumerate(skills)}
    shift_index = {row["shift_type_id"]: index for index, row in enumerate(shifts)}
    day_index = {row["day_id"]: row["global_day_index"] for row in days}

    eligibility = [
        {"nurse_index": nurse_index[row["nurse_id"]], "skill_index": skill_index[row["skill_id"]]}
        for row in conn.execute(
            """
            SELECT nurse_id, skill_id
            FROM nurse_skill
            WHERE nurse_id IN (SELECT nurse_id FROM nurse WHERE scenario_id = ?)
            ORDER BY nurse_id, skill_id
            """,
            (scenario_id,),
        ).fetchall()
    ]

    forbidden_matrix = [
        {
            "prev_shift_index": shift_index[row["prev_shift_type_id"]],
            "next_shift_index": shift_index[row["next_shift_type_id"]],
            "is_forbidden": int(row["is_forbidden"]),
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
    ]

    coverage_rows = [
        {
            "stage_index": row["stage_index"],
            "global_day_index": day_index[row["day_id"]],
            "day_name": row["day_name"],
            "shift_index": shift_index[row["shift_type_id"]],
            "skill_index": skill_index[row["skill_id"]],
            "min_required": row["min_required"],
            "optimal_required": row["optimal_required"],
        }
        for row in conn.execute(
            """
            SELECT
                iwm.stage_index,
                cr.day_id,
                d.day_name,
                cr.shift_type_id,
                cr.skill_id,
                cr.min_required,
                cr.optimal_required
            FROM instance_week_map AS iwm
            JOIN coverage_requirement AS cr ON cr.week_id = iwm.week_id
            JOIN day AS d ON d.day_id = cr.day_id
            WHERE iwm.instance_id = ?
            ORDER BY iwm.stage_index, d.day_index, cr.shift_type_id, cr.skill_id
            """,
            (instance_id,),
        ).fetchall()
    ]

    request_rows = [
        {
            "stage_index": row["stage_index"],
            "global_day_index": day_index[row["day_id"]],
            "nurse_index": nurse_index[row["nurse_id"]],
            "request_type": row["request_type"],
            "shift_index": (None if row["requested_off_shift_type_id"] is None else shift_index[row["requested_off_shift_type_id"]]),
        }
        for row in conn.execute(
            """
            SELECT
                iwm.stage_index,
                nr.day_id,
                nr.nurse_id,
                nr.request_type,
                nr.requested_off_shift_type_id
            FROM instance_week_map AS iwm
            JOIN nurse_request AS nr ON nr.week_id = iwm.week_id
            WHERE iwm.instance_id = ?
            ORDER BY iwm.stage_index, nr.nurse_id, nr.day_id
            """,
            (instance_id,),
        ).fetchall()
    ]

    reference_assignments = [
        {
            "stage_index": row["stage_index"],
            "global_day_index": day_index[row["day_id"]],
            "nurse_index": nurse_index[row["nurse_id"]],
            "shift_index": shift_index[row["shift_type_id"]],
            "skill_index": skill_index[row["skill_id"]],
        }
        for row in conn.execute(
            """
            SELECT stage_index, day_id, nurse_id, shift_type_id, skill_id
            FROM assignment
            WHERE instance_id = ?
            ORDER BY stage_index, nurse_id, day_id
            """,
            (instance_id,),
        ).fetchall()
    ]

    history_rows = [
        {
            "nurse_index": nurse_index[row["nurse_id"]],
            "last_shift_index": (None if row["last_shift_type_id"] is None else shift_index[row["last_shift_type_id"]]),
            "consecutive_same_shift_count": row["consecutive_same_shift_count"],
            "consecutive_work_days_count": row["consecutive_work_days_count"],
            "consecutive_days_off_count": row["consecutive_days_off_count"],
            "total_worked_shifts_so_far": row["total_worked_shifts_so_far"],
            "total_working_weekends_so_far": row["total_working_weekends_so_far"],
        }
        for row in initial_history
    ]

    return {
        "bundle_type": "MULTISTAGE_INSTANCE_BUNDLE",
        "dataset_family": DATASET_FAMILY_INRC_II,
        "problem_mode": "MULTISTAGE",
        "instance_id": instance_id,
        "instance_code": instance_code,
        "scenario_id": scenario_id,
        "counts": {
            "nurses": len(nurses),
            "skills": len(skills),
            "shift_types": len(shifts),
            "global_days": len(days),
            "coverage_rows": len(coverage_rows),
            "request_rows": len(request_rows),
            "reference_assignments": len(reference_assignments),
        },
        "index_maps": {
            "nurse_index": {row["nurse_code"]: nurse_index[row["nurse_id"]] for row in nurses},
            "skill_index": {row["skill_code"]: skill_index[row["skill_id"]] for row in skills},
            "shift_index": {row["shift_code"]: shift_index[row["shift_type_id"]] for row in shifts},
        },
        "mip_cpsat": {
            "eligibility_edges": eligibility,
            "forbidden_succession_matrix": forbidden_matrix,
            "coverage_rows": coverage_rows,
            "request_rows": request_rows,
            "initial_history": history_rows,
        },
        "ga_memetic": {
            "chromosome_unit": "one decision per nurse and global day",
            "global_day_index": [dict(row) for row in days],
            "reference_assignments": reference_assignments,
        },
        "rl_search": {
            "state_features_seed": history_rows,
            "coverage_gap_source_view": "coverage_summary_view",
            "nurse_week_summary_source_view": "nurse_week_summary_view",
            "history_transition_source_view": "history_transition_view",
        },
    }


def _build_inrc_static_bundle(
    conn: sqlite3.Connection,
    scenario_id: str,
    dataset_id: str,
    week_id: str,
    week_code: str,
    history_id: str,
    history_variant_code: str | None,
) -> StaticInstanceBundle:
    nurses = list(
        conn.execute(
            """
            SELECT n.nurse_id, n.nurse_code, c.contract_code
            FROM nurse AS n
            JOIN contract AS c ON c.contract_id = n.contract_id
            WHERE n.scenario_id = ?
            ORDER BY n.nurse_code
            """,
            (scenario_id,),
        ).fetchall()
    )
    skills = list(
        conn.execute("SELECT skill_id, skill_code FROM skill WHERE scenario_id = ? ORDER BY skill_code", (scenario_id,)).fetchall()
    )
    shifts = list(
        conn.execute(
            """
            SELECT shift_type_id, shift_code, min_consecutive_shift_assignments, max_consecutive_shift_assignments
            FROM shift_type
            WHERE scenario_id = ?
            ORDER BY shift_code
            """,
            (scenario_id,),
        ).fetchall()
    )
    days = list(
        conn.execute(
            """
            SELECT day_id, day_index, day_name, day_short_name, is_weekend
            FROM day
            WHERE week_id = ?
            ORDER BY day_index
            """,
            (week_id,),
        ).fetchall()
    )
    history_rows = list(
        conn.execute(
            """
            SELECT
                nhs.nurse_id,
                nhs.last_shift_type_id,
                nhs.consecutive_same_shift_count,
                nhs.consecutive_work_days_count,
                nhs.consecutive_days_off_count,
                nhs.total_worked_shifts_so_far,
                nhs.total_working_weekends_so_far
            FROM nurse_history_state AS nhs
            WHERE nhs.history_id = ?
            ORDER BY nhs.nurse_id
            """,
            (history_id,),
        ).fetchall()
    )
    nurse_index = {row["nurse_id"]: index for index, row in enumerate(nurses)}
    skill_index = {row["skill_id"]: index for index, row in enumerate(skills)}
    shift_index = {row["shift_type_id"]: index for index, row in enumerate(shifts)}
    day_index = {row["day_id"]: row["day_index"] for row in days}

    eligibility_edges = [
        {"staff_index": nurse_index[row["nurse_id"]], "skill_index": skill_index[row["skill_id"]]}
        for row in conn.execute(
            """
            SELECT nurse_id, skill_id
            FROM nurse_skill
            WHERE nurse_id IN (SELECT nurse_id FROM nurse WHERE scenario_id = ?)
            ORDER BY nurse_id, skill_id
            """,
            (scenario_id,),
        ).fetchall()
    ]
    forbidden_successions = [
        {
            "prev_shift_index": shift_index[row["prev_shift_type_id"]],
            "next_shift_index": shift_index[row["next_shift_type_id"]],
            "is_forbidden": int(row["is_forbidden"]),
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
    ]
    coverage_rows = [
        {
            "day_index": day_index[row["day_id"]],
            "shift_index": shift_index[row["shift_type_id"]],
            "skill_index": skill_index[row["skill_id"]],
            "min_required": row["min_required"],
            "optimal_required": row["optimal_required"],
        }
        for row in conn.execute(
            """
            SELECT day_id, shift_type_id, skill_id, min_required, optimal_required
            FROM coverage_requirement
            WHERE week_id = ?
            ORDER BY day_id, shift_type_id, skill_id
            """,
            (week_id,),
        ).fetchall()
    ]
    request_rows = [
        {
            "staff_index": nurse_index[row["nurse_id"]],
            "day_index": day_index[row["day_id"]],
            "request_type": row["request_type"],
            "shift_index": (None if row["requested_off_shift_type_id"] is None else shift_index[row["requested_off_shift_type_id"]]),
        }
        for row in conn.execute(
            """
            SELECT nurse_id, day_id, request_type, requested_off_shift_type_id
            FROM nurse_request
            WHERE week_id = ?
            ORDER BY nurse_id, day_id
            """,
            (week_id,),
        ).fetchall()
    ]

    bundle_id = stable_id("static_bundle", "inrc", scenario_id, f"H{history_variant_code}", f"W{week_code}")
    return StaticInstanceBundle(
        bundle_id=bundle_id,
        dataset_family=DATASET_FAMILY_INRC_II,
        problem_mode="STATIC",
        instance_id=None,
        instance_code=f"{scenario_id}|H{history_variant_code}|W{week_code}",
        native_case_code=None,
        counts={
            "staff": len(nurses),
            "skills": len(skills),
            "shift_types": len(shifts),
            "days": len(days),
            "coverage_rows": len(coverage_rows),
            "request_rows": len(request_rows),
            "fixed_assignments": 0,
        },
        days=[dict(row) for row in days],
        staff=[
            {
                "staff_index": nurse_index[row["nurse_id"]],
                "staff_code": row["nurse_code"],
                "contract_code": row["contract_code"],
            }
            for row in nurses
        ],
        shifts=[
            {
                "shift_index": shift_index[row["shift_type_id"]],
                "shift_code": row["shift_code"],
                "min_consecutive_assignments": row["min_consecutive_shift_assignments"],
                "max_consecutive_assignments": row["max_consecutive_shift_assignments"],
            }
            for row in shifts
        ],
        skills=[
            {"skill_index": skill_index[row["skill_id"]], "skill_code": row["skill_code"]}
            for row in skills
        ],
        constraints={
            "eligibility_edges": eligibility_edges,
            "forbidden_successions": forbidden_successions,
            "coverage_rows": coverage_rows,
            "request_rows": request_rows,
            "fixed_assignments": [],
            "employee_shift_limits": [],
            "initial_history": [
                {
                    "staff_index": nurse_index[row["nurse_id"]],
                    "last_shift_index": (None if row["last_shift_type_id"] is None else shift_index[row["last_shift_type_id"]]),
                    "consecutive_same_shift_count": row["consecutive_same_shift_count"],
                    "consecutive_work_days_count": row["consecutive_work_days_count"],
                    "consecutive_days_off_count": row["consecutive_days_off_count"],
                    "total_worked_shifts_so_far": row["total_worked_shifts_so_far"],
                    "total_working_weekends_so_far": row["total_working_weekends_so_far"],
                }
                for row in history_rows
            ],
        },
        objective_weights={},
        metadata={"dataset_id": dataset_id, "scenario_id": scenario_id, "week_id": week_id, "history_id": history_id},
    )


def _build_scheduling_case_bundle(
    conn: sqlite3.Connection,
    instance_id: str,
    instance_code: str,
    case_code: str,
) -> StaticInstanceBundle:
    case_row = conn.execute(
        """
        SELECT sb_case_id, start_date, end_date, horizon_days, global_min_rest_minutes
        FROM sb_case
        WHERE instance_id = ?
        """,
        (instance_id,),
    ).fetchone()
    if case_row is None:
        raise ValueError(f"Missing sb_case row for SchedulingBenchmarks instance {instance_id}.")

    days = list(
        conn.execute(
            """
            SELECT sb_day_id, day_index, calendar_date, day_name, day_of_week_index, is_weekend
            FROM sb_day
            WHERE sb_case_id = ?
            ORDER BY day_index
            """,
            (case_row["sb_case_id"],),
        ).fetchall()
    )
    shifts = list(
        conn.execute(
            """
            SELECT sb_shift_type_id, shift_code, duration_minutes, start_time, end_time, color
            FROM sb_shift_type
            WHERE sb_case_id = ?
            ORDER BY shift_code
            """,
            (case_row["sb_case_id"],),
        ).fetchall()
    )
    employees = list(
        conn.execute(
            """
            SELECT e.sb_employee_id, e.employee_code, c.contract_code
            FROM sb_employee AS e
            JOIN sb_contract AS c ON c.sb_contract_id = e.sb_contract_id
            WHERE e.sb_case_id = ?
            ORDER BY e.employee_code
            """,
            (case_row["sb_case_id"],),
        ).fetchall()
    )
    staff_index = {row["sb_employee_id"]: index for index, row in enumerate(employees)}
    shift_index = {row["sb_shift_type_id"]: index for index, row in enumerate(shifts)}
    day_index = {row["sb_day_id"]: row["day_index"] for row in days}

    shift_definitions = {
        row["shift_code"]: {
            "start_time": row["start_time"],
            "duration_minutes": row["duration_minutes"],
        }
        for row in shifts
    }
    forbidden_successions = derive_forbidden_successions(shift_definitions, case_row["global_min_rest_minutes"] or 0)

    fixed_assignments = [
        {
            "staff_index": staff_index[row["sb_employee_id"]],
            "day_index": day_index[row["sb_day_id"]],
            "shift_index": (None if row["sb_shift_type_id"] is None else shift_index[row["sb_shift_type_id"]]),
            "assignment_code": row["assignment_code"],
            "is_off": row["is_off"],
        }
        for row in conn.execute(
            """
            SELECT sb_day_id, sb_employee_id, sb_shift_type_id, assignment_code, is_off
            FROM sb_fixed_assignment
            WHERE sb_case_id = ?
            ORDER BY sb_employee_id, sb_day_id
            """,
            (case_row["sb_case_id"],),
        ).fetchall()
    ]
    request_rows = [
        {
            "staff_index": staff_index[row["sb_employee_id"]],
            "day_index": day_index[row["sb_day_id"]],
            "shift_index": shift_index[row["sb_shift_type_id"]],
            "request_type": row["request_type"],
            "weight": row["weight"],
        }
        for row in conn.execute(
            """
            SELECT sb_day_id, sb_employee_id, sb_shift_type_id, request_type, weight
            FROM sb_request
            WHERE sb_case_id = ?
            ORDER BY sb_employee_id, sb_day_id
            """,
            (case_row["sb_case_id"],),
        ).fetchall()
    ]
    coverage_rows = [
        {
            "day_index": day_index[row["sb_day_id"]],
            "shift_index": shift_index[row["sb_shift_type_id"]],
            "min_required": row["min_required"],
            "preferred_required": row["preferred_required"],
            "under_weight": row["under_weight"],
            "over_weight": row["over_weight"],
        }
        for row in conn.execute(
            """
            SELECT sb_day_id, sb_shift_type_id, min_required, preferred_required, under_weight, over_weight
            FROM sb_cover_requirement
            WHERE sb_case_id = ?
            ORDER BY sb_day_id, sb_shift_type_id
            """,
            (case_row["sb_case_id"],),
        ).fetchall()
    ]
    shift_limits = [
        {
            "staff_index": staff_index[row["sb_employee_id"]],
            "shift_index": shift_index[row["sb_shift_type_id"]],
            "max_assignments": row["max_assignments"],
        }
        for row in conn.execute(
            """
            SELECT sb_employee_id, sb_shift_type_id, max_assignments
            FROM sb_employee_shift_limit
            WHERE sb_employee_id IN (SELECT sb_employee_id FROM sb_employee WHERE sb_case_id = ?)
            ORDER BY sb_employee_id, sb_shift_type_id
            """,
            (case_row["sb_case_id"],),
        ).fetchall()
    ]

    bundle_id = stable_id("static_bundle", "scheduling_benchmarks", case_code)
    return StaticInstanceBundle(
        bundle_id=bundle_id,
        dataset_family=DATASET_FAMILY_SCHEDULING_BENCHMARKS,
        problem_mode="STATIC",
        instance_id=instance_id,
        instance_code=instance_code,
        native_case_code=case_code,
        counts={
            "staff": len(employees),
            "skills": 0,
            "shift_types": len(shifts),
            "days": len(days),
            "coverage_rows": len(coverage_rows),
            "request_rows": len(request_rows),
            "fixed_assignments": len(fixed_assignments),
        },
        days=[dict(row) for row in days],
        staff=[
            {
                "staff_index": staff_index[row["sb_employee_id"]],
                "staff_code": row["employee_code"],
                "contract_code": row["contract_code"],
            }
            for row in employees
        ],
        shifts=[
            {
                "shift_index": shift_index[row["sb_shift_type_id"]],
                "shift_code": row["shift_code"],
                "duration_minutes": row["duration_minutes"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "color": row["color"],
            }
            for row in shifts
        ],
        skills=[],
        constraints={
            "eligibility_edges": [],
            "forbidden_successions": [
                {
                    "prev_shift_code": prev_shift_code,
                    "next_shift_code": next_shift_code,
                    "is_forbidden": 1,
                }
                for prev_shift_code, next_shift_codes in sorted(forbidden_successions.items())
                for next_shift_code in sorted(next_shift_codes)
            ],
            "coverage_rows": coverage_rows,
            "request_rows": request_rows,
            "fixed_assignments": fixed_assignments,
            "employee_shift_limits": shift_limits,
            "initial_history": [],
        },
        objective_weights={"cover_weights_are_row_specific": True},
        metadata={
            "sb_case_id": case_row["sb_case_id"],
            "start_date": case_row["start_date"],
            "end_date": case_row["end_date"],
            "global_min_rest_minutes": case_row["global_min_rest_minutes"],
        },
    )
