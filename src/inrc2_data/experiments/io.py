"""Bundle discovery, enrichment, and persistence helpers for experiment runners."""

from __future__ import annotations

import copy
import json
import sqlite3
from pathlib import Path
from typing import Any

from ..constants import (
    ASSIGNMENT_SOURCE_SOLVER,
    DATASET_FAMILY_INRC_II,
    DATASET_FAMILY_SCHEDULING_BENCHMARKS,
    PROBLEM_MODE_STATIC,
)
from ..utils import stable_id


def resolve_pipeline_summary(project_root: Path) -> dict[str, Any]:
    summary_path = project_root / "data" / "processed" / "pipeline_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(
            f"Missing pipeline summary at {summary_path}. Run scripts/build_foundation.py first."
        )
    return json.loads(summary_path.read_text(encoding="utf-8"))


def resolve_database_path(project_root: Path, explicit_path: Path | None = None) -> Path:
    if explicit_path is not None:
        return explicit_path.resolve()
    summary = resolve_pipeline_summary(project_root)
    db_path = Path(summary["database_path"])
    if not db_path.exists():
        raise FileNotFoundError(f"Expected experiment database at {db_path}, but it does not exist.")
    return db_path


def resolve_feature_manifest(project_root: Path) -> dict[str, Any]:
    manifest_path = project_root / "data" / "processed" / "feature_bundles" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Missing feature manifest at {manifest_path}. Run scripts/build_foundation.py first."
        )
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def list_static_bundle_entries(project_root: Path) -> list[tuple[str, Path]]:
    manifest = resolve_feature_manifest(project_root)
    entries: list[tuple[str, Path]] = []
    for bundle_id, raw_path in manifest.get("static", {}).items():
        entries.append((bundle_id, Path(raw_path)))
    return sorted(entries, key=lambda item: item[0])


def find_static_bundle_path(
    project_root: Path,
    *,
    bundle_id: str | None = None,
    instance_code: str | None = None,
    case_code: str | None = None,
) -> Path:
    entries = list_static_bundle_entries(project_root)
    if bundle_id is not None:
        for candidate_bundle_id, candidate_path in entries:
            if candidate_bundle_id == bundle_id:
                return candidate_path
        raise KeyError(f"No static bundle found with id {bundle_id}.")

    if case_code:
        exact_matches = [
            candidate_path
            for candidate_bundle_id, candidate_path in entries
            if candidate_bundle_id.endswith(f"::{case_code}")
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            raise ValueError(f"Multiple static bundles matched case code {case_code}.")

    if instance_code:
        exact_matches = [
            candidate_path
            for candidate_bundle_id, candidate_path in entries
            if f"::{instance_code}::" in candidate_bundle_id or candidate_bundle_id.endswith(f"::{instance_code}")
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1 and not case_code:
            match_text = ", ".join(str(path.name) for path in exact_matches[:8])
            raise ValueError(f"Multiple static bundles matched instance code {instance_code}: {match_text}")
        header_matches = []
        for _, candidate_path in entries:
            try:
                payload = json.loads(candidate_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if payload.get("instance_code") == instance_code:
                header_matches.append(candidate_path)
        if len(header_matches) == 1:
            return header_matches[0]
        if len(header_matches) > 1:
            match_text = ", ".join(str(path.name) for path in header_matches[:8])
            raise ValueError(f"Multiple static bundles matched instance code {instance_code}: {match_text}")

    tokens = [token for token in (instance_code, case_code) if token]
    if not tokens:
        raise ValueError("Provide bundle_id, instance_code, or case_code to resolve a static bundle.")

    matches = [
        candidate_path
        for candidate_bundle_id, candidate_path in entries
        if all(token.lower() in candidate_bundle_id.lower() for token in tokens)
    ]
    if not matches:
        raise KeyError(f"No static bundle matched tokens {tokens}.")
    if len(matches) > 1:
        match_text = ", ".join(str(path.name) for path in matches[:8])
        raise ValueError(f"Multiple static bundles matched tokens {tokens}: {match_text}")
    return matches[0]


def load_static_bundle(bundle_path: Path) -> dict[str, Any]:
    if not bundle_path.exists():
        raise FileNotFoundError(f"Static bundle file not found: {bundle_path}")
    return json.loads(bundle_path.read_text(encoding="utf-8"))


def enrich_static_bundle(bundle: dict[str, Any], db_path: Path) -> dict[str, Any]:
    enriched = copy.deepcopy(bundle)
    family = enriched["dataset_family"]
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if family == DATASET_FAMILY_INRC_II:
            _enrich_inrc_static_bundle(enriched, conn)
        elif family == DATASET_FAMILY_SCHEDULING_BENCHMARKS:
            _enrich_scheduling_bundle(enriched, conn)
        else:
            raise ValueError(f"Unsupported dataset family for enrichment: {family}")
    finally:
        conn.close()
    return enriched


def _enrich_inrc_static_bundle(bundle: dict[str, Any], conn: sqlite3.Connection) -> None:
    metadata = bundle["metadata"]
    scenario_id = metadata["scenario_id"]
    week_id = metadata["week_id"]
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
    nurse_rows = conn.execute(
        """
        SELECT n.nurse_id, n.nurse_code, c.contract_code
        FROM nurse AS n
        JOIN contract AS c ON c.contract_id = n.contract_id
        WHERE n.scenario_id = ?
        ORDER BY n.nurse_code
        """,
        (scenario_id,),
    ).fetchall()
    nurse_by_code = {row["nurse_code"]: dict(row) for row in nurse_rows}
    for staff_row in bundle["staff"]:
        source_row = nurse_by_code.get(staff_row["staff_code"])
        if source_row is None:
            raise ValueError(
                f"Missing nurse lookup for {staff_row['staff_code']} in scenario {scenario_id}."
            )
        staff_row["nurse_id"] = source_row["nurse_id"]

    shift_rows = conn.execute(
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
    shift_by_code = {row["shift_code"]: dict(row) for row in shift_rows}
    for shift_row in bundle["shifts"]:
        source_row = shift_by_code.get(shift_row["shift_code"])
        if source_row is None:
            raise ValueError(
                f"Missing shift lookup for {shift_row['shift_code']} in scenario {scenario_id}."
            )
        shift_row["shift_type_id"] = source_row["shift_type_id"]

    skill_rows = conn.execute(
        "SELECT skill_id, skill_code FROM skill WHERE scenario_id = ? ORDER BY skill_code",
        (scenario_id,),
    ).fetchall()
    skill_by_code = {row["skill_code"]: dict(row) for row in skill_rows}
    for skill_row in bundle["skills"]:
        source_row = skill_by_code.get(skill_row["skill_code"])
        if source_row is None:
            raise ValueError(
                f"Missing skill lookup for {skill_row['skill_code']} in scenario {scenario_id}."
            )
        skill_row["skill_id"] = source_row["skill_id"]

    for day_row in bundle["days"]:
        day_lookup = conn.execute(
            """
            SELECT day_id
            FROM day
            WHERE week_id = ? AND day_index = ?
            """,
            (week_id, day_row["day_index"]),
        ).fetchone()
        if day_lookup is None:
            raise ValueError(
                f"Missing day lookup for week {week_id} day index {day_row['day_index']}."
            )
        day_row["day_id"] = day_lookup["day_id"]

    bundle["contracts"] = contracts
    bundle["metadata"]["planning_mode"] = "PARTIAL_WEEK"


def _enrich_scheduling_bundle(bundle: dict[str, Any], conn: sqlite3.Connection) -> None:
    instance_id = bundle["instance_id"]
    case_row = conn.execute(
        """
        SELECT sb_case_id
        FROM sb_case
        WHERE instance_id = ?
        """,
        (instance_id,),
    ).fetchone()
    if case_row is None:
        raise ValueError(f"Missing SchedulingBenchmarks case row for instance {instance_id}.")
    sb_case_id = case_row["sb_case_id"]

    contract_rows = conn.execute(
        """
        SELECT
            contract_code,
            min_rest_minutes,
            min_total_minutes,
            max_total_minutes,
            min_consecutive_shifts,
            max_consecutive_shifts,
            min_consecutive_days_off,
            max_working_weekends,
            valid_shift_codes,
            is_meta_contract
        FROM sb_contract
        WHERE sb_case_id = ?
        ORDER BY contract_code
        """,
        (sb_case_id,),
    ).fetchall()
    bundle["contracts"] = [dict(row) for row in contract_rows]

    employee_rows = conn.execute(
        """
        SELECT e.sb_employee_id, e.employee_code, c.contract_code
        FROM sb_employee AS e
        JOIN sb_contract AS c ON c.sb_contract_id = e.sb_contract_id
        WHERE e.sb_case_id = ?
        ORDER BY e.employee_code
        """,
        (sb_case_id,),
    ).fetchall()
    employee_by_code = {row["employee_code"]: dict(row) for row in employee_rows}
    for staff_row in bundle["staff"]:
        source_row = employee_by_code.get(staff_row["staff_code"])
        if source_row is None:
            raise ValueError(
                f"Missing employee lookup for {staff_row['staff_code']} in case {instance_id}."
            )
        staff_row["sb_employee_id"] = source_row["sb_employee_id"]

    shift_rows = conn.execute(
        """
        SELECT sb_shift_type_id, shift_code, duration_minutes, start_time, end_time, color
        FROM sb_shift_type
        WHERE sb_case_id = ?
        ORDER BY shift_code
        """,
        (sb_case_id,),
    ).fetchall()
    shift_by_code = {row["shift_code"]: dict(row) for row in shift_rows}
    for shift_row in bundle["shifts"]:
        source_row = shift_by_code.get(shift_row["shift_code"])
        if source_row is None:
            raise ValueError(
                f"Missing shift lookup for {shift_row['shift_code']} in case {instance_id}."
            )
        shift_row["sb_shift_type_id"] = source_row["sb_shift_type_id"]

    day_rows = conn.execute(
        """
        SELECT sb_day_id, day_index
        FROM sb_day
        WHERE sb_case_id = ?
        ORDER BY day_index
        """,
        (sb_case_id,),
    ).fetchall()
    day_by_index = {row["day_index"]: dict(row) for row in day_rows}
    for day_row in bundle["days"]:
        source_row = day_by_index.get(day_row["day_index"])
        if source_row is None:
            raise ValueError(
                f"Missing static day lookup for case {instance_id} day index {day_row['day_index']}."
            )
        day_row["sb_day_id"] = source_row["sb_day_id"]

    bundle["metadata"]["sb_case_id"] = sb_case_id
    bundle["metadata"]["planning_mode"] = "FULL_HORIZON"


def get_multistage_instance_record(
    db_path: Path,
    *,
    instance_code: str | None = None,
    instance_id: str | None = None,
) -> dict[str, Any]:
    if not instance_code and not instance_id:
        raise ValueError("Provide instance_code or instance_id to resolve a multistage instance.")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT instance_id, instance_code, scenario_id, initial_history_id, num_weeks
            FROM instance
            WHERE dataset_family = ?
              AND problem_mode != ?
        """
        params: list[Any] = [DATASET_FAMILY_INRC_II, PROBLEM_MODE_STATIC]
        if instance_id:
            sql += " AND instance_id = ?"
            params.append(instance_id)
        if instance_code:
            sql += " AND instance_code = ?"
            params.append(instance_code)
        row = conn.execute(sql, params).fetchone()
        if row is None:
            raise KeyError(f"No multistage INRC-II instance matched instance_id={instance_id} instance_code={instance_code}.")
        stage_rows = [
            dict(stage_row)
            for stage_row in conn.execute(
                """
                SELECT iwm.stage_index, iwm.week_id, w.week_code
                FROM instance_week_map AS iwm
                JOIN week AS w ON w.week_id = iwm.week_id
                WHERE iwm.instance_id = ?
                ORDER BY iwm.stage_index
                """,
                (row["instance_id"],),
            ).fetchall()
        ]
        history_rows = [
            dict(history_row)
            for history_row in conn.execute(
                """
                SELECT
                    nhs.nurse_id,
                    n.nurse_code,
                    nhs.last_shift_type_id,
                    nhs.consecutive_same_shift_count,
                    nhs.consecutive_work_days_count,
                    nhs.consecutive_days_off_count,
                    nhs.total_worked_shifts_so_far,
                    nhs.total_working_weekends_so_far
                FROM nurse_history_state AS nhs
                JOIN nurse AS n ON n.nurse_id = nhs.nurse_id
                WHERE nhs.history_id = ?
                ORDER BY n.nurse_code
                """,
                (row["initial_history_id"],),
            ).fetchall()
        ]
    finally:
        conn.close()
    payload = dict(row)
    payload["stage_rows"] = stage_rows
    payload["history_rows"] = history_rows
    return payload


def ensure_inrc_static_instance(conn: sqlite3.Connection, bundle: dict[str, Any]) -> str:
    metadata = bundle["metadata"]
    instance_id = stable_id(
        "instance",
        "static_projection",
        metadata["scenario_id"],
        metadata["history_id"],
        metadata["week_id"],
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO instance (
            instance_id,
            dataset_id,
            dataset_family,
            problem_mode,
            scenario_id,
            initial_history_id,
            num_weeks,
            instance_code,
            native_case_code,
            instance_dir_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            instance_id,
            metadata["dataset_id"],
            DATASET_FAMILY_INRC_II,
            PROBLEM_MODE_STATIC,
            metadata["scenario_id"],
            metadata["history_id"],
            1,
            bundle["instance_code"],
            None,
            None,
        ),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO instance_week_map (instance_id, stage_index, week_id)
        VALUES (?, ?, ?)
        """,
        (instance_id, 0, metadata["week_id"]),
    )
    return instance_id


def persist_static_run(
    db_path: Path,
    bundle: dict[str, Any],
    result: dict[str, Any],
    report_path: Path,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        instance_id = bundle.get("instance_id")
        if bundle["dataset_family"] == DATASET_FAMILY_INRC_II:
            if not instance_id:
                instance_id = ensure_inrc_static_instance(conn, bundle)
            _insert_solver_run(
                conn,
                solver_run_id=result["solver_run_id"],
                instance_id=instance_id,
                algorithm_name=result["algorithm_name"],
                variant_name=result["variant_name"],
                seed=result["seed"],
                runtime_sec=result["runtime_sec"],
                status=result["evaluation"]["status"],
            )
            _insert_inrc_assignments(conn, instance_id, bundle, result, report_path)
        else:
            _insert_solver_run(
                conn,
                solver_run_id=result["solver_run_id"],
                instance_id=instance_id,
                algorithm_name=result["algorithm_name"],
                variant_name=result["variant_name"],
                seed=result["seed"],
                runtime_sec=result["runtime_sec"],
                status=result["evaluation"]["status"],
            )
            _insert_sb_assignments(conn, bundle, result, report_path)

        _insert_constraint_results(conn, bundle, result)
        _insert_ablation_metrics(conn, result)
        conn.commit()
    finally:
        conn.close()


def persist_multistage_run(
    db_path: Path,
    instance_record: dict[str, Any],
    result: dict[str, Any],
    report_path: Path,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        _insert_solver_run(
            conn,
            solver_run_id=result["solver_run_id"],
            instance_id=instance_record["instance_id"],
            algorithm_name=result["algorithm_name"],
            variant_name=result["variant_name"],
            seed=result["seed"],
            runtime_sec=result["runtime_sec"],
            status=result["evaluation"]["status"],
        )
        _insert_multistage_assignments(conn, instance_record["instance_id"], result, report_path)
        _insert_multistage_constraint_results(conn, result)
        _insert_ablation_metrics(conn, result)
        conn.commit()
    finally:
        conn.close()


def _insert_solver_run(
    conn: sqlite3.Connection,
    *,
    solver_run_id: str,
    instance_id: str,
    algorithm_name: str,
    variant_name: str,
    seed: int,
    runtime_sec: float,
    status: str,
) -> None:
    conn.execute(
        """
        INSERT INTO solver_run (
            solver_run_id,
            instance_id,
            algorithm_name,
            variant_name,
            seed,
            runtime_sec,
            status
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (solver_run_id, instance_id, algorithm_name, variant_name, seed, runtime_sec, status),
    )


def _insert_inrc_assignments(
    conn: sqlite3.Connection,
    instance_id: str,
    bundle: dict[str, Any],
    result: dict[str, Any],
    report_path: Path,
) -> None:
    day_id_by_index = {row["day_index"]: row["day_id"] for row in bundle["days"]}
    nurse_id_by_index = {row["staff_index"]: row["nurse_id"] for row in bundle["staff"]}
    shift_id_by_index = {row["shift_index"]: row["shift_type_id"] for row in bundle["shifts"]}
    skill_id_by_index = {row["skill_index"]: row["skill_id"] for row in bundle["skills"]}
    week_id = bundle["metadata"]["week_id"]
    rows = []
    for ordinal, assignment in enumerate(result["assignments"], start=1):
        rows.append(
            (
                stable_id("assignment", result["solver_run_id"], ordinal),
                instance_id,
                0,
                week_id,
                day_id_by_index[assignment["day_index"]],
                nurse_id_by_index[assignment["staff_index"]],
                shift_id_by_index[assignment["shift_index"]],
                skill_id_by_index[assignment["skill_index"]],
                result["solver_run_id"],
                str(report_path),
                ASSIGNMENT_SOURCE_SOLVER,
            )
        )
    conn.executemany(
        """
        INSERT INTO assignment (
            assignment_id,
            instance_id,
            stage_index,
            week_id,
            day_id,
            nurse_id,
            shift_type_id,
            skill_id,
            solver_run_id,
            source_file,
            source_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _insert_sb_assignments(
    conn: sqlite3.Connection,
    bundle: dict[str, Any],
    result: dict[str, Any],
    report_path: Path,
) -> None:
    day_id_by_index = {row["day_index"]: row["sb_day_id"] for row in bundle["days"]}
    employee_id_by_index = {row["staff_index"]: row["sb_employee_id"] for row in bundle["staff"]}
    shift_id_by_index = {row["shift_index"]: row["sb_shift_type_id"] for row in bundle["shifts"]}
    sb_case_id = bundle["metadata"]["sb_case_id"]
    rows = []
    for ordinal, assignment in enumerate(result["assignments"], start=1):
        rows.append(
            (
                stable_id("sb_assignment", result["solver_run_id"], ordinal),
                bundle["instance_id"],
                sb_case_id,
                day_id_by_index[assignment["day_index"]],
                employee_id_by_index[assignment["staff_index"]],
                shift_id_by_index[assignment["shift_index"]],
                result["solver_run_id"],
                str(report_path),
                ASSIGNMENT_SOURCE_SOLVER,
            )
        )
    conn.executemany(
        """
        INSERT INTO sb_assignment (
            sb_assignment_id,
            instance_id,
            sb_case_id,
            sb_day_id,
            sb_employee_id,
            sb_shift_type_id,
            solver_run_id,
            source_file,
            source_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _insert_multistage_assignments(
    conn: sqlite3.Connection,
    instance_id: str,
    result: dict[str, Any],
    report_path: Path,
) -> None:
    rows = []
    for stage_summary in result["stage_results"]:
        for ordinal, assignment in enumerate(stage_summary["assignments"], start=1):
            rows.append(
                (
                    stable_id("assignment", result["solver_run_id"], stage_summary["stage_index"], ordinal),
                    instance_id,
                    stage_summary["stage_index"],
                    stage_summary["week_id"],
                    assignment["day_id"],
                    assignment["nurse_id"],
                    assignment["shift_type_id"],
                    assignment["skill_id"],
                    result["solver_run_id"],
                    str(report_path),
                    ASSIGNMENT_SOURCE_SOLVER,
                )
            )
    conn.executemany(
        """
        INSERT INTO assignment (
            assignment_id,
            instance_id,
            stage_index,
            week_id,
            day_id,
            nurse_id,
            shift_type_id,
            skill_id,
            solver_run_id,
            source_file,
            source_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _insert_constraint_results(conn: sqlite3.Connection, bundle: dict[str, Any], result: dict[str, Any]) -> None:
    week_id = bundle["metadata"].get("week_id")
    rows = []
    for ordinal, constraint_row in enumerate(result["evaluation"]["constraint_results"], start=1):
        rows.append(
            (
                stable_id("constraint_result", result["solver_run_id"], ordinal),
                result["solver_run_id"],
                week_id,
                None,
                constraint_row["constraint_code"],
                constraint_row["violation_count"],
                constraint_row["penalty_cost"],
                int(constraint_row["is_hard"]),
                constraint_row["evaluation_scope"],
            )
        )
    conn.executemany(
        """
        INSERT INTO constraint_result (
            constraint_result_id,
            solver_run_id,
            week_id,
            nurse_id,
            constraint_code,
            violation_count,
            penalty_cost,
            is_hard,
            evaluation_scope
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _insert_multistage_constraint_results(conn: sqlite3.Connection, result: dict[str, Any]) -> None:
    rows = []
    ordinal = 0
    for constraint_row in result["evaluation"]["constraint_results"]:
        ordinal += 1
        rows.append(
            (
                stable_id("constraint_result", result["solver_run_id"], ordinal),
                result["solver_run_id"],
                constraint_row.get("week_id"),
                None,
                constraint_row["constraint_code"],
                constraint_row["violation_count"],
                constraint_row["penalty_cost"],
                int(constraint_row["is_hard"]),
                constraint_row["evaluation_scope"],
            )
        )
    conn.executemany(
        """
        INSERT INTO constraint_result (
            constraint_result_id,
            solver_run_id,
            week_id,
            nurse_id,
            constraint_code,
            violation_count,
            penalty_cost,
            is_hard,
            evaluation_scope
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _insert_ablation_metrics(conn: sqlite3.Connection, result: dict[str, Any]) -> None:
    evaluation = result["evaluation"]
    metrics = {
        "objective_value": evaluation["objective_value"],
        "hard_violation_count": evaluation["hard_violation_count"],
        "soft_penalty": evaluation["soft_penalty"],
        "assignment_count": len(result["assignments"]) if "assignments" in result else evaluation["metrics"].get("assignment_count", 0),
    }
    rows = []
    for ordinal, (metric_name, metric_value) in enumerate(metrics.items(), start=1):
        rows.append(
            (
                stable_id("ablation_result", result["solver_run_id"], ordinal),
                result["solver_run_id"],
                result["variant_name"],
                metric_name,
                float(metric_value),
                None,
            )
        )
    conn.executemany(
        """
        INSERT INTO ablation_result (
            ablation_result_id,
            solver_run_id,
            ablation_group,
            metric_name,
            metric_value,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
