"""SQLite materialization for the canonical INRC-II foundation."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .constants import SNAPSHOT_TYPE_INITIAL_SOURCE


TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "raw_document": (
        "raw_document_id",
        "dataset_id",
        "dataset_code",
        "dataset_family",
        "problem_mode",
        "source_file",
        "relative_path",
        "source_format",
        "source_group_code",
        "preferred_canonical_source",
        "paired_raw_document_id",
        "scenario_folder_code",
        "instance_dir_name",
        "file_role",
        "root_tag",
        "scenario_id_in_xml",
        "scenario_token_in_name",
        "file_code",
        "native_case_code",
        "content_sha256",
        "raw_payload",
    ),
    "dataset": (
        "dataset_id",
        "dataset_code",
        "dataset_family",
        "problem_mode",
        "display_name",
        "root_path",
        "scan_root_path",
        "is_test_dataset",
    ),
    "scenario": (
        "scenario_id",
        "dataset_id",
        "dataset_name",
        "planning_horizon_weeks",
        "source_file",
        "scenario_folder_code",
    ),
    "skill": ("skill_id", "scenario_id", "skill_code"),
    "contract": (
        "contract_id",
        "scenario_id",
        "contract_code",
        "min_total_assignments",
        "max_total_assignments",
        "min_consecutive_work_days",
        "max_consecutive_work_days",
        "min_consecutive_days_off",
        "max_consecutive_days_off",
        "max_working_weekends",
        "complete_weekend_required",
    ),
    "nurse": ("nurse_id", "scenario_id", "nurse_code", "contract_id"),
    "nurse_skill": ("nurse_id", "skill_id"),
    "shift_type": (
        "shift_type_id",
        "scenario_id",
        "shift_code",
        "min_consecutive_shift_assignments",
        "max_consecutive_shift_assignments",
    ),
    "forbidden_shift_succession": ("scenario_id", "prev_shift_type_id", "next_shift_type_id", "is_forbidden"),
    "week": ("week_id", "scenario_id", "week_index", "week_code", "source_file"),
    "day": ("day_id", "week_id", "day_index", "day_name", "day_short_name", "is_weekend"),
    "coverage_requirement": (
        "requirement_id",
        "week_id",
        "day_id",
        "shift_type_id",
        "skill_id",
        "min_required",
        "optimal_required",
    ),
    "nurse_request": ("request_id", "week_id", "nurse_id", "day_id", "requested_off_shift_type_id", "request_type"),
    "history_snapshot": (
        "history_id",
        "scenario_id",
        "instance_id",
        "week_index_before_solve",
        "source_file",
        "snapshot_type",
        "history_variant_code",
    ),
    "nurse_history_state": (
        "history_id",
        "nurse_id",
        "last_shift_type_id",
        "consecutive_same_shift_count",
        "consecutive_work_days_count",
        "consecutive_days_off_count",
        "total_worked_shifts_so_far",
        "total_working_weekends_so_far",
    ),
    "instance": (
        "instance_id",
        "dataset_id",
        "dataset_family",
        "problem_mode",
        "scenario_id",
        "initial_history_id",
        "num_weeks",
        "instance_code",
        "native_case_code",
        "instance_dir_name",
    ),
    "instance_week_map": ("instance_id", "stage_index", "week_id"),
    "assignment": (
        "assignment_id",
        "instance_id",
        "stage_index",
        "week_id",
        "day_id",
        "nurse_id",
        "shift_type_id",
        "skill_id",
        "solver_run_id",
        "source_file",
        "source_kind",
    ),
    "sb_case": (
        "sb_case_id",
        "dataset_id",
        "instance_id",
        "case_code",
        "source_ros_file",
        "source_txt_file",
        "start_date",
        "end_date",
        "horizon_days",
        "global_min_rest_minutes",
    ),
    "sb_day": ("sb_day_id", "sb_case_id", "day_index", "calendar_date", "day_name", "day_of_week_index", "is_weekend"),
    "sb_shift_type": (
        "sb_shift_type_id",
        "sb_case_id",
        "shift_code",
        "duration_minutes",
        "start_time",
        "end_time",
        "color",
    ),
    "sb_contract": (
        "sb_contract_id",
        "sb_case_id",
        "contract_code",
        "min_rest_minutes",
        "min_total_minutes",
        "max_total_minutes",
        "min_consecutive_shifts",
        "max_consecutive_shifts",
        "min_consecutive_days_off",
        "max_working_weekends",
        "valid_shift_codes",
        "is_meta_contract",
    ),
    "sb_employee": ("sb_employee_id", "sb_case_id", "employee_code", "sb_contract_id"),
    "sb_employee_shift_limit": ("sb_employee_id", "sb_shift_type_id", "max_assignments"),
    "sb_fixed_assignment": (
        "sb_fixed_assignment_id",
        "sb_case_id",
        "sb_day_id",
        "sb_employee_id",
        "sb_shift_type_id",
        "assignment_code",
        "is_off",
    ),
    "sb_request": ("sb_request_id", "sb_case_id", "sb_day_id", "sb_employee_id", "sb_shift_type_id", "request_type", "weight"),
    "sb_cover_requirement": (
        "sb_cover_requirement_id",
        "sb_case_id",
        "sb_day_id",
        "sb_shift_type_id",
        "min_required",
        "preferred_required",
        "under_weight",
        "over_weight",
    ),
    "sb_assignment": (
        "sb_assignment_id",
        "instance_id",
        "sb_case_id",
        "sb_day_id",
        "sb_employee_id",
        "sb_shift_type_id",
        "solver_run_id",
        "source_file",
        "source_kind",
    ),
    "solver_run": ("solver_run_id", "instance_id", "algorithm_name", "variant_name", "seed", "runtime_sec", "status"),
    "constraint_result": (
        "constraint_result_id",
        "solver_run_id",
        "week_id",
        "nurse_id",
        "constraint_code",
        "violation_count",
        "penalty_cost",
        "is_hard",
        "evaluation_scope",
    ),
    "ablation_result": ("ablation_result_id", "solver_run_id", "ablation_group", "metric_name", "metric_value", "notes"),
}


def build_database(db_path: Path, tables: dict[str, list[dict[str, Any]]]) -> Path:
    target_path = _prepare_target_path(db_path.resolve())
    target_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    _create_schema(conn)
    _insert_rows(conn, tables)
    _create_indexes(conn)
    _create_views(conn)
    conn.commit()
    conn.close()
    return target_path


def _prepare_target_path(db_path: Path) -> Path:
    if not db_path.exists():
        return db_path
    try:
        db_path.unlink()
        return db_path
    except PermissionError:
        suffix = 1
        while True:
            fallback_path = db_path.with_name(f"{db_path.stem}_rebuild_{suffix}{db_path.suffix}")
            if fallback_path.exists():
                try:
                    fallback_path.unlink()
                except PermissionError:
                    suffix += 1
                    continue
            return fallback_path


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE raw_document (
            raw_document_id TEXT PRIMARY KEY,
            dataset_id TEXT NOT NULL,
            dataset_code TEXT NOT NULL,
            dataset_family TEXT NOT NULL,
            problem_mode TEXT NOT NULL,
            source_file TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            source_format TEXT NOT NULL,
            source_group_code TEXT,
            preferred_canonical_source INTEGER NOT NULL,
            paired_raw_document_id TEXT,
            scenario_folder_code TEXT,
            instance_dir_name TEXT,
            file_role TEXT NOT NULL,
            root_tag TEXT,
            scenario_id_in_xml TEXT,
            scenario_token_in_name TEXT,
            file_code INTEGER,
            native_case_code TEXT,
            content_sha256 TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id)
        );

        CREATE TABLE dataset (
            dataset_id TEXT PRIMARY KEY,
            dataset_code TEXT NOT NULL,
            dataset_family TEXT NOT NULL,
            problem_mode TEXT NOT NULL,
            display_name TEXT NOT NULL,
            root_path TEXT NOT NULL,
            scan_root_path TEXT NOT NULL,
            is_test_dataset INTEGER NOT NULL
        );

        CREATE TABLE scenario (
            scenario_id TEXT PRIMARY KEY,
            dataset_id TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            planning_horizon_weeks INTEGER NOT NULL,
            source_file TEXT NOT NULL,
            scenario_folder_code TEXT,
            FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id)
        );

        CREATE TABLE skill (
            skill_id TEXT PRIMARY KEY,
            scenario_id TEXT NOT NULL,
            skill_code TEXT NOT NULL,
            FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id)
        );

        CREATE TABLE contract (
            contract_id TEXT PRIMARY KEY,
            scenario_id TEXT NOT NULL,
            contract_code TEXT NOT NULL,
            min_total_assignments INTEGER NOT NULL,
            max_total_assignments INTEGER NOT NULL,
            min_consecutive_work_days INTEGER NOT NULL,
            max_consecutive_work_days INTEGER NOT NULL,
            min_consecutive_days_off INTEGER NOT NULL,
            max_consecutive_days_off INTEGER NOT NULL,
            max_working_weekends INTEGER NOT NULL,
            complete_weekend_required INTEGER NOT NULL,
            FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id)
        );

        CREATE TABLE nurse (
            nurse_id TEXT PRIMARY KEY,
            scenario_id TEXT NOT NULL,
            nurse_code TEXT NOT NULL,
            contract_id TEXT NOT NULL,
            FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id),
            FOREIGN KEY (contract_id) REFERENCES contract(contract_id)
        );

        CREATE TABLE nurse_skill (
            nurse_id TEXT NOT NULL,
            skill_id TEXT NOT NULL,
            PRIMARY KEY (nurse_id, skill_id),
            FOREIGN KEY (nurse_id) REFERENCES nurse(nurse_id),
            FOREIGN KEY (skill_id) REFERENCES skill(skill_id)
        );

        CREATE TABLE shift_type (
            shift_type_id TEXT PRIMARY KEY,
            scenario_id TEXT NOT NULL,
            shift_code TEXT NOT NULL,
            min_consecutive_shift_assignments INTEGER NOT NULL,
            max_consecutive_shift_assignments INTEGER NOT NULL,
            FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id)
        );

        CREATE TABLE forbidden_shift_succession (
            scenario_id TEXT NOT NULL,
            prev_shift_type_id TEXT NOT NULL,
            next_shift_type_id TEXT NOT NULL,
            is_forbidden INTEGER NOT NULL,
            PRIMARY KEY (scenario_id, prev_shift_type_id, next_shift_type_id),
            FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id),
            FOREIGN KEY (prev_shift_type_id) REFERENCES shift_type(shift_type_id),
            FOREIGN KEY (next_shift_type_id) REFERENCES shift_type(shift_type_id)
        );

        CREATE TABLE week (
            week_id TEXT PRIMARY KEY,
            scenario_id TEXT NOT NULL,
            week_index INTEGER NOT NULL,
            week_code TEXT NOT NULL,
            source_file TEXT NOT NULL,
            FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id)
        );

        CREATE TABLE day (
            day_id TEXT PRIMARY KEY,
            week_id TEXT NOT NULL,
            day_index INTEGER NOT NULL,
            day_name TEXT NOT NULL,
            day_short_name TEXT NOT NULL,
            is_weekend INTEGER NOT NULL,
            FOREIGN KEY (week_id) REFERENCES week(week_id)
        );

        CREATE TABLE coverage_requirement (
            requirement_id TEXT PRIMARY KEY,
            week_id TEXT NOT NULL,
            day_id TEXT NOT NULL,
            shift_type_id TEXT NOT NULL,
            skill_id TEXT NOT NULL,
            min_required INTEGER NOT NULL,
            optimal_required INTEGER NOT NULL,
            FOREIGN KEY (week_id) REFERENCES week(week_id),
            FOREIGN KEY (day_id) REFERENCES day(day_id),
            FOREIGN KEY (shift_type_id) REFERENCES shift_type(shift_type_id),
            FOREIGN KEY (skill_id) REFERENCES skill(skill_id)
        );

        CREATE TABLE nurse_request (
            request_id TEXT PRIMARY KEY,
            week_id TEXT NOT NULL,
            nurse_id TEXT NOT NULL,
            day_id TEXT NOT NULL,
            requested_off_shift_type_id TEXT,
            request_type TEXT NOT NULL,
            FOREIGN KEY (week_id) REFERENCES week(week_id),
            FOREIGN KEY (nurse_id) REFERENCES nurse(nurse_id),
            FOREIGN KEY (day_id) REFERENCES day(day_id),
            FOREIGN KEY (requested_off_shift_type_id) REFERENCES shift_type(shift_type_id)
        );

        CREATE TABLE history_snapshot (
            history_id TEXT PRIMARY KEY,
            scenario_id TEXT,
            instance_id TEXT,
            week_index_before_solve INTEGER NOT NULL,
            source_file TEXT,
            snapshot_type TEXT NOT NULL,
            history_variant_code TEXT,
            FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id),
            FOREIGN KEY (instance_id) REFERENCES instance(instance_id)
        );

        CREATE TABLE nurse_history_state (
            history_id TEXT NOT NULL,
            nurse_id TEXT NOT NULL,
            last_shift_type_id TEXT,
            consecutive_same_shift_count INTEGER NOT NULL,
            consecutive_work_days_count INTEGER NOT NULL,
            consecutive_days_off_count INTEGER NOT NULL,
            total_worked_shifts_so_far INTEGER NOT NULL,
            total_working_weekends_so_far INTEGER NOT NULL,
            PRIMARY KEY (history_id, nurse_id),
            FOREIGN KEY (history_id) REFERENCES history_snapshot(history_id),
            FOREIGN KEY (nurse_id) REFERENCES nurse(nurse_id),
            FOREIGN KEY (last_shift_type_id) REFERENCES shift_type(shift_type_id)
        );

        CREATE TABLE instance (
            instance_id TEXT PRIMARY KEY,
            dataset_id TEXT NOT NULL,
            dataset_family TEXT NOT NULL,
            problem_mode TEXT NOT NULL,
            scenario_id TEXT,
            initial_history_id TEXT,
            num_weeks INTEGER NOT NULL,
            instance_code TEXT NOT NULL,
            native_case_code TEXT,
            instance_dir_name TEXT,
            FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id),
            FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id),
            FOREIGN KEY (initial_history_id) REFERENCES history_snapshot(history_id)
        );

        CREATE TABLE instance_week_map (
            instance_id TEXT NOT NULL,
            stage_index INTEGER NOT NULL,
            week_id TEXT NOT NULL,
            PRIMARY KEY (instance_id, stage_index),
            FOREIGN KEY (instance_id) REFERENCES instance(instance_id),
            FOREIGN KEY (week_id) REFERENCES week(week_id)
        );

        CREATE TABLE solver_run (
            solver_run_id TEXT PRIMARY KEY,
            instance_id TEXT NOT NULL,
            algorithm_name TEXT NOT NULL,
            variant_name TEXT NOT NULL,
            seed INTEGER,
            runtime_sec REAL,
            status TEXT NOT NULL,
            FOREIGN KEY (instance_id) REFERENCES instance(instance_id)
        );

        CREATE TABLE assignment (
            assignment_id TEXT PRIMARY KEY,
            instance_id TEXT NOT NULL,
            stage_index INTEGER NOT NULL,
            week_id TEXT NOT NULL,
            day_id TEXT NOT NULL,
            nurse_id TEXT NOT NULL,
            shift_type_id TEXT NOT NULL,
            skill_id TEXT NOT NULL,
            solver_run_id TEXT,
            source_file TEXT,
            source_kind TEXT NOT NULL,
            FOREIGN KEY (instance_id) REFERENCES instance(instance_id),
            FOREIGN KEY (week_id) REFERENCES week(week_id),
            FOREIGN KEY (day_id) REFERENCES day(day_id),
            FOREIGN KEY (nurse_id) REFERENCES nurse(nurse_id),
            FOREIGN KEY (shift_type_id) REFERENCES shift_type(shift_type_id),
            FOREIGN KEY (skill_id) REFERENCES skill(skill_id),
            FOREIGN KEY (solver_run_id) REFERENCES solver_run(solver_run_id)
        );

        CREATE TABLE sb_case (
            sb_case_id TEXT PRIMARY KEY,
            dataset_id TEXT NOT NULL,
            instance_id TEXT NOT NULL,
            case_code TEXT NOT NULL,
            source_ros_file TEXT NOT NULL,
            source_txt_file TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            global_min_rest_minutes INTEGER,
            FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id),
            FOREIGN KEY (instance_id) REFERENCES instance(instance_id)
        );

        CREATE TABLE sb_day (
            sb_day_id TEXT PRIMARY KEY,
            sb_case_id TEXT NOT NULL,
            day_index INTEGER NOT NULL,
            calendar_date TEXT NOT NULL,
            day_name TEXT NOT NULL,
            day_of_week_index INTEGER NOT NULL,
            is_weekend INTEGER NOT NULL,
            FOREIGN KEY (sb_case_id) REFERENCES sb_case(sb_case_id)
        );

        CREATE TABLE sb_shift_type (
            sb_shift_type_id TEXT PRIMARY KEY,
            sb_case_id TEXT NOT NULL,
            shift_code TEXT NOT NULL,
            duration_minutes INTEGER NOT NULL,
            start_time TEXT,
            end_time TEXT,
            color TEXT,
            FOREIGN KEY (sb_case_id) REFERENCES sb_case(sb_case_id)
        );

        CREATE TABLE sb_contract (
            sb_contract_id TEXT PRIMARY KEY,
            sb_case_id TEXT NOT NULL,
            contract_code TEXT NOT NULL,
            min_rest_minutes INTEGER,
            min_total_minutes INTEGER,
            max_total_minutes INTEGER,
            min_consecutive_shifts INTEGER,
            max_consecutive_shifts INTEGER,
            min_consecutive_days_off INTEGER,
            max_working_weekends INTEGER,
            valid_shift_codes TEXT,
            is_meta_contract INTEGER NOT NULL,
            FOREIGN KEY (sb_case_id) REFERENCES sb_case(sb_case_id)
        );

        CREATE TABLE sb_employee (
            sb_employee_id TEXT PRIMARY KEY,
            sb_case_id TEXT NOT NULL,
            employee_code TEXT NOT NULL,
            sb_contract_id TEXT NOT NULL,
            FOREIGN KEY (sb_case_id) REFERENCES sb_case(sb_case_id),
            FOREIGN KEY (sb_contract_id) REFERENCES sb_contract(sb_contract_id)
        );

        CREATE TABLE sb_employee_shift_limit (
            sb_employee_id TEXT NOT NULL,
            sb_shift_type_id TEXT NOT NULL,
            max_assignments INTEGER NOT NULL,
            PRIMARY KEY (sb_employee_id, sb_shift_type_id),
            FOREIGN KEY (sb_employee_id) REFERENCES sb_employee(sb_employee_id),
            FOREIGN KEY (sb_shift_type_id) REFERENCES sb_shift_type(sb_shift_type_id)
        );

        CREATE TABLE sb_fixed_assignment (
            sb_fixed_assignment_id TEXT PRIMARY KEY,
            sb_case_id TEXT NOT NULL,
            sb_day_id TEXT NOT NULL,
            sb_employee_id TEXT NOT NULL,
            sb_shift_type_id TEXT,
            assignment_code TEXT NOT NULL,
            is_off INTEGER NOT NULL,
            FOREIGN KEY (sb_case_id) REFERENCES sb_case(sb_case_id),
            FOREIGN KEY (sb_day_id) REFERENCES sb_day(sb_day_id),
            FOREIGN KEY (sb_employee_id) REFERENCES sb_employee(sb_employee_id),
            FOREIGN KEY (sb_shift_type_id) REFERENCES sb_shift_type(sb_shift_type_id)
        );

        CREATE TABLE sb_request (
            sb_request_id TEXT PRIMARY KEY,
            sb_case_id TEXT NOT NULL,
            sb_day_id TEXT NOT NULL,
            sb_employee_id TEXT NOT NULL,
            sb_shift_type_id TEXT NOT NULL,
            request_type TEXT NOT NULL,
            weight INTEGER NOT NULL,
            FOREIGN KEY (sb_case_id) REFERENCES sb_case(sb_case_id),
            FOREIGN KEY (sb_day_id) REFERENCES sb_day(sb_day_id),
            FOREIGN KEY (sb_employee_id) REFERENCES sb_employee(sb_employee_id),
            FOREIGN KEY (sb_shift_type_id) REFERENCES sb_shift_type(sb_shift_type_id)
        );

        CREATE TABLE sb_cover_requirement (
            sb_cover_requirement_id TEXT PRIMARY KEY,
            sb_case_id TEXT NOT NULL,
            sb_day_id TEXT NOT NULL,
            sb_shift_type_id TEXT NOT NULL,
            min_required INTEGER NOT NULL,
            preferred_required INTEGER NOT NULL,
            under_weight INTEGER NOT NULL,
            over_weight INTEGER NOT NULL,
            FOREIGN KEY (sb_case_id) REFERENCES sb_case(sb_case_id),
            FOREIGN KEY (sb_day_id) REFERENCES sb_day(sb_day_id),
            FOREIGN KEY (sb_shift_type_id) REFERENCES sb_shift_type(sb_shift_type_id)
        );

        CREATE TABLE sb_assignment (
            sb_assignment_id TEXT PRIMARY KEY,
            instance_id TEXT NOT NULL,
            sb_case_id TEXT NOT NULL,
            sb_day_id TEXT NOT NULL,
            sb_employee_id TEXT NOT NULL,
            sb_shift_type_id TEXT NOT NULL,
            solver_run_id TEXT,
            source_file TEXT,
            source_kind TEXT NOT NULL,
            FOREIGN KEY (instance_id) REFERENCES instance(instance_id),
            FOREIGN KEY (sb_case_id) REFERENCES sb_case(sb_case_id),
            FOREIGN KEY (sb_day_id) REFERENCES sb_day(sb_day_id),
            FOREIGN KEY (sb_employee_id) REFERENCES sb_employee(sb_employee_id),
            FOREIGN KEY (sb_shift_type_id) REFERENCES sb_shift_type(sb_shift_type_id),
            FOREIGN KEY (solver_run_id) REFERENCES solver_run(solver_run_id)
        );

        CREATE TABLE constraint_result (
            constraint_result_id TEXT PRIMARY KEY,
            solver_run_id TEXT NOT NULL,
            week_id TEXT,
            nurse_id TEXT,
            constraint_code TEXT NOT NULL,
            violation_count INTEGER NOT NULL,
            penalty_cost REAL NOT NULL,
            is_hard INTEGER NOT NULL,
            evaluation_scope TEXT NOT NULL,
            FOREIGN KEY (solver_run_id) REFERENCES solver_run(solver_run_id),
            FOREIGN KEY (week_id) REFERENCES week(week_id),
            FOREIGN KEY (nurse_id) REFERENCES nurse(nurse_id)
        );

        CREATE TABLE ablation_result (
            ablation_result_id TEXT PRIMARY KEY,
            solver_run_id TEXT NOT NULL,
            ablation_group TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            notes TEXT,
            FOREIGN KEY (solver_run_id) REFERENCES solver_run(solver_run_id)
        );
        """
    )


def _insert_many(conn: sqlite3.Connection, table_name: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = TABLE_COLUMNS[table_name]
    placeholders = ", ".join("?" for _ in columns)
    column_sql = ", ".join(columns)
    values = [tuple(row.get(column) for column in columns) for row in rows]
    conn.executemany(f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})", values)


def _insert_rows(conn: sqlite3.Connection, tables: dict[str, list[dict[str, Any]]]) -> None:
    _insert_many(conn, "dataset", tables["dataset"])
    _insert_many(conn, "raw_document", tables["raw_document"])
    _insert_many(conn, "scenario", tables["scenario"])
    _insert_many(conn, "skill", tables["skill"])
    _insert_many(conn, "contract", tables["contract"])
    _insert_many(conn, "nurse", tables["nurse"])
    _insert_many(conn, "nurse_skill", tables["nurse_skill"])
    _insert_many(conn, "shift_type", tables["shift_type"])
    _insert_many(conn, "forbidden_shift_succession", tables["forbidden_shift_succession"])
    _insert_many(conn, "week", tables["week"])
    _insert_many(conn, "day", tables["day"])
    _insert_many(conn, "coverage_requirement", tables["coverage_requirement"])
    _insert_many(conn, "nurse_request", tables["nurse_request"])

    source_history_rows = [row for row in tables["history_snapshot"] if row["snapshot_type"] == SNAPSHOT_TYPE_INITIAL_SOURCE]
    derived_history_rows = [row for row in tables["history_snapshot"] if row["snapshot_type"] != SNAPSHOT_TYPE_INITIAL_SOURCE]
    source_history_ids = {row["history_id"] for row in source_history_rows}
    source_state_rows = [row for row in tables["nurse_history_state"] if row["history_id"] in source_history_ids]
    derived_state_rows = [row for row in tables["nurse_history_state"] if row["history_id"] not in source_history_ids]

    _insert_many(conn, "history_snapshot", source_history_rows)
    _insert_many(conn, "nurse_history_state", source_state_rows)
    _insert_many(conn, "instance", tables["instance"])
    _insert_many(conn, "instance_week_map", tables["instance_week_map"])
    _insert_many(conn, "history_snapshot", derived_history_rows)
    _insert_many(conn, "nurse_history_state", derived_state_rows)
    _insert_many(conn, "solver_run", tables["solver_run"])
    _insert_many(conn, "assignment", tables["assignment"])
    _insert_many(conn, "sb_case", tables["sb_case"])
    _insert_many(conn, "sb_day", tables["sb_day"])
    _insert_many(conn, "sb_shift_type", tables["sb_shift_type"])
    _insert_many(conn, "sb_contract", tables["sb_contract"])
    _insert_many(conn, "sb_employee", tables["sb_employee"])
    _insert_many(conn, "sb_employee_shift_limit", tables["sb_employee_shift_limit"])
    _insert_many(conn, "sb_fixed_assignment", tables["sb_fixed_assignment"])
    _insert_many(conn, "sb_request", tables["sb_request"])
    _insert_many(conn, "sb_cover_requirement", tables["sb_cover_requirement"])
    _insert_many(conn, "sb_assignment", tables["sb_assignment"])
    _insert_many(conn, "constraint_result", tables["constraint_result"])
    _insert_many(conn, "ablation_result", tables["ablation_result"])


def _create_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX idx_week_scenario ON week(scenario_id);
        CREATE INDEX idx_day_week ON day(week_id);
        CREATE INDEX idx_coverage_week ON coverage_requirement(week_id);
        CREATE INDEX idx_request_week_nurse ON nurse_request(week_id, nurse_id);
        CREATE INDEX idx_history_instance ON history_snapshot(instance_id, week_index_before_solve);
        CREATE INDEX idx_assignment_instance_stage ON assignment(instance_id, stage_index, nurse_id, day_id);
        CREATE INDEX idx_assignment_week_day_shift_skill ON assignment(week_id, day_id, shift_type_id, skill_id);
        CREATE INDEX idx_instance_stage_week ON instance_week_map(instance_id, stage_index, week_id);
        CREATE INDEX idx_instance_family_mode ON instance(dataset_family, problem_mode);
        CREATE INDEX idx_sb_day_case ON sb_day(sb_case_id, day_index);
        CREATE INDEX idx_sb_cover_case_day_shift ON sb_cover_requirement(sb_case_id, sb_day_id, sb_shift_type_id);
        CREATE INDEX idx_sb_request_case_employee ON sb_request(sb_case_id, sb_employee_id, sb_day_id);
        """
    )


def _create_views(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE VIEW instance_day_view AS
        SELECT
            i.instance_id,
            i.scenario_id,
            iwm.stage_index,
            iwm.week_id,
            d.day_id,
            d.day_index,
            d.day_name,
            d.day_short_name,
            d.is_weekend,
            (iwm.stage_index * 7) + d.day_index AS global_day_index
        FROM instance AS i
        JOIN instance_week_map AS iwm ON iwm.instance_id = i.instance_id
        JOIN day AS d ON d.week_id = iwm.week_id;

        CREATE VIEW roster_day_view AS
        SELECT
            i.instance_id,
            i.scenario_id,
            idv.stage_index,
            idv.week_id,
            idv.day_id,
            idv.day_index,
            idv.day_name,
            idv.day_short_name,
            idv.is_weekend,
            idv.global_day_index,
            n.nurse_id,
            n.nurse_code,
            a.assignment_id,
            a.shift_type_id,
            COALESCE(st.shift_code, 'OFF') AS shift_code,
            a.skill_id,
            sk.skill_code,
            CASE WHEN a.assignment_id IS NULL THEN 1 ELSE 0 END AS is_off
        FROM instance AS i
        JOIN instance_day_view AS idv ON idv.instance_id = i.instance_id
        JOIN nurse AS n ON n.scenario_id = i.scenario_id
        LEFT JOIN assignment AS a
            ON a.instance_id = i.instance_id
            AND a.stage_index = idv.stage_index
            AND a.day_id = idv.day_id
            AND a.nurse_id = n.nurse_id
        LEFT JOIN shift_type AS st ON st.shift_type_id = a.shift_type_id
        LEFT JOIN skill AS sk ON sk.skill_id = a.skill_id;

        CREATE VIEW coverage_summary_view AS
        WITH assigned AS (
            SELECT
                instance_id,
                stage_index,
                week_id,
                day_id,
                shift_type_id,
                skill_id,
                COUNT(*) AS assigned_count
            FROM assignment
            GROUP BY instance_id, stage_index, week_id, day_id, shift_type_id, skill_id
        )
        SELECT
            iwm.instance_id,
            i.scenario_id,
            iwm.stage_index,
            cr.week_id,
            cr.day_id,
            d.day_name,
            d.day_short_name,
            cr.shift_type_id,
            st.shift_code,
            cr.skill_id,
            sk.skill_code,
            cr.min_required,
            cr.optimal_required,
            COALESCE(a.assigned_count, 0) AS assigned_count,
            cr.min_required - COALESCE(a.assigned_count, 0) AS min_gap,
            cr.optimal_required - COALESCE(a.assigned_count, 0) AS optimal_gap
        FROM instance_week_map AS iwm
        JOIN instance AS i ON i.instance_id = iwm.instance_id
        JOIN coverage_requirement AS cr ON cr.week_id = iwm.week_id
        JOIN day AS d ON d.day_id = cr.day_id
        JOIN shift_type AS st ON st.shift_type_id = cr.shift_type_id
        JOIN skill AS sk ON sk.skill_id = cr.skill_id
        LEFT JOIN assigned AS a
            ON a.instance_id = iwm.instance_id
            AND a.stage_index = iwm.stage_index
            AND a.week_id = cr.week_id
            AND a.day_id = cr.day_id
            AND a.shift_type_id = cr.shift_type_id
            AND a.skill_id = cr.skill_id;

        CREATE VIEW nurse_week_summary_view AS
        WITH base AS (
            SELECT
                r.instance_id,
                r.stage_index,
                r.week_id,
                r.nurse_id,
                n.nurse_code,
                SUM(CASE WHEN r.is_off = 0 THEN 1 ELSE 0 END) AS worked_days,
                SUM(CASE WHEN r.is_off = 1 THEN 1 ELSE 0 END) AS days_off,
                SUM(CASE WHEN r.is_off = 0 AND r.is_weekend = 1 THEN 1 ELSE 0 END) AS weekend_days_worked,
                MAX(CASE WHEN r.is_off = 0 AND r.is_weekend = 1 THEN 1 ELSE 0 END) AS weekends_worked,
                SUM(CASE WHEN r.is_off = 0 AND r.shift_code = 'Night' THEN 1 ELSE 0 END) AS night_assignments,
                SUM(CASE WHEN r.is_off = 0 THEN 1 ELSE 0 END) AS total_assignments
            FROM roster_day_view AS r
            JOIN nurse AS n ON n.nurse_id = r.nurse_id
            GROUP BY r.instance_id, r.stage_index, r.week_id, r.nurse_id, n.nurse_code
        ),
        request_hits AS (
            SELECT
                a.instance_id,
                a.stage_index,
                a.week_id,
                a.nurse_id,
                COUNT(*) AS undesired_assignments
            FROM assignment AS a
            JOIN nurse_request AS nr
                ON nr.week_id = a.week_id
                AND nr.nurse_id = a.nurse_id
                AND nr.day_id = a.day_id
            WHERE nr.request_type = 'OFF_ANY'
               OR nr.requested_off_shift_type_id = a.shift_type_id
            GROUP BY a.instance_id, a.stage_index, a.week_id, a.nurse_id
        )
        SELECT
            base.instance_id,
            base.stage_index,
            base.week_id,
            base.nurse_id,
            base.nurse_code,
            base.worked_days,
            base.days_off,
            base.weekend_days_worked,
            base.weekends_worked,
            base.night_assignments,
            base.total_assignments,
            COALESCE(request_hits.undesired_assignments, 0) AS undesired_assignments,
            AVG(base.worked_days) OVER (PARTITION BY base.instance_id, base.stage_index) AS mean_worked_days_in_stage,
            base.worked_days - AVG(base.worked_days) OVER (PARTITION BY base.instance_id, base.stage_index) AS worked_days_minus_stage_mean
        FROM base
        LEFT JOIN request_hits
            ON request_hits.instance_id = base.instance_id
            AND request_hits.stage_index = base.stage_index
            AND request_hits.week_id = base.week_id
            AND request_hits.nurse_id = base.nurse_id;

        CREATE VIEW history_transition_view AS
        WITH ordered_histories AS (
            SELECT
                hs.instance_id,
                hs.history_id,
                hs.week_index_before_solve,
                LAG(hs.history_id) OVER (
                    PARTITION BY hs.instance_id
                    ORDER BY hs.week_index_before_solve
                ) AS previous_history_id
            FROM history_snapshot AS hs
            WHERE hs.instance_id IS NOT NULL
        )
        SELECT
            oh.instance_id,
            oh.week_index_before_solve - 1 AS stage_index_completed,
            curr.nurse_id,
            n.nurse_code,
            prev.last_shift_type_id AS previous_last_shift_type_id,
            prev_shift.shift_code AS previous_last_shift_code,
            curr.last_shift_type_id AS new_last_shift_type_id,
            curr_shift.shift_code AS new_last_shift_code,
            prev.consecutive_same_shift_count AS previous_consecutive_same_shift_count,
            curr.consecutive_same_shift_count AS new_consecutive_same_shift_count,
            prev.consecutive_work_days_count AS previous_consecutive_work_days_count,
            curr.consecutive_work_days_count AS new_consecutive_work_days_count,
            prev.consecutive_days_off_count AS previous_consecutive_days_off_count,
            curr.consecutive_days_off_count AS new_consecutive_days_off_count,
            prev.total_worked_shifts_so_far AS previous_total_worked_shifts_so_far,
            curr.total_worked_shifts_so_far AS new_total_worked_shifts_so_far,
            prev.total_working_weekends_so_far AS previous_total_working_weekends_so_far,
            curr.total_working_weekends_so_far AS new_total_working_weekends_so_far
        FROM ordered_histories AS oh
        JOIN nurse_history_state AS curr ON curr.history_id = oh.history_id
        JOIN nurse_history_state AS prev
            ON prev.history_id = oh.previous_history_id
            AND prev.nurse_id = curr.nurse_id
        JOIN nurse AS n ON n.nurse_id = curr.nurse_id
        LEFT JOIN shift_type AS prev_shift ON prev_shift.shift_type_id = prev.last_shift_type_id
        LEFT JOIN shift_type AS curr_shift ON curr_shift.shift_type_id = curr.last_shift_type_id
        WHERE oh.previous_history_id IS NOT NULL;
        """
    )
