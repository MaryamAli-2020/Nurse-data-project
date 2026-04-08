"""End-to-end pipeline for building the roster data foundation."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .database import build_database
from .discovery import (
    build_scan_summary,
    discover_dataset_roots,
    discover_instance_dirs,
    discover_source_files,
    discover_static_cases,
)
from .features import build_feature_bundles
from .parse import build_canonical_tables
from .reporting import write_reports
from .validation import validate_foundation


def run_pipeline(project_root: Path) -> dict[str, Any]:
    project_root = project_root.resolve()
    dataset_roots = discover_dataset_roots(project_root)
    source_files = []
    instance_dirs = []
    static_cases = []
    for dataset_root in dataset_roots:
        dataset_source_files = discover_source_files(dataset_root)
        source_files.extend(dataset_source_files)
        instance_dirs.extend(discover_instance_dirs(dataset_root))
        static_cases.extend(discover_static_cases(dataset_root, dataset_source_files))

    scan_summary = build_scan_summary(dataset_roots, source_files, instance_dirs, static_cases)
    tables, metadata = build_canonical_tables(dataset_roots, source_files, instance_dirs, static_cases)

    processed_dir = project_root / "data" / "processed"
    reports_dir = project_root / "reports"
    features_dir = processed_dir / "feature_bundles"
    db_path = processed_dir / "inrc2_foundation.sqlite"

    actual_db_path = build_database(db_path, tables)
    validation_report = validate_foundation(actual_db_path, tables, scan_summary)
    report_paths = write_reports(reports_dir, actual_db_path, scan_summary, validation_report)
    feature_manifest = build_feature_bundles(actual_db_path, features_dir)

    summary = {
        "project_root": str(project_root),
        "database_path": str(actual_db_path.resolve()),
        "reports": report_paths,
        "feature_manifest": feature_manifest,
        "table_counts": _table_counts(actual_db_path),
        "validation": {
            "errors": validation_report.error_count,
            "warnings": validation_report.warning_count,
        },
        "scan_summary": scan_summary,
        "metadata": {
            "scenario_by_folder": {f"{key[0]}::{key[1]}": value for key, value in metadata["scenario_by_folder"].items()},
            "planning_horizon_by_scenario": metadata["planning_horizon_by_scenario"],
            "scheduling_case_by_instance": metadata["scheduling_case_by_instance"],
        },
    }
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "pipeline_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def _table_counts(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    counts: dict[str, int] = {}
    for table_name in (
        "dataset",
        "raw_document",
        "scenario",
        "skill",
        "contract",
        "nurse",
        "nurse_skill",
        "shift_type",
        "forbidden_shift_succession",
        "week",
        "day",
        "coverage_requirement",
        "nurse_request",
        "history_snapshot",
        "nurse_history_state",
        "instance",
        "instance_week_map",
        "assignment",
        "sb_case",
        "sb_day",
        "sb_shift_type",
        "sb_contract",
        "sb_employee",
        "sb_employee_shift_limit",
        "sb_fixed_assignment",
        "sb_request",
        "sb_cover_requirement",
        "sb_assignment",
    ):
        counts[table_name] = int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    conn.close()
    return counts
