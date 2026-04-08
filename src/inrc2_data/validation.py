"""Structural and referential validation for the multi-family roster foundation."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .models import ValidationReport


def validate_foundation(
    db_path: Path,
    tables: dict[str, list[dict[str, Any]]],
    scan_summary: dict[str, Any],
) -> ValidationReport:
    report = ValidationReport()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    _check_foreign_keys(conn, report)
    _check_reference_integrity(conn, report)
    _check_natural_key_duplicates(conn, report)
    _check_instance_completeness(conn, report)
    _check_scan_inconsistencies(scan_summary, report)
    _add_table_count_checks(conn, report)

    conn.close()
    return report


def _check_foreign_keys(conn: sqlite3.Connection, report: ValidationReport) -> None:
    issues = list(conn.execute("PRAGMA foreign_key_check").fetchall())
    report.add_check(
        name="sqlite_foreign_key_check",
        passed=(len(issues) == 0),
        details="SQLite foreign-key check across all canonical tables.",
        count=len(issues),
    )
    for issue in issues:
        report.add_issue(
            level="ERROR",
            code="BROKEN_FOREIGN_KEY",
            message=f"Broken foreign key in table {issue['table']} at rowid {issue['rowid']}.",
            context={"table": issue["table"], "rowid": issue["rowid"], "parent": issue["parent"]},
        )


def _missing_fk_count(conn: sqlite3.Connection, sql: str) -> int:
    return int(conn.execute(sql).fetchone()[0])


def _check_reference_integrity(conn: sqlite3.Connection, report: ValidationReport) -> None:
    checks = {
        "nurse_contract_reference": """
            SELECT COUNT(*)
            FROM nurse AS n
            LEFT JOIN contract AS c ON c.contract_id = n.contract_id
            WHERE c.contract_id IS NULL
        """,
        "nurse_skill_nurse_reference": """
            SELECT COUNT(*)
            FROM nurse_skill AS ns
            LEFT JOIN nurse AS n ON n.nurse_id = ns.nurse_id
            WHERE n.nurse_id IS NULL
        """,
        "nurse_skill_skill_reference": """
            SELECT COUNT(*)
            FROM nurse_skill AS ns
            LEFT JOIN skill AS s ON s.skill_id = ns.skill_id
            WHERE s.skill_id IS NULL
        """,
        "forbidden_prev_shift_reference": """
            SELECT COUNT(*)
            FROM forbidden_shift_succession AS f
            LEFT JOIN shift_type AS s ON s.shift_type_id = f.prev_shift_type_id
            WHERE s.shift_type_id IS NULL
        """,
        "forbidden_next_shift_reference": """
            SELECT COUNT(*)
            FROM forbidden_shift_succession AS f
            LEFT JOIN shift_type AS s ON s.shift_type_id = f.next_shift_type_id
            WHERE s.shift_type_id IS NULL
        """,
        "week_scenario_reference": """
            SELECT COUNT(*)
            FROM week AS w
            LEFT JOIN scenario AS s ON s.scenario_id = w.scenario_id
            WHERE s.scenario_id IS NULL
        """,
        "coverage_week_reference": """
            SELECT COUNT(*)
            FROM coverage_requirement AS c
            LEFT JOIN week AS w ON w.week_id = c.week_id
            WHERE w.week_id IS NULL
        """,
        "coverage_day_reference": """
            SELECT COUNT(*)
            FROM coverage_requirement AS c
            LEFT JOIN day AS d ON d.day_id = c.day_id
            WHERE d.day_id IS NULL
        """,
        "coverage_shift_reference": """
            SELECT COUNT(*)
            FROM coverage_requirement AS c
            LEFT JOIN shift_type AS s ON s.shift_type_id = c.shift_type_id
            WHERE s.shift_type_id IS NULL
        """,
        "coverage_skill_reference": """
            SELECT COUNT(*)
            FROM coverage_requirement AS c
            LEFT JOIN skill AS s ON s.skill_id = c.skill_id
            WHERE s.skill_id IS NULL
        """,
        "request_nurse_reference": """
            SELECT COUNT(*)
            FROM nurse_request AS r
            LEFT JOIN nurse AS n ON n.nurse_id = r.nurse_id
            WHERE n.nurse_id IS NULL
        """,
        "request_day_reference": """
            SELECT COUNT(*)
            FROM nurse_request AS r
            LEFT JOIN day AS d ON d.day_id = r.day_id
            WHERE d.day_id IS NULL
        """,
        "history_nurse_reference": """
            SELECT COUNT(*)
            FROM nurse_history_state AS h
            LEFT JOIN nurse AS n ON n.nurse_id = h.nurse_id
            WHERE n.nurse_id IS NULL
        """,
        "instance_week_map_reference": """
            SELECT COUNT(*)
            FROM instance_week_map AS iwm
            LEFT JOIN week AS w ON w.week_id = iwm.week_id
            WHERE w.week_id IS NULL
        """,
        "sb_case_instance_reference": """
            SELECT COUNT(*)
            FROM sb_case AS c
            LEFT JOIN instance AS i ON i.instance_id = c.instance_id
            WHERE i.instance_id IS NULL
        """,
        "sb_day_case_reference": """
            SELECT COUNT(*)
            FROM sb_day AS d
            LEFT JOIN sb_case AS c ON c.sb_case_id = d.sb_case_id
            WHERE c.sb_case_id IS NULL
        """,
        "sb_employee_contract_reference": """
            SELECT COUNT(*)
            FROM sb_employee AS e
            LEFT JOIN sb_contract AS c ON c.sb_contract_id = e.sb_contract_id
            WHERE c.sb_contract_id IS NULL
        """,
        "sb_shift_limit_employee_reference": """
            SELECT COUNT(*)
            FROM sb_employee_shift_limit AS l
            LEFT JOIN sb_employee AS e ON e.sb_employee_id = l.sb_employee_id
            WHERE e.sb_employee_id IS NULL
        """,
        "sb_shift_limit_shift_reference": """
            SELECT COUNT(*)
            FROM sb_employee_shift_limit AS l
            LEFT JOIN sb_shift_type AS s ON s.sb_shift_type_id = l.sb_shift_type_id
            WHERE s.sb_shift_type_id IS NULL
        """,
        "sb_fixed_assignment_day_reference": """
            SELECT COUNT(*)
            FROM sb_fixed_assignment AS f
            LEFT JOIN sb_day AS d ON d.sb_day_id = f.sb_day_id
            WHERE d.sb_day_id IS NULL
        """,
        "sb_fixed_assignment_employee_reference": """
            SELECT COUNT(*)
            FROM sb_fixed_assignment AS f
            LEFT JOIN sb_employee AS e ON e.sb_employee_id = f.sb_employee_id
            WHERE e.sb_employee_id IS NULL
        """,
        "sb_request_employee_reference": """
            SELECT COUNT(*)
            FROM sb_request AS r
            LEFT JOIN sb_employee AS e ON e.sb_employee_id = r.sb_employee_id
            WHERE e.sb_employee_id IS NULL
        """,
        "sb_request_day_reference": """
            SELECT COUNT(*)
            FROM sb_request AS r
            LEFT JOIN sb_day AS d ON d.sb_day_id = r.sb_day_id
            WHERE d.sb_day_id IS NULL
        """,
        "sb_cover_case_reference": """
            SELECT COUNT(*)
            FROM sb_cover_requirement AS c
            LEFT JOIN sb_case AS sc ON sc.sb_case_id = c.sb_case_id
            WHERE sc.sb_case_id IS NULL
        """,
        "sb_cover_day_reference": """
            SELECT COUNT(*)
            FROM sb_cover_requirement AS c
            LEFT JOIN sb_day AS d ON d.sb_day_id = c.sb_day_id
            WHERE d.sb_day_id IS NULL
        """,
        "sb_cover_shift_reference": """
            SELECT COUNT(*)
            FROM sb_cover_requirement AS c
            LEFT JOIN sb_shift_type AS s ON s.sb_shift_type_id = c.sb_shift_type_id
            WHERE s.sb_shift_type_id IS NULL
        """,
    }
    for name, sql in checks.items():
        count = _missing_fk_count(conn, sql)
        report.add_check(name=name, passed=(count == 0), details="Referential integrity check.", count=count)
        if count:
            report.add_issue(
                level="ERROR",
                code="REFERENCE_INTEGRITY_FAILURE",
                message=f"{name} found {count} missing references.",
                context={"check": name, "count": count},
            )


def _check_natural_key_duplicates(conn: sqlite3.Connection, report: ValidationReport) -> None:
    duplicate_queries = {
        "duplicate_coverage_natural_key": """
            SELECT COUNT(*) FROM (
                SELECT week_id, day_id, shift_type_id, skill_id, COUNT(*) AS row_count
                FROM coverage_requirement
                GROUP BY week_id, day_id, shift_type_id, skill_id
                HAVING row_count > 1
            )
        """,
        "duplicate_assignment_natural_key": """
            SELECT COUNT(*) FROM (
                SELECT instance_id, stage_index, nurse_id, day_id, COUNT(*) AS row_count
                FROM assignment
                GROUP BY instance_id, stage_index, nurse_id, day_id
                HAVING row_count > 1
            )
        """,
        "duplicate_instance_stage_mapping": """
            SELECT COUNT(*) FROM (
                SELECT instance_id, stage_index, COUNT(*) AS row_count
                FROM instance_week_map
                GROUP BY instance_id, stage_index
                HAVING row_count > 1
            )
        """,
        "duplicate_sb_cover_natural_key": """
            SELECT COUNT(*) FROM (
                SELECT sb_case_id, sb_day_id, sb_shift_type_id, COUNT(*) AS row_count
                FROM sb_cover_requirement
                GROUP BY sb_case_id, sb_day_id, sb_shift_type_id
                HAVING row_count > 1
            )
        """,
        "duplicate_sb_request_natural_key": """
            SELECT COUNT(*) FROM (
                SELECT sb_case_id, sb_employee_id, sb_day_id, sb_shift_type_id, request_type, COUNT(*) AS row_count
                FROM sb_request
                GROUP BY sb_case_id, sb_employee_id, sb_day_id, sb_shift_type_id, request_type
                HAVING row_count > 1
            )
        """,
    }
    for name, sql in duplicate_queries.items():
        count = _missing_fk_count(conn, sql)
        report.add_check(name=name, passed=(count == 0), details="Natural-key duplicate check.", count=count)
        if count:
            report.add_issue(
                level="ERROR",
                code="DUPLICATE_NATURAL_KEY",
                message=f"{name} found {count} duplicate natural-key groups.",
                context={"check": name, "count": count},
            )


def _check_instance_completeness(conn: sqlite3.Connection, report: ValidationReport) -> None:
    incomplete_instances = list(
        conn.execute(
            """
            WITH assignment_stage_counts AS (
                SELECT instance_id, COUNT(DISTINCT stage_index) AS solved_stages
                FROM assignment
                GROUP BY instance_id
            ),
            mismatch_stage_map AS (
                SELECT
                    a.instance_id,
                    a.stage_index,
                    COUNT(DISTINCT a.week_id) AS observed_week_ids,
                    MAX(CASE WHEN a.week_id = iwm.week_id THEN 1 ELSE 0 END) AS matches_expected_week
                FROM assignment AS a
                JOIN instance_week_map AS iwm
                    ON iwm.instance_id = a.instance_id
                    AND iwm.stage_index = a.stage_index
                GROUP BY a.instance_id, a.stage_index
            )
            SELECT
                i.instance_id,
                i.instance_code,
                i.num_weeks,
                COALESCE(asc.solved_stages, 0) AS solved_stages,
                (
                    SELECT COUNT(*)
                    FROM mismatch_stage_map AS msm
                    WHERE msm.instance_id = i.instance_id
                      AND msm.matches_expected_week = 0
                ) AS mismatched_stage_week_links
            FROM instance AS i
            LEFT JOIN assignment_stage_counts AS asc ON asc.instance_id = i.instance_id
            WHERE i.problem_mode = 'MULTISTAGE'
            ORDER BY i.instance_id
            """
        ).fetchall()
    )

    for row in incomplete_instances:
        solved_stages = int(row["solved_stages"])
        num_weeks = int(row["num_weeks"])
        if solved_stages != num_weeks:
            report.add_issue(
                level="WARNING",
                code="PARTIAL_REFERENCE_SOLUTION",
                message=(
                    f"Instance {row['instance_code']} has assignments for {solved_stages} "
                    f"of {num_weeks} declared stages."
                ),
                context={"instance_id": row["instance_id"], "solved_stages": solved_stages, "num_weeks": num_weeks},
            )
        if int(row["mismatched_stage_week_links"]) > 0:
            report.add_issue(
                level="ERROR",
                code="INSTANCE_STAGE_WEEK_MISMATCH",
                message=f"Assignments in {row['instance_code']} do not match the declared week sequence.",
                context={
                    "instance_id": row["instance_id"],
                    "mismatched_stage_week_links": int(row["mismatched_stage_week_links"]),
                },
            )
    report.add_check(
        name="instance_reference_solution_completeness",
        passed=(not any(issue.code == "PARTIAL_REFERENCE_SOLUTION" for issue in report.issues)),
        details="Checks whether explicit instance folders contain a full set of stage solutions.",
        count=sum(1 for issue in report.issues if issue.code == "PARTIAL_REFERENCE_SOLUTION"),
    )
    report.add_check(
        name="instance_stage_week_alignment",
        passed=(not any(issue.code == "INSTANCE_STAGE_WEEK_MISMATCH" for issue in report.issues)),
        details="Checks whether solution files align with the week sequence declared by the instance folder name.",
        count=sum(1 for issue in report.issues if issue.code == "INSTANCE_STAGE_WEEK_MISMATCH"),
    )


def _check_scan_inconsistencies(scan_summary: dict[str, Any], report: ValidationReport) -> None:
    dataset_roots = scan_summary.get("dataset_roots", [])
    alias_warnings = 0
    sb_pair_issues = 0
    for dataset_row in dataset_roots:
        for folder_row in dataset_row.get("scenario_folders", []):
            scenario_ids = folder_row.get("scenario_ids_in_xml", [])
            folder_code = folder_row.get("scenario_folder_code")
            if scenario_ids and folder_code and folder_code not in scenario_ids:
                alias_warnings += 1
                report.add_issue(
                    level="WARNING",
                    code="SCENARIO_FOLDER_ALIAS",
                    message=(
                        f"Scenario folder {folder_code} uses XML scenario ids {', '.join(scenario_ids)}. "
                        "The parser resolved this as a folder alias instead of treating it as a hard failure."
                    ),
                    context={"scenario_folder_code": folder_code, "scenario_ids_in_xml": ", ".join(scenario_ids)},
                )
        for case_row in dataset_row.get("cases", []):
            if not case_row.get("ros_present") or not case_row.get("txt_present"):
                sb_pair_issues += 1
                report.add_issue(
                    level="ERROR",
                    code="SCHEDULING_CASE_PAIR_MISSING",
                    message=f"SchedulingBenchmarks case {case_row.get('case_code')} is missing a paired .ros or .txt file.",
                    context={"case_code": case_row.get("case_code")},
                )
    report.add_check(
        name="scenario_folder_alias_review",
        passed=(alias_warnings == 0),
        details="Highlights folders whose name differs from the scenario id declared in XML.",
        count=alias_warnings,
    )
    report.add_check(
        name="scheduling_case_pair_integrity",
        passed=(sb_pair_issues == 0),
        details="Checks that each SchedulingBenchmarks case has both a .ros and a .txt source file.",
        count=sb_pair_issues,
    )


def _add_table_count_checks(conn: sqlite3.Connection, report: ValidationReport) -> None:
    for table_name in (
        "dataset",
        "scenario",
        "skill",
        "contract",
        "nurse",
        "shift_type",
        "week",
        "history_snapshot",
        "instance",
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
    ):
        count = _missing_fk_count(conn, f"SELECT COUNT(*) FROM {table_name}")
        report.add_check(
            name=f"{table_name}_row_count",
            passed=(count > 0 or table_name in {"instance", "assignment"}),
            details=f"Row count for {table_name}.",
            count=count,
        )
