"""Markdown and JSON reporting for the roster data foundation."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .schema import TABLE_SPECS
from .validation import ValidationReport


def write_reports(
    reports_dir: Path,
    db_path: Path,
    scan_summary: dict[str, Any],
    validation_report: ValidationReport,
) -> dict[str, str]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    report_paths = {
        "dataset_inspection": str(_write_dataset_inspection(reports_dir, scan_summary, validation_report)),
        "schema_summary": str(_write_schema_summary(reports_dir)),
        "data_dictionary": str(_write_data_dictionary(reports_dir)),
        "architecture_notes": str(_write_architecture_notes(reports_dir)),
        "validation_report": str(_write_validation_report(reports_dir, validation_report)),
        "example_outputs": str(_write_example_outputs(reports_dir, conn)),
        "project_structure_recommendation": str(_write_project_structure_recommendation(reports_dir)),
        "solver_integration_roadmap": str(_write_solver_integration_roadmap(reports_dir)),
    }

    conn.close()
    return report_paths


def _write_dataset_inspection(
    reports_dir: Path,
    scan_summary: dict[str, Any],
    validation_report: ValidationReport,
) -> Path:
    path = reports_dir / "dataset_inspection.md"
    lines = ["# Dataset Inspection Findings", ""]
    for dataset_row in scan_summary.get("dataset_roots", []):
        lines.extend(
            [
                f"## {dataset_row['dataset_code']}",
                "",
                f"- Root path: `{dataset_row['root_path']}`",
                f"- Dataset family: `{dataset_row['dataset_family']}`",
                f"- Problem mode: `{dataset_row['problem_mode']}`",
                f"- Scan root: `{dataset_row['scan_root_path']}`",
                f"- Source files: {dataset_row['source_file_count']}",
                "",
            ]
        )
        if dataset_row["dataset_family"] == "INRC_II":
            lines.extend(
                [
                    f"- Scenario folders: {dataset_row['scenario_folder_count']}",
                    f"- Explicit instance folders: {dataset_row['instance_dir_count']}",
                    "",
                    "| Scenario Folder | Scenario XML | History XML | Week XML | Solution XML | Solution Dirs | Scenario IDs In XML |",
                    "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
                ]
            )
            for folder_row in dataset_row.get("scenario_folders", []):
                lines.append(
                    "| {folder} | {sc} | {hc} | {wc} | {sol} | {dirs} | {xml_ids} |".format(
                        folder=folder_row["scenario_folder_code"],
                        sc=folder_row["scenario_xml_count"],
                        hc=folder_row["history_xml_count"],
                        wc=folder_row["week_xml_count"],
                        sol=folder_row["solution_xml_count"],
                        dirs=folder_row["solution_dir_count"],
                        xml_ids=", ".join(folder_row["scenario_ids_in_xml"]),
                    )
                )
        else:
            lines.extend(
                [
                    f"- Cases: {dataset_row['case_count']}",
                    "",
                    "| Case | .ros | .txt | File Count |",
                    "| --- | --- | --- | ---: |",
                ]
            )
            for case_row in dataset_row.get("cases", []):
                lines.append(
                    f"| {case_row['case_code']} | {'yes' if case_row['ros_present'] else 'no'} | {'yes' if case_row['txt_present'] else 'no'} | {case_row['file_count']} |"
                )
        lines.append("")

    lines.extend(["## Inconsistencies", ""])
    issues = [issue for issue in validation_report.issues if issue.level == "WARNING"]
    if not issues:
        lines.append("- No scan-time inconsistencies were detected.")
    else:
        for issue in issues:
            lines.append(f"- `{issue.code}`: {issue.message}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_schema_summary(reports_dir: Path) -> Path:
    path = reports_dir / "schema_summary.md"
    lines = ["# Canonical Schema Summary", ""]
    for table_spec in TABLE_SPECS:
        lines.extend(
            [
                f"## {table_spec.name}",
                "",
                f"- Layer: {table_spec.layer}",
                f"- Description: {table_spec.description}",
                f"- Primary key: `{', '.join(table_spec.primary_key) if table_spec.primary_key else 'view'}`",
                f"- Foreign keys: `{'; '.join(table_spec.foreign_keys) if table_spec.foreign_keys else 'n/a'}`",
                "",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_data_dictionary(reports_dir: Path) -> Path:
    path = reports_dir / "data_dictionary.md"
    lines = ["# Data Dictionary", ""]
    for table_spec in TABLE_SPECS:
        if not table_spec.fields:
            continue
        lines.extend(
            [
                f"## {table_spec.name}",
                "",
                table_spec.description,
                "",
                "| Field | Type | Nullable | Description |",
                "| --- | --- | --- | --- |",
            ]
        )
        for field in table_spec.fields:
            lines.append(
                f"| {field.name} | {field.dtype} | {'yes' if field.nullable else 'no'} | {field.description} |"
            )
        lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_architecture_notes(reports_dir: Path) -> Path:
    path = reports_dir / "architecture_notes.md"
    lines = [
        "# Canonical Data Architecture",
        "",
        "## Why this is not one flat table",
        "",
        "- Scenario data, reusable week files, and rolling history states live at different semantic levels and change at different rates.",
        "- SchedulingBenchmarks adds a second static family whose contracts, fixed assignments, and weighted cover semantics do not fit losslessly into INRC-II week/history tables.",
        "- A single flattened table would duplicate scenario rules across every assignment row, obscure week reuse, and make history transitions hard to audit.",
        "- The normalized design keeps each entity close to its source semantics and makes hard/soft constraint checks explainable by direct joins.",
        "",
        "## Layer interaction",
        "",
        "```mermaid",
        'flowchart TD',
        '    A["Raw source files"] --> B["raw_document"]',
        '    B --> C["scenario / contract / nurse / shift_type / skill"]',
        '    B --> D["week / day / coverage_requirement / nurse_request"]',
        '    B --> E["history_snapshot / nurse_history_state"]',
        '    B --> F["sb_case / sb_day / sb_employee / sb_cover_requirement"]',
        '    C --> G["instance / instance_week_map"]',
        '    D --> G',
        '    E --> G',
        '    G --> H["assignment"]',
        '    E --> I["history_transition_view"]',
        '    H --> J["roster_day_view"]',
        '    H --> K["coverage_summary_view"]',
        '    H --> L["nurse_week_summary_view"]',
        "```",
        "",
        "## Static and multi-stage compatibility",
        "",
        "- Static experiments can consume either an INRC-II weekly projection or a SchedulingBenchmarks case through the shared `StaticInstanceBundle` interface.",
        "- Multi-stage experiments still use `instance`, `instance_week_map`, and instance-specific history snapshots to preserve week order, week reuse, and cross-week fairness/workload state.",
        "",
        "## Explainability benefits",
        "",
        "- Every canonical row keeps a trace back to its source file or source instance folder.",
        "- Derived views use simple joins and aggregations, which makes coverage gaps, request violations, and history transitions inspectable without hidden preprocessing.",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_validation_report(reports_dir: Path, validation_report: ValidationReport) -> Path:
    path = reports_dir / "validation_report.md"
    lines = [
        "# Validation Report",
        "",
        f"- Errors: {validation_report.error_count}",
        f"- Warnings: {validation_report.warning_count}",
        "",
        "## Checks",
        "",
        "| Check | Passed | Count | Details |",
        "| --- | --- | ---: | --- |",
    ]
    for check in validation_report.checks:
        lines.append(
            f"| {check['name']} | {'yes' if check['passed'] else 'no'} | {check.get('count', '')} | {check['details']} |"
        )
    lines.extend(["", "## Issues", ""])
    if not validation_report.issues:
        lines.append("- No issues detected.")
    else:
        for issue in validation_report.issues:
            lines.append(f"- `{issue.level}:{issue.code}`: {issue.message}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _rows_to_markdown(rows: list[sqlite3.Row]) -> list[str]:
    if not rows:
        return ["_No rows available._"]
    columns = rows[0].keys()
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(row[column]) for column in columns) + " |")
    return lines


def _write_example_outputs(reports_dir: Path, conn: sqlite3.Connection) -> Path:
    path = reports_dir / "example_outputs.md"
    sections = {
        "scenario": "SELECT scenario_id, dataset_name, planning_horizon_weeks, scenario_folder_code FROM scenario ORDER BY scenario_id LIMIT 5",
        "nurse": "SELECT nurse_id, scenario_id, nurse_code, contract_id FROM nurse ORDER BY scenario_id, nurse_code LIMIT 8",
        "contract": "SELECT contract_id, scenario_id, contract_code, min_total_assignments, max_total_assignments FROM contract ORDER BY scenario_id, contract_code LIMIT 8",
        "shift_type": "SELECT shift_type_id, scenario_id, shift_code, min_consecutive_shift_assignments, max_consecutive_shift_assignments FROM shift_type ORDER BY scenario_id, shift_code LIMIT 8",
        "week_requirements": """
            SELECT week_id, day_id, shift_type_id, skill_id, min_required, optimal_required
            FROM coverage_requirement
            ORDER BY week_id, day_id, shift_type_id, skill_id
            LIMIT 10
        """,
        "nurse_requests": """
            SELECT request_id, week_id, nurse_id, day_id, requested_off_shift_type_id, request_type
            FROM nurse_request
            ORDER BY week_id, nurse_id, day_id
            LIMIT 10
        """,
        "history_state": """
            SELECT hs.history_id, hs.snapshot_type, nhs.nurse_id, nhs.last_shift_type_id,
                   nhs.consecutive_same_shift_count, nhs.total_worked_shifts_so_far
            FROM history_snapshot AS hs
            JOIN nurse_history_state AS nhs ON nhs.history_id = hs.history_id
            ORDER BY hs.history_id, nhs.nurse_id
            LIMIT 10
        """,
        "sb_case": """
            SELECT case_code, start_date, end_date, horizon_days, global_min_rest_minutes
            FROM sb_case
            ORDER BY case_code
            LIMIT 5
        """,
        "sb_employee": """
            SELECT e.employee_code, c.contract_code
            FROM sb_employee AS e
            JOIN sb_contract AS c ON c.sb_contract_id = e.sb_contract_id
            ORDER BY e.employee_code
            LIMIT 10
        """,
        "sb_shift_type": """
            SELECT shift_code, duration_minutes, start_time, end_time
            FROM sb_shift_type
            ORDER BY shift_code
            LIMIT 10
        """,
        "sb_cover_requirement": """
            SELECT sb_case_id, sb_day_id, sb_shift_type_id, min_required, preferred_required, under_weight, over_weight
            FROM sb_cover_requirement
            ORDER BY sb_case_id, sb_day_id, sb_shift_type_id
            LIMIT 10
        """,
        "sb_request": """
            SELECT sb_case_id, sb_day_id, sb_employee_id, sb_shift_type_id, request_type, weight
            FROM sb_request
            ORDER BY sb_case_id, sb_employee_id, sb_day_id
            LIMIT 10
        """,
        "coverage_summary_view": """
            SELECT instance_id, stage_index, day_name, shift_code, skill_code, min_required, optimal_required, assigned_count, min_gap, optimal_gap
            FROM coverage_summary_view
            ORDER BY instance_id, stage_index, day_name, shift_code, skill_code
            LIMIT 10
        """,
        "nurse_week_summary_view": """
            SELECT instance_id, stage_index, nurse_code, worked_days, days_off, weekends_worked, undesired_assignments, worked_days_minus_stage_mean
            FROM nurse_week_summary_view
            ORDER BY instance_id, stage_index, nurse_code
            LIMIT 10
        """,
        "history_transition_view": """
            SELECT instance_id, stage_index_completed, nurse_code, previous_last_shift_code, new_last_shift_code,
                   previous_consecutive_work_days_count, new_consecutive_work_days_count
            FROM history_transition_view
            ORDER BY instance_id, stage_index_completed, nurse_code
            LIMIT 10
        """,
    }
    lines = ["# Example Outputs", ""]
    json_payload: dict[str, list[dict[str, Any]]] = {}
    for section_name, sql in sections.items():
        rows = list(conn.execute(sql).fetchall())
        lines.extend([f"## {section_name}", ""])
        lines.extend(_rows_to_markdown(rows))
        lines.append("")
        json_payload[section_name] = [dict(row) for row in rows]

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    (reports_dir / "example_outputs.json").write_text(json.dumps(json_payload, indent=2), encoding="utf-8")
    return path


def _write_project_structure_recommendation(reports_dir: Path) -> Path:
    path = reports_dir / "recommended_project_structure.md"
    lines = [
        "# Recommended Project Structure",
        "",
        "```text",
        "INRC-proj/",
        "  INRC-II_datasets _xml/",
        "  INRC-II_test datasets _xml/",
        "  SchedulingBenchmarks/",
        "  src/",
        "    inrc2_data/",
        "      discovery.py",
        "      parse.py",
        "      scheduling_benchmarks.py",
        "      database.py",
        "      validation.py",
        "      reporting.py",
        "      features.py",
        "      pipeline.py",
        "  scripts/",
        "    build_foundation.py",
        "  data/",
        "    processed/",
        "      inrc2_foundation.sqlite",
        "      feature_bundles/",
        "  reports/",
        "    dataset_inspection.md",
        "    schema_summary.md",
        "    data_dictionary.md",
        "    validation_report.md",
        "    example_outputs.md",
        "```",
        "",
        "- Keep the raw XML untouched in place.",
        "- Treat SQLite as the canonical relational layer consumed by solver modules.",
        "- Keep solver-specific tensors or index maps under `data/processed/feature_bundles/` so the canonical layer remains stable.",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_solver_integration_roadmap(reports_dir: Path) -> Path:
    path = reports_dir / "solver_integration_roadmap.md"
    lines = [
        "# Roadmap for Solver Integration",
        "",
        "1. Exact baseline on small weekly subproblems.",
        "   Use `week`, `coverage_requirement`, `nurse_request`, `forbidden_shift_succession`, and a chosen `history_snapshot` to build a CP-SAT or MIP formulation.",
        "2. Plain GA with the same canonical evaluator interface.",
        "   Encode one nurse-day decision per stage day and reuse the same hard/soft evaluation joins.",
        "3. Feasibility repair operator.",
        "   Build repairs around `coverage_summary_view`, nurse-skill eligibility, and shift-succession checks.",
        "4. Local search and memetic GA.",
        "   Add neighborhood moves over `roster_day_view` and use `nurse_week_summary_view` for focused improvement.",
        "5. Fairness and preference-aware objective extensions.",
        "   Extend penalties using `nurse_week_summary_view` and cumulative `history_transition_view` data.",
        "6. Hyper-heuristic controller.",
        "   Expose move outcomes and constraint deltas through `constraint_result` and `solver_run`.",
        "7. Matheuristic exact repair.",
        "   Use solver-friendly feature bundles to carve out local exact subproblems around violated regions.",
        "8. RL-guided operator selection.",
        "   Feed per-stage summaries, coverage gaps, and rolling history counters into the state representation.",
        "9. Multi-stage transfer.",
        "   Replace weekly reset logic with the instance-specific history snapshots and continue stage by stage through `instance_week_map`.",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
