"""Source-to-canonical parsing logic for INRC-II and SchedulingBenchmarks."""

from __future__ import annotations

import copy
from collections import defaultdict
from typing import Any

from .constants import (
    ASSIGNMENT_SOURCE_REFERENCE,
    DATASET_FAMILY_INRC_II,
    DATASET_FAMILY_SCHEDULING_BENCHMARKS,
    DAY_BY_ANY_NAME,
    DAY_SPECS,
    PROBLEM_MODE_MULTISTAGE,
    PROBLEM_MODE_STATIC,
    REQUEST_TYPE_OFF_ANY,
    REQUEST_TYPE_OFF_SHIFT,
    SNAPSHOT_TYPE_DERIVED_POST_WEEK,
    SNAPSHOT_TYPE_INITIAL_SOURCE,
    SNAPSHOT_TYPE_INSTANCE_INITIAL,
)
from .discovery import SOLUTION_FILE_RE
from .models import DatasetRoot, DiscoveredInstanceDir, DiscoveredSourceFile, DiscoveredStaticCase
from .scheduling_benchmarks import parse_case
from .utils import as_posix_path, stable_id
from .xml_utils import child_text, parse_xml


TABLE_NAMES = (
    "raw_document",
    "dataset",
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
    "solver_run",
    "constraint_result",
    "ablation_result",
)


def initialize_tables() -> dict[str, list[dict[str, Any]]]:
    return {name: [] for name in TABLE_NAMES}


def _skill_id(scenario_id: str, skill_code: str) -> str:
    return stable_id("skill", scenario_id, skill_code)


def _contract_id(scenario_id: str, contract_code: str) -> str:
    return stable_id("contract", scenario_id, contract_code)


def _nurse_id(scenario_id: str, nurse_code: str) -> str:
    return stable_id("nurse", scenario_id, nurse_code)


def _shift_type_id(scenario_id: str, shift_code: str) -> str:
    return stable_id("shift_type", scenario_id, shift_code)


def _week_id(scenario_id: str, week_code: int) -> str:
    return stable_id("week", scenario_id, week_code)


def _day_id(week_id: str, day_index: int) -> str:
    return stable_id("day", week_id, day_index)


def _history_id(scenario_id: str, history_variant: int, namespace: str) -> str:
    return stable_id("history_snapshot", scenario_id, namespace, history_variant)


def _sb_case_id(case_code: str) -> str:
    return stable_id("sb_case", case_code)


def _sb_day_id(case_code: str, day_index: int) -> str:
    return stable_id("sb_day", case_code, day_index)


def _sb_shift_type_id(case_code: str, shift_code: str) -> str:
    return stable_id("sb_shift_type", case_code, shift_code)


def _sb_employee_id(case_code: str, employee_code: str) -> str:
    return stable_id("sb_employee", case_code, employee_code)


def build_canonical_tables(
    dataset_roots: list[DatasetRoot],
    source_files: list[DiscoveredSourceFile],
    instance_dirs: list[DiscoveredInstanceDir],
    static_cases: list[DiscoveredStaticCase],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    tables = initialize_tables()

    for dataset_root in dataset_roots:
        tables["dataset"].append(
            {
                "dataset_id": dataset_root.dataset_id,
                "dataset_code": dataset_root.dataset_code,
                "dataset_family": dataset_root.dataset_family,
                "problem_mode": dataset_root.problem_mode,
                "display_name": dataset_root.display_name,
                "root_path": as_posix_path(dataset_root.root_path),
                "scan_root_path": as_posix_path(dataset_root.scan_root_path),
                "is_test_dataset": int(dataset_root.is_test),
            }
        )

    for source_file in source_files:
        tables["raw_document"].append(
            {
                "raw_document_id": source_file.raw_document_id,
                "dataset_id": source_file.dataset_id,
                "dataset_code": source_file.dataset_code,
                "dataset_family": source_file.dataset_family,
                "problem_mode": source_file.problem_mode,
                "source_file": as_posix_path(source_file.source_file),
                "relative_path": source_file.relative_path,
                "source_format": source_file.source_format,
                "source_group_code": source_file.source_group_code,
                "preferred_canonical_source": int(source_file.preferred_canonical_source),
                "paired_raw_document_id": source_file.paired_raw_document_id,
                "scenario_folder_code": source_file.scenario_folder_code,
                "instance_dir_name": source_file.instance_dir_name,
                "file_role": source_file.file_role,
                "root_tag": source_file.root_tag,
                "scenario_id_in_xml": source_file.scenario_id_in_xml,
                "scenario_token_in_name": source_file.scenario_token_in_name,
                "file_code": source_file.file_code,
                "native_case_code": source_file.native_case_code,
                "content_sha256": source_file.content_sha256,
                "raw_payload": source_file.raw_payload,
            }
        )

    scenario_by_folder: dict[tuple[str, str], str] = {}
    history_lookup: dict[tuple[str, int], str] = {}
    week_lookup: dict[tuple[str, int], str] = {}
    planning_horizon_by_scenario: dict[str, int] = {}
    scheduling_case_by_instance: dict[str, str] = {}

    inrc_source_files = [file for file in source_files if file.dataset_family == DATASET_FAMILY_INRC_II]
    scenario_files = [file for file in inrc_source_files if file.file_role == "SCENARIO"]
    history_files = [file for file in inrc_source_files if file.file_role == "HISTORY"]
    week_files = [file for file in inrc_source_files if file.file_role == "WEEK"]
    solution_files = [file for file in inrc_source_files if file.file_role == "SOLUTION"]

    for xml_file in sorted(scenario_files, key=lambda item: item.relative_path):
        _parse_scenario_file(xml_file, tables, scenario_by_folder, planning_horizon_by_scenario)

    for xml_file in sorted(history_files, key=lambda item: item.relative_path):
        _parse_history_file(xml_file, tables, history_lookup)

    for xml_file in sorted(week_files, key=lambda item: item.relative_path):
        _parse_week_file(xml_file, tables, week_lookup)

    instance_lookup = _materialize_instances(
        tables=tables,
        instance_dirs=instance_dirs,
        scenario_by_folder=scenario_by_folder,
        history_lookup=history_lookup,
        week_lookup=week_lookup,
    )

    for xml_file in sorted(solution_files, key=lambda item: item.relative_path):
        _parse_solution_file(xml_file, tables, instance_lookup)

    _materialize_instance_history_snapshots(
        tables=tables,
        planning_horizon_by_scenario=planning_horizon_by_scenario,
    )

    _parse_scheduling_cases(tables=tables, source_files=source_files, static_cases=static_cases, scheduling_case_by_instance=scheduling_case_by_instance)

    metadata = {
        "scenario_by_folder": scenario_by_folder,
        "history_lookup": history_lookup,
        "week_lookup": week_lookup,
        "instance_lookup": instance_lookup,
        "planning_horizon_by_scenario": planning_horizon_by_scenario,
        "scheduling_case_by_instance": scheduling_case_by_instance,
    }
    return tables, metadata


def _parse_scenario_file(
    xml_file: DiscoveredSourceFile,
    tables: dict[str, list[dict[str, Any]]],
    scenario_by_folder: dict[tuple[str, str], str],
    planning_horizon_by_scenario: dict[str, int],
) -> None:
    root = parse_xml(xml_file.raw_payload)
    scenario_id = root.attrib["Id"]
    planning_horizon_weeks = int(child_text(root, "NumberOfWeeks"))
    planning_horizon_by_scenario[scenario_id] = planning_horizon_weeks
    if xml_file.scenario_folder_code:
        scenario_by_folder[(xml_file.dataset_id, xml_file.scenario_folder_code)] = scenario_id

    tables["scenario"].append(
        {
            "scenario_id": scenario_id,
            "dataset_id": xml_file.dataset_id,
            "dataset_name": xml_file.dataset_code,
            "planning_horizon_weeks": planning_horizon_weeks,
            "source_file": as_posix_path(xml_file.source_file),
            "scenario_folder_code": xml_file.scenario_folder_code,
        }
    )

    for skill_node in root.find("Skills").findall("Skill"):
        skill_code = (skill_node.text or "").strip()
        tables["skill"].append(
            {
                "skill_id": _skill_id(scenario_id, skill_code),
                "scenario_id": scenario_id,
                "skill_code": skill_code,
            }
        )

    shift_rows: list[dict[str, Any]] = []
    shift_codes: list[str] = []
    for shift_node in root.find("ShiftTypes").findall("ShiftType"):
        shift_code = shift_node.attrib["Id"]
        shift_codes.append(shift_code)
        consecutive_node = shift_node.find("NumberOfConsecutiveAssignments")
        shift_rows.append(
            {
                "shift_type_id": _shift_type_id(scenario_id, shift_code),
                "scenario_id": scenario_id,
                "shift_code": shift_code,
                "min_consecutive_shift_assignments": int(child_text(consecutive_node, "Minimum")),
                "max_consecutive_shift_assignments": int(child_text(consecutive_node, "Maximum")),
            }
        )
    tables["shift_type"].extend(shift_rows)

    forbidden_targets: dict[str, set[str]] = defaultdict(set)
    succession_root = root.find("ForbiddenShiftTypeSuccessions")
    for succession_node in succession_root.findall("ShiftTypeSuccession"):
        prev_code = child_text(succession_node, "PrecedingShiftType")
        succeeding_node = succession_node.find("SucceedingShiftTypes")
        if succeeding_node is not None:
            for next_node in succeeding_node.findall("ShiftType"):
                forbidden_targets[prev_code].add((next_node.text or "").strip())

    for prev_code in shift_codes:
        for next_code in shift_codes:
            tables["forbidden_shift_succession"].append(
                {
                    "scenario_id": scenario_id,
                    "prev_shift_type_id": _shift_type_id(scenario_id, prev_code),
                    "next_shift_type_id": _shift_type_id(scenario_id, next_code),
                    "is_forbidden": int(next_code in forbidden_targets.get(prev_code, set())),
                }
            )

    for contract_node in root.find("Contracts").findall("Contract"):
        contract_code = contract_node.attrib["Id"]
        assignment_node = contract_node.find("NumberOfAssignments")
        work_days_node = contract_node.find("ConsecutiveWorkingDays")
        days_off_node = contract_node.find("ConsecutiveDaysOff")
        tables["contract"].append(
            {
                "contract_id": _contract_id(scenario_id, contract_code),
                "scenario_id": scenario_id,
                "contract_code": contract_code,
                "min_total_assignments": int(child_text(assignment_node, "Minimum")),
                "max_total_assignments": int(child_text(assignment_node, "Maximum")),
                "min_consecutive_work_days": int(child_text(work_days_node, "Minimum")),
                "max_consecutive_work_days": int(child_text(work_days_node, "Maximum")),
                "min_consecutive_days_off": int(child_text(days_off_node, "Minimum")),
                "max_consecutive_days_off": int(child_text(days_off_node, "Maximum")),
                "max_working_weekends": int(child_text(contract_node, "MaximumNumberOfWorkingWeekends")),
                "complete_weekend_required": int(child_text(contract_node, "CompleteWeekends")),
            }
        )

    for nurse_node in root.find("Nurses").findall("Nurse"):
        nurse_code = nurse_node.attrib["Id"]
        contract_code = child_text(nurse_node, "Contract")
        nurse_id = _nurse_id(scenario_id, nurse_code)
        tables["nurse"].append(
            {
                "nurse_id": nurse_id,
                "scenario_id": scenario_id,
                "nurse_code": nurse_code,
                "contract_id": _contract_id(scenario_id, contract_code),
            }
        )
        skills_node = nurse_node.find("Skills")
        for skill_node in skills_node.findall("Skill"):
            skill_code = (skill_node.text or "").strip()
            tables["nurse_skill"].append(
                {
                    "nurse_id": nurse_id,
                    "skill_id": _skill_id(scenario_id, skill_code),
                }
            )


def _parse_history_file(
    xml_file: DiscoveredSourceFile,
    tables: dict[str, list[dict[str, Any]]],
    history_lookup: dict[tuple[str, int], str],
) -> None:
    root = parse_xml(xml_file.raw_payload)
    scenario_id = child_text(root, "Scenario")
    history_variant = int(xml_file.file_code if xml_file.file_code is not None else 0)
    history_id = _history_id(scenario_id, history_variant, "source")
    history_lookup[(scenario_id, history_variant)] = history_id

    tables["history_snapshot"].append(
        {
            "history_id": history_id,
            "scenario_id": scenario_id,
            "instance_id": None,
            "week_index_before_solve": int(child_text(root, "Week")),
            "source_file": as_posix_path(xml_file.source_file),
            "snapshot_type": SNAPSHOT_TYPE_INITIAL_SOURCE,
            "history_variant_code": str(history_variant),
        }
    )

    nurse_history_root = root.find("NursesHistory")
    for nurse_history in nurse_history_root.findall("NurseHistory"):
        nurse_code = child_text(nurse_history, "Nurse")
        last_shift = child_text(nurse_history, "LastAssignedShiftType")
        last_shift_type_id = None if last_shift == "None" else _shift_type_id(scenario_id, last_shift)
        tables["nurse_history_state"].append(
            {
                "history_id": history_id,
                "nurse_id": _nurse_id(scenario_id, nurse_code),
                "last_shift_type_id": last_shift_type_id,
                "consecutive_same_shift_count": int(child_text(nurse_history, "NumberOfConsecutiveAssignments")),
                "consecutive_work_days_count": int(child_text(nurse_history, "NumberOfConsecutiveWorkingDays")),
                "consecutive_days_off_count": int(child_text(nurse_history, "NumberOfConsecutiveDaysOff")),
                "total_worked_shifts_so_far": int(child_text(nurse_history, "NumberOfAssignments")),
                "total_working_weekends_so_far": int(child_text(nurse_history, "NumberOfWorkingWeekends")),
            }
        )


def _parse_week_file(
    xml_file: DiscoveredSourceFile,
    tables: dict[str, list[dict[str, Any]]],
    week_lookup: dict[tuple[str, int], str],
) -> None:
    root = parse_xml(xml_file.raw_payload)
    scenario_id = child_text(root, "Scenario")
    if xml_file.file_code is None:
        raise ValueError(f"Week XML {xml_file.source_file} is missing a filename-derived week code.")
    week_code = int(xml_file.file_code)
    week_id = _week_id(scenario_id, week_code)
    week_lookup[(scenario_id, week_code)] = week_id
    tables["week"].append(
        {
            "week_id": week_id,
            "scenario_id": scenario_id,
            "week_index": week_code,
            "week_code": str(week_code),
            "source_file": as_posix_path(xml_file.source_file),
        }
    )

    for day_spec in DAY_SPECS:
        tables["day"].append(
            {
                "day_id": _day_id(week_id, day_spec.day_index),
                "week_id": week_id,
                "day_index": day_spec.day_index,
                "day_name": day_spec.long_name,
                "day_short_name": day_spec.short_name,
                "is_weekend": int(day_spec.is_weekend),
            }
        )

    for requirement_node in root.find("Requirements").findall("Requirement"):
        shift_code = child_text(requirement_node, "ShiftType")
        skill_code = child_text(requirement_node, "Skill")
        for day_spec in DAY_SPECS:
            day_node = requirement_node.find(day_spec.requirement_tag)
            if day_node is None:
                raise ValueError(
                    f"Requirement {shift_code}/{skill_code} in {xml_file.source_file} "
                    f"is missing {day_spec.requirement_tag}."
                )
            tables["coverage_requirement"].append(
                {
                    "requirement_id": stable_id("coverage_requirement", week_id, day_spec.day_index, shift_code, skill_code),
                    "week_id": week_id,
                    "day_id": _day_id(week_id, day_spec.day_index),
                    "shift_type_id": _shift_type_id(scenario_id, shift_code),
                    "skill_id": _skill_id(scenario_id, skill_code),
                    "min_required": int(child_text(day_node, "Minimum")),
                    "optimal_required": int(child_text(day_node, "Optimal")),
                }
            )

    shift_off_root = root.find("ShiftOffRequests")
    if shift_off_root is None:
        return
    for ordinal, request_node in enumerate(shift_off_root.findall("ShiftOffRequest"), start=1):
        nurse_code = child_text(request_node, "Nurse")
        day_name = child_text(request_node, "Day")
        shift_code = child_text(request_node, "ShiftType")
        day_spec = DAY_BY_ANY_NAME[day_name]
        request_type = REQUEST_TYPE_OFF_ANY if shift_code == "Any" else REQUEST_TYPE_OFF_SHIFT
        tables["nurse_request"].append(
            {
                "request_id": stable_id("nurse_request", week_id, ordinal),
                "week_id": week_id,
                "nurse_id": _nurse_id(scenario_id, nurse_code),
                "day_id": _day_id(week_id, day_spec.day_index),
                "requested_off_shift_type_id": (
                    None if request_type == REQUEST_TYPE_OFF_ANY else _shift_type_id(scenario_id, shift_code)
                ),
                "request_type": request_type,
            }
        )


def _materialize_instances(
    tables: dict[str, list[dict[str, Any]]],
    instance_dirs: list[DiscoveredInstanceDir],
    scenario_by_folder: dict[tuple[str, str], str],
    history_lookup: dict[tuple[str, int], str],
    week_lookup: dict[tuple[str, int], str],
) -> dict[tuple[str, str, str], str]:
    instance_lookup: dict[tuple[str, str, str], str] = {}
    for instance_dir in sorted(instance_dirs, key=lambda item: (item.dataset_code, item.scenario_folder_code, item.instance_dir_name)):
        scenario_id = scenario_by_folder[(instance_dir.dataset_id, instance_dir.scenario_folder_code)]
        initial_history_id = history_lookup[(scenario_id, instance_dir.initial_history_variant)]
        instance_id = stable_id("instance", instance_dir.dataset_code, scenario_id, instance_dir.instance_dir_name)
        instance_lookup[(instance_dir.dataset_id, instance_dir.scenario_folder_code, instance_dir.instance_dir_name)] = instance_id
        tables["instance"].append(
            {
                "instance_id": instance_id,
                "dataset_id": instance_dir.dataset_id,
                "dataset_family": instance_dir.dataset_family,
                "problem_mode": PROBLEM_MODE_MULTISTAGE,
                "scenario_id": scenario_id,
                "initial_history_id": initial_history_id,
                "num_weeks": len(instance_dir.week_sequence),
                "instance_code": f"{scenario_id}|H{instance_dir.initial_history_variant}|WD_{'-'.join(str(code) for code in instance_dir.week_sequence)}",
                "native_case_code": None,
                "instance_dir_name": instance_dir.instance_dir_name,
            }
        )
        for stage_index, week_code in enumerate(instance_dir.week_sequence):
            tables["instance_week_map"].append(
                {
                    "instance_id": instance_id,
                    "stage_index": stage_index,
                    "week_id": week_lookup[(scenario_id, week_code)],
                }
            )
    return instance_lookup


def _parse_solution_file(
    xml_file: DiscoveredSourceFile,
    tables: dict[str, list[dict[str, Any]]],
    instance_lookup: dict[tuple[str, str, str], str],
) -> None:
    if not xml_file.instance_dir_name or not xml_file.scenario_folder_code:
        raise ValueError(f"Solution XML {xml_file.source_file} is not nested under a discovered instance directory.")
    instance_key = (xml_file.dataset_id, xml_file.scenario_folder_code, xml_file.instance_dir_name)
    instance_id = instance_lookup[instance_key]
    root = parse_xml(xml_file.raw_payload)
    scenario_id = child_text(root, "Scenario")
    stage_index = int(child_text(root, "Week"))
    file_name_match = SOLUTION_FILE_RE.match(xml_file.source_file.name)
    if not file_name_match:
        raise ValueError(f"Could not parse solution filename metadata from {xml_file.source_file.name}.")
    week_code = int(file_name_match.group("week_code"))
    week_id = _week_id(scenario_id, week_code)
    assignments_root = root.find("Assignments")
    for ordinal, assignment_node in enumerate(assignments_root.findall("Assignment"), start=1):
        nurse_code = child_text(assignment_node, "Nurse")
        day_name = child_text(assignment_node, "Day")
        shift_code = child_text(assignment_node, "ShiftType")
        skill_code = child_text(assignment_node, "Skill")
        day_spec = DAY_BY_ANY_NAME[day_name]
        tables["assignment"].append(
            {
                "assignment_id": stable_id("assignment", instance_id, stage_index, ordinal),
                "instance_id": instance_id,
                "stage_index": stage_index,
                "week_id": week_id,
                "day_id": _day_id(week_id, day_spec.day_index),
                "nurse_id": _nurse_id(scenario_id, nurse_code),
                "shift_type_id": _shift_type_id(scenario_id, shift_code),
                "skill_id": _skill_id(scenario_id, skill_code),
                "solver_run_id": None,
                "source_file": as_posix_path(xml_file.source_file),
                "source_kind": ASSIGNMENT_SOURCE_REFERENCE,
            }
        )


def _materialize_instance_history_snapshots(
    tables: dict[str, list[dict[str, Any]]],
    planning_horizon_by_scenario: dict[str, int],
) -> None:
    scenario_by_instance = {
        row["instance_id"]: row["scenario_id"]
        for row in tables["instance"]
        if row["dataset_family"] == DATASET_FAMILY_INRC_II and row["scenario_id"] is not None
    }
    initial_history_by_instance = {
        row["instance_id"]: row["initial_history_id"]
        for row in tables["instance"]
        if row["dataset_family"] == DATASET_FAMILY_INRC_II and row["initial_history_id"] is not None
    }
    history_rows_by_id = {row["history_id"]: row for row in tables["history_snapshot"]}
    state_rows_by_history: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in tables["nurse_history_state"]:
        state_rows_by_history[row["history_id"]][row["nurse_id"]] = row

    nurse_ids_by_scenario: dict[str, list[str]] = defaultdict(list)
    for row in tables["nurse"]:
        nurse_ids_by_scenario[row["scenario_id"]].append(row["nurse_id"])

    shift_code_by_id = {row["shift_type_id"]: row["shift_code"] for row in tables["shift_type"]}
    shift_id_by_code_by_scenario: dict[str, dict[str, str]] = defaultdict(dict)
    for row in tables["shift_type"]:
        shift_id_by_code_by_scenario[row["scenario_id"]][row["shift_code"]] = row["shift_type_id"]

    assignments_by_instance_stage: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in tables["assignment"]:
        assignments_by_instance_stage[(row["instance_id"], row["stage_index"])].append(row)

    for instance_row in tables["instance"]:
        if instance_row["dataset_family"] != DATASET_FAMILY_INRC_II:
            continue
        instance_id = instance_row["instance_id"]
        scenario_id = scenario_by_instance[instance_id]
        initial_history_id = initial_history_by_instance[instance_id]
        source_history_row = history_rows_by_id[initial_history_id]
        instance_initial_history_id = stable_id("history_snapshot", instance_id, "before", 0)
        tables["history_snapshot"].append(
            {
                "history_id": instance_initial_history_id,
                "scenario_id": scenario_id,
                "instance_id": instance_id,
                "week_index_before_solve": 0,
                "source_file": source_history_row["source_file"],
                "snapshot_type": SNAPSHOT_TYPE_INSTANCE_INITIAL,
                "history_variant_code": source_history_row["history_variant_code"],
            }
        )
        current_states: dict[str, dict[str, Any]] = {}
        for nurse_id in nurse_ids_by_scenario[scenario_id]:
            source_state = copy.deepcopy(state_rows_by_history[initial_history_id][nurse_id])
            source_state["history_id"] = instance_initial_history_id
            tables["nurse_history_state"].append(source_state)
            current_states[nurse_id] = source_state

        if not any(key[0] == instance_id for key in assignments_by_instance_stage):
            continue

        horizon = planning_horizon_by_scenario[scenario_id]
        stage_limit = min(horizon, instance_row["num_weeks"])
        for stage_index in range(stage_limit):
            stage_rows = assignments_by_instance_stage.get((instance_id, stage_index))
            if not stage_rows:
                break
            nurse_day_shift: dict[str, dict[int, str | None]] = {
                nurse_id: {day_spec.day_index: None for day_spec in DAY_SPECS}
                for nurse_id in nurse_ids_by_scenario[scenario_id]
            }
            for row in stage_rows:
                day_index = int(str(row["day_id"]).rsplit("::", 1)[-1])
                nurse_day_shift[row["nurse_id"]][day_index] = shift_code_by_id[row["shift_type_id"]]

            next_history_id = stable_id("history_snapshot", instance_id, "before", stage_index + 1)
            tables["history_snapshot"].append(
                {
                    "history_id": next_history_id,
                    "scenario_id": scenario_id,
                    "instance_id": instance_id,
                    "week_index_before_solve": stage_index + 1,
                    "source_file": None,
                    "snapshot_type": SNAPSHOT_TYPE_DERIVED_POST_WEEK,
                    "history_variant_code": None,
                }
            )
            next_states: dict[str, dict[str, Any]] = {}
            for nurse_id in nurse_ids_by_scenario[scenario_id]:
                next_state = _apply_week_history_transition(
                    previous_state=current_states[nurse_id],
                    nurse_week_assignments=nurse_day_shift[nurse_id],
                    shift_id_by_code=shift_id_by_code_by_scenario[scenario_id],
                    next_history_id=next_history_id,
                )
                tables["nurse_history_state"].append(next_state)
                next_states[nurse_id] = next_state
            current_states = next_states


def _parse_scheduling_cases(
    *,
    tables: dict[str, list[dict[str, Any]]],
    source_files: list[DiscoveredSourceFile],
    static_cases: list[DiscoveredStaticCase],
    scheduling_case_by_instance: dict[str, str],
) -> None:
    source_by_case_and_format: dict[str, dict[str, DiscoveredSourceFile]] = defaultdict(dict)
    for source_file in source_files:
        if source_file.dataset_family != DATASET_FAMILY_SCHEDULING_BENCHMARKS or source_file.native_case_code is None:
            continue
        source_by_case_and_format[source_file.native_case_code][source_file.source_format] = source_file

    for static_case in sorted(static_cases, key=lambda item: item.case_code):
        members = source_by_case_and_format[static_case.case_code]
        ros_source = members["xml"]
        txt_source = members["text"]
        parsed_case = parse_case(
            case_code=static_case.case_code,
            ros_payload=ros_source.raw_payload,
            txt_payload=txt_source.raw_payload,
        )
        instance_id = stable_id("instance", ros_source.dataset_code, static_case.case_code)
        scheduling_case_by_instance[instance_id] = static_case.case_code
        sb_case_id = _sb_case_id(static_case.case_code)

        tables["instance"].append(
            {
                "instance_id": instance_id,
                "dataset_id": ros_source.dataset_id,
                "dataset_family": ros_source.dataset_family,
                "problem_mode": PROBLEM_MODE_STATIC,
                "scenario_id": None,
                "initial_history_id": None,
                "num_weeks": 1,
                "instance_code": static_case.case_code,
                "native_case_code": static_case.case_code,
                "instance_dir_name": None,
            }
        )
        tables["sb_case"].append(
            {
                "sb_case_id": sb_case_id,
                "dataset_id": ros_source.dataset_id,
                "instance_id": instance_id,
                "case_code": static_case.case_code,
                "source_ros_file": as_posix_path(ros_source.source_file),
                "source_txt_file": as_posix_path(txt_source.source_file),
                "start_date": parsed_case["start_date"],
                "end_date": parsed_case["end_date"],
                "horizon_days": parsed_case["horizon_days"],
                "global_min_rest_minutes": parsed_case["global_min_rest_minutes"],
            }
        )

        for day_row in parsed_case["day_rows"]:
            tables["sb_day"].append(
                {
                    "sb_day_id": _sb_day_id(static_case.case_code, day_row["day_index"]),
                    "sb_case_id": sb_case_id,
                    **day_row,
                }
            )

        for shift_code, definition in sorted(parsed_case["shift_definitions"].items()):
            tables["sb_shift_type"].append(
                {
                    "sb_shift_type_id": _sb_shift_type_id(static_case.case_code, shift_code),
                    "sb_case_id": sb_case_id,
                    "shift_code": shift_code,
                    "duration_minutes": definition["duration_minutes"],
                    "start_time": definition["start_time"],
                    "end_time": definition["end_time"],
                    "color": definition["color"],
                }
            )

        for contract_code, definition in sorted(parsed_case["contract_definitions"].items()):
            tables["sb_contract"].append(
                {
                    "sb_contract_id": definition["stable_contract_id"],
                    "sb_case_id": sb_case_id,
                    "contract_code": contract_code,
                    "min_rest_minutes": definition["min_rest_minutes"],
                    "min_total_minutes": definition["min_total_minutes"],
                    "max_total_minutes": definition["max_total_minutes"],
                    "min_consecutive_shifts": definition["min_consecutive_shifts"],
                    "max_consecutive_shifts": definition["max_consecutive_shifts"],
                    "min_consecutive_days_off": definition["min_consecutive_days_off"],
                    "max_working_weekends": definition["max_working_weekends"],
                    "valid_shift_codes": ",".join(definition["valid_shift_codes"]),
                    "is_meta_contract": int(definition["is_meta_contract"]),
                }
            )

        for employee_row in parsed_case["employee_rows"]:
            employee_code = employee_row["employee_code"]
            contract_code = employee_row["contract_code"]
            tables["sb_employee"].append(
                {
                    "sb_employee_id": _sb_employee_id(static_case.case_code, employee_code),
                    "sb_case_id": sb_case_id,
                    "employee_code": employee_code,
                    "sb_contract_id": parsed_case["contract_definitions"][contract_code]["stable_contract_id"],
                }
            )

        for limit_row in parsed_case["shift_limit_rows"]:
            tables["sb_employee_shift_limit"].append(
                {
                    "sb_employee_id": _sb_employee_id(static_case.case_code, limit_row["employee_code"]),
                    "sb_shift_type_id": _sb_shift_type_id(static_case.case_code, limit_row["shift_code"]),
                    "max_assignments": limit_row["max_assignments"],
                }
            )

        for ordinal, fixed_row in enumerate(parsed_case["fixed_assignments"], start=1):
            shift_code = fixed_row["shift_code"]
            tables["sb_fixed_assignment"].append(
                {
                    "sb_fixed_assignment_id": stable_id("sb_fixed_assignment", static_case.case_code, ordinal),
                    "sb_case_id": sb_case_id,
                    "sb_day_id": _sb_day_id(static_case.case_code, fixed_row["day_index"]),
                    "sb_employee_id": _sb_employee_id(static_case.case_code, fixed_row["employee_code"]),
                    "sb_shift_type_id": None if shift_code == "-" else _sb_shift_type_id(static_case.case_code, shift_code),
                    "assignment_code": shift_code,
                    "is_off": int(shift_code == "-"),
                }
            )

        for ordinal, request_row in enumerate(parsed_case["request_rows"], start=1):
            tables["sb_request"].append(
                {
                    "sb_request_id": stable_id("sb_request", static_case.case_code, ordinal),
                    "sb_case_id": sb_case_id,
                    "sb_day_id": _sb_day_id(static_case.case_code, request_row["day_index"]),
                    "sb_employee_id": _sb_employee_id(static_case.case_code, request_row["employee_code"]),
                    "sb_shift_type_id": _sb_shift_type_id(static_case.case_code, request_row["shift_code"]),
                    "request_type": request_row["request_type"],
                    "weight": request_row["weight"],
                }
            )

        for ordinal, cover_row in enumerate(parsed_case["cover_rows"], start=1):
            tables["sb_cover_requirement"].append(
                {
                    "sb_cover_requirement_id": stable_id("sb_cover_requirement", static_case.case_code, ordinal),
                    "sb_case_id": sb_case_id,
                    "sb_day_id": _sb_day_id(static_case.case_code, cover_row["day_index"]),
                    "sb_shift_type_id": _sb_shift_type_id(static_case.case_code, cover_row["shift_code"]),
                    "min_required": cover_row["min_required"],
                    "preferred_required": cover_row["preferred_required"],
                    "under_weight": cover_row["under_weight"],
                    "over_weight": cover_row["over_weight"],
                }
            )


def _apply_week_history_transition(
    previous_state: dict[str, Any],
    nurse_week_assignments: dict[int, str | None],
    shift_id_by_code: dict[str, str],
    next_history_id: str,
) -> dict[str, Any]:
    ordered_assignments = [nurse_week_assignments[index] for index in range(len(DAY_SPECS))]
    worked_days = sum(1 for shift_code in ordered_assignments if shift_code is not None)
    worked_weekend = int(ordered_assignments[5] is not None or ordered_assignments[6] is not None)

    trailing_days_off = 0
    for shift_code in reversed(ordered_assignments):
        if shift_code is None:
            trailing_days_off += 1
        else:
            break

    if trailing_days_off == len(ordered_assignments):
        last_shift_type_id = None
        consecutive_same_shift_count = 0
        consecutive_work_days_count = 0
        consecutive_days_off_count = int(previous_state["consecutive_days_off_count"]) + len(ordered_assignments)
    elif trailing_days_off > 0:
        last_shift_type_id = None
        consecutive_same_shift_count = 0
        consecutive_work_days_count = 0
        consecutive_days_off_count = trailing_days_off
    else:
        trailing_shift = ordered_assignments[-1]
        assert trailing_shift is not None
        same_shift_run = 0
        for shift_code in reversed(ordered_assignments):
            if shift_code == trailing_shift:
                same_shift_run += 1
            else:
                break
        work_day_run = 0
        for shift_code in reversed(ordered_assignments):
            if shift_code is not None:
                work_day_run += 1
            else:
                break
        last_shift_type_id = shift_id_by_code[trailing_shift]
        consecutive_same_shift_count = same_shift_run
        consecutive_work_days_count = work_day_run
        consecutive_days_off_count = 0

    return {
        "history_id": next_history_id,
        "nurse_id": previous_state["nurse_id"],
        "last_shift_type_id": last_shift_type_id,
        "consecutive_same_shift_count": consecutive_same_shift_count,
        "consecutive_work_days_count": consecutive_work_days_count,
        "consecutive_days_off_count": consecutive_days_off_count,
        "total_worked_shifts_so_far": int(previous_state["total_worked_shifts_so_far"]) + worked_days,
        "total_working_weekends_so_far": int(previous_state["total_working_weekends_so_far"]) + worked_weekend,
    }
