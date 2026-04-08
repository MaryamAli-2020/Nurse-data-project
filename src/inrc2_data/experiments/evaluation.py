"""Static solution evaluation for INRC-II weekly projections and SchedulingBenchmarks cases."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any

from ..constants import (
    DATASET_FAMILY_INRC_II,
    DATASET_FAMILY_SCHEDULING_BENCHMARKS,
    REQUEST_TYPE_OFF_ANY,
    REQUEST_TYPE_OFF_SHIFT,
    REQUEST_TYPE_ON_SHIFT,
)

HARD_PENALTY_MULTIPLIER = 1_000_000.0

DEFAULT_INRC_WEIGHTS = {
    "optimal_coverage": 30.0,
    "request_off": 10.0,
    "consecutive_work_days": 15.0,
    "consecutive_days_off": 15.0,
    "consecutive_same_shift": 15.0,
    "complete_weekend": 30.0,
    "total_assignments": 20.0,
    "working_weekends": 30.0,
    "fairness_workload": 0.0,
}

DEFAULT_SB_WEIGHTS = {
    "request": 1.0,
    "total_minutes": 1.0,
    "consecutive_shifts": 30.0,
    "consecutive_days_off": 30.0,
    "working_weekends": 30.0,
}


@dataclass
class PreparedStaticBundle:
    raw_bundle: dict[str, Any]
    dataset_family: str
    problem_mode: str
    partial_horizon: bool
    days: list[dict[str, Any]]
    staff: list[dict[str, Any]]
    shifts: list[dict[str, Any]]
    skills: list[dict[str, Any]]
    contracts_by_code: dict[str, dict[str, Any]]
    day_lookup: dict[int, dict[str, Any]]
    staff_lookup: dict[int, dict[str, Any]]
    shift_lookup: dict[int, dict[str, Any]]
    skill_lookup: dict[int, dict[str, Any]]
    weekend_groups: list[list[int]]
    initial_history: dict[int, dict[str, Any]]
    eligibility_by_staff: dict[int, set[int]]
    forbidden_pairs: set[tuple[int, int]]
    request_rows_by_staff_day: dict[tuple[int, int], list[dict[str, Any]]]
    fixed_assignments_by_staff_day: dict[tuple[int, int], dict[str, Any]]
    coverage_rows: list[dict[str, Any]]
    employee_shift_limits: dict[tuple[int, int], int]
    objective_weights: dict[str, Any]


def prepare_static_bundle(
    bundle: dict[str, Any],
    *,
    treat_open_end_as_incomplete: bool | None = None,
) -> PreparedStaticBundle:
    day_lookup = {row["day_index"]: row for row in bundle["days"]}
    staff_lookup = {row["staff_index"]: row for row in bundle["staff"]}
    shift_lookup = {row["shift_index"]: row for row in bundle["shifts"]}
    skill_lookup = {row["skill_index"]: row for row in bundle.get("skills", [])}
    contracts_by_code = {
        row["contract_code"]: row
        for row in bundle.get("contracts", [])
    }
    partial_horizon = (
        bundle["dataset_family"] == DATASET_FAMILY_INRC_II
        if treat_open_end_as_incomplete is None
        else treat_open_end_as_incomplete
    )

    request_rows_by_staff_day: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for request_row in bundle["constraints"].get("request_rows", []):
        request_rows_by_staff_day[(request_row["staff_index"], request_row["day_index"])].append(request_row)

    fixed_assignments_by_staff_day = {
        (row["staff_index"], row["day_index"]): row
        for row in bundle["constraints"].get("fixed_assignments", [])
    }
    employee_shift_limits = {
        (row["staff_index"], row["shift_index"]): row["max_assignments"]
        for row in bundle["constraints"].get("employee_shift_limits", [])
    }
    eligibility_by_staff: dict[int, set[int]] = defaultdict(set)
    for edge in bundle["constraints"].get("eligibility_edges", []):
        eligibility_by_staff[edge["staff_index"]].add(edge["skill_index"])
    forbidden_pairs = {
        (row["prev_shift_index"], row["next_shift_index"])
        for row in bundle["constraints"].get("forbidden_successions", [])
        if row.get("is_forbidden", 1)
    }
    initial_history = {
        row["staff_index"]: row
        for row in bundle["constraints"].get("initial_history", [])
    }
    return PreparedStaticBundle(
        raw_bundle=bundle,
        dataset_family=bundle["dataset_family"],
        problem_mode=bundle["problem_mode"],
        partial_horizon=partial_horizon,
        days=bundle["days"],
        staff=bundle["staff"],
        shifts=bundle["shifts"],
        skills=bundle.get("skills", []),
        contracts_by_code=contracts_by_code,
        day_lookup=day_lookup,
        staff_lookup=staff_lookup,
        shift_lookup=shift_lookup,
        skill_lookup=skill_lookup,
        weekend_groups=_build_weekend_groups(bundle["days"]),
        initial_history=initial_history,
        eligibility_by_staff=eligibility_by_staff,
        forbidden_pairs=forbidden_pairs,
        request_rows_by_staff_day=request_rows_by_staff_day,
        fixed_assignments_by_staff_day=fixed_assignments_by_staff_day,
        coverage_rows=bundle["constraints"].get("coverage_rows", []),
        employee_shift_limits=employee_shift_limits,
        objective_weights=bundle.get("objective_weights", {}),
    )


def evaluate_static_solution(
    prepared: PreparedStaticBundle,
    assignments: list[dict[str, Any]],
    *,
    fairness_weight: float | None = None,
) -> dict[str, Any]:
    assignment_rows = list(assignments)
    assignment_map: dict[tuple[int, int], dict[str, Any]] = {}
    duplicate_counter: Counter[tuple[int, int]] = Counter()
    for row in assignment_rows:
        key = (row["staff_index"], row["day_index"])
        duplicate_counter[key] += 1
        assignment_map[key] = row

    constraint_results: list[dict[str, Any]] = []

    def add_result(code: str, count: int, penalty: float, is_hard: bool) -> None:
        if count <= 0 and penalty <= 0:
            return
        constraint_results.append(
            {
                "constraint_code": code,
                "violation_count": int(count),
                "penalty_cost": float(penalty),
                "is_hard": bool(is_hard),
                "evaluation_scope": "STATIC_INSTANCE",
            }
        )

    duplicate_count = sum(max(0, count - 1) for count in duplicate_counter.values())
    add_result("H_SINGLE_ASSIGNMENT_PER_DAY", duplicate_count, float(duplicate_count), True)

    if prepared.dataset_family == DATASET_FAMILY_INRC_II:
        _evaluate_inrc_static(prepared, assignment_map, add_result, fairness_weight)
    elif prepared.dataset_family == DATASET_FAMILY_SCHEDULING_BENCHMARKS:
        _evaluate_scheduling_static(prepared, assignment_map, add_result)
    else:
        raise ValueError(f"Unsupported dataset family: {prepared.dataset_family}")

    hard_violation_count = sum(row["violation_count"] for row in constraint_results if row["is_hard"])
    soft_penalty = sum(row["penalty_cost"] for row in constraint_results if not row["is_hard"])
    objective_value = hard_violation_count * HARD_PENALTY_MULTIPLIER + soft_penalty
    metrics = _build_metrics(prepared, assignment_map)
    return {
        "status": "FEASIBLE" if hard_violation_count == 0 else "INFEASIBLE",
        "hard_violation_count": hard_violation_count,
        "soft_penalty": float(soft_penalty),
        "objective_value": float(objective_value),
        "constraint_results": constraint_results,
        "metrics": metrics,
    }


def _evaluate_inrc_static(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    add_result: Any,
    fairness_weight: float | None,
) -> None:
    weights = dict(DEFAULT_INRC_WEIGHTS)
    weights.update({key: value for key, value in prepared.objective_weights.items() if isinstance(value, (int, float))})
    if fairness_weight is not None:
        weights["fairness_workload"] = fairness_weight

    skill_counts: Counter[tuple[int, int, int]] = Counter()
    shift_counts: Counter[tuple[int, int]] = Counter()
    invalid_skill_count = 0
    forbidden_count = 0

    for (staff_index, day_index), assignment in assignment_map.items():
        shift_index = assignment["shift_index"]
        skill_index = assignment.get("skill_index")
        if skill_index is None or skill_index not in prepared.eligibility_by_staff.get(staff_index, set()):
            invalid_skill_count += 1
        else:
            skill_counts[(day_index, shift_index, skill_index)] += 1
        shift_counts[(day_index, shift_index)] += 1

        previous_shift_index = _previous_shift_index(prepared, assignment_map, staff_index, day_index)
        if previous_shift_index is not None and (previous_shift_index, shift_index) in prepared.forbidden_pairs:
            forbidden_count += 1

    add_result("H_MISSING_REQUIRED_SKILL", invalid_skill_count, float(invalid_skill_count), True)
    add_result("H_FORBIDDEN_SUCCESSION", forbidden_count, float(forbidden_count), True)

    min_gap_total = 0
    optimal_gap_total = 0
    for coverage_row in prepared.coverage_rows:
        key = (
            coverage_row["day_index"],
            coverage_row["shift_index"],
            coverage_row["skill_index"],
        )
        assigned = skill_counts.get(key, 0)
        min_gap = max(0, coverage_row["min_required"] - assigned)
        optimal_gap = max(0, coverage_row["optimal_required"] - assigned)
        min_gap_total += min_gap
        optimal_gap_total += optimal_gap
    add_result("H_MIN_COVERAGE", min_gap_total, float(min_gap_total), True)
    add_result(
        "S_OPTIMAL_COVERAGE",
        optimal_gap_total,
        optimal_gap_total * weights["optimal_coverage"],
        False,
    )

    request_hits = 0
    for key, request_rows in prepared.request_rows_by_staff_day.items():
        assignment = assignment_map.get(key)
        for request_row in request_rows:
            if request_row["request_type"] == REQUEST_TYPE_OFF_ANY and assignment is not None:
                request_hits += 1
            elif (
                request_row["request_type"] == REQUEST_TYPE_OFF_SHIFT
                and assignment is not None
                and assignment["shift_index"] == request_row["shift_index"]
            ):
                request_hits += 1
    add_result("S_REQUEST_OFF", request_hits, request_hits * weights["request_off"], False)

    contract_penalties = _evaluate_inrc_contract_penalties(prepared, assignment_map, weights)
    for code, count, penalty in contract_penalties:
        add_result(code, count, penalty, False)


def _evaluate_inrc_contract_penalties(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    weights: dict[str, float],
) -> list[tuple[str, int, float]]:
    total_consecutive_work = 0
    total_consecutive_off = 0
    total_consecutive_shift = 0
    total_complete_weekend = 0
    total_assignments_over = 0
    total_working_weekends_over = 0
    workloads: list[int] = []

    for staff_row in prepared.staff:
        staff_index = staff_row["staff_index"]
        contract = prepared.contracts_by_code.get(staff_row["contract_code"], {})
        day_assignments = [
            assignment_map.get((staff_index, day["day_index"]))
            for day in sorted(prepared.days, key=lambda row: row["day_index"])
        ]
        shift_sequence = [None if row is None else row["shift_index"] for row in day_assignments]
        worked_flags = [shift is not None for shift in shift_sequence]
        workloads.append(sum(1 for flag in worked_flags if flag))

        total_consecutive_work += _evaluate_binary_runs(
            worked_flags,
            carry_in=(
                prepared.initial_history.get(staff_index, {}).get("consecutive_work_days_count", 0)
                if prepared.initial_history.get(staff_index, {}).get("last_shift_index") is not None
                else 0
            ),
            min_allowed=contract.get("min_consecutive_work_days"),
            max_allowed=contract.get("max_consecutive_work_days"),
            partial_horizon=prepared.partial_horizon,
        )
        total_consecutive_off += _evaluate_binary_runs(
            [not flag for flag in worked_flags],
            carry_in=(
                prepared.initial_history.get(staff_index, {}).get("consecutive_days_off_count", 0)
                if prepared.initial_history.get(staff_index, {}).get("last_shift_index") is None
                else 0
            ),
            min_allowed=contract.get("min_consecutive_days_off"),
            max_allowed=contract.get("max_consecutive_days_off"),
            partial_horizon=prepared.partial_horizon,
        )
        total_consecutive_shift += _evaluate_shift_runs(
            shift_sequence,
            carry_shift=prepared.initial_history.get(staff_index, {}).get("last_shift_index"),
            carry_count=prepared.initial_history.get(staff_index, {}).get("consecutive_same_shift_count", 0),
            shift_min_max={
                shift_row["shift_index"]: (
                    shift_row.get("min_consecutive_assignments"),
                    shift_row.get("max_consecutive_assignments"),
                )
                for shift_row in prepared.shifts
            },
            partial_horizon=prepared.partial_horizon,
        )

        if contract.get("complete_weekend_required"):
            total_complete_weekend += _count_incomplete_weekends(prepared, worked_flags)

        previous_total_assignments = prepared.initial_history.get(staff_index, {}).get("total_worked_shifts_so_far", 0)
        current_total_assignments = previous_total_assignments + workloads[-1]
        max_total_assignments = contract.get("max_total_assignments")
        if max_total_assignments is not None:
            total_assignments_over += max(0, current_total_assignments - max_total_assignments)

        previous_working_weekends = prepared.initial_history.get(staff_index, {}).get("total_working_weekends_so_far", 0)
        current_working_weekends = previous_working_weekends + _count_working_weekends(prepared, worked_flags)
        max_working_weekends = contract.get("max_working_weekends")
        if max_working_weekends is not None:
            total_working_weekends_over += max(0, current_working_weekends - max_working_weekends)

    penalties = [
        (
            "S_CONSECUTIVE_WORK_DAYS",
            total_consecutive_work,
            total_consecutive_work * weights["consecutive_work_days"],
        ),
        (
            "S_CONSECUTIVE_DAYS_OFF",
            total_consecutive_off,
            total_consecutive_off * weights["consecutive_days_off"],
        ),
        (
            "S_CONSECUTIVE_SAME_SHIFT",
            total_consecutive_shift,
            total_consecutive_shift * weights["consecutive_same_shift"],
        ),
        (
            "S_COMPLETE_WEEKEND",
            total_complete_weekend,
            total_complete_weekend * weights["complete_weekend"],
        ),
        (
            "S_TOTAL_ASSIGNMENTS_MAX",
            total_assignments_over,
            total_assignments_over * weights["total_assignments"],
        ),
        (
            "S_WORKING_WEEKENDS_MAX",
            total_working_weekends_over,
            total_working_weekends_over * weights["working_weekends"],
        ),
    ]

    fairness_weight = float(weights.get("fairness_workload", 0.0))
    if fairness_weight > 0 and workloads:
        mean_workload = sum(workloads) / len(workloads)
        fairness_gap = int(round(sum(abs(workload - mean_workload) for workload in workloads)))
        penalties.append(("S_FAIRNESS_WORKLOAD", fairness_gap, fairness_gap * fairness_weight))
    return penalties


def _evaluate_scheduling_static(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    add_result: Any,
) -> None:
    weights = dict(DEFAULT_SB_WEIGHTS)
    weights.update({key: value for key, value in prepared.objective_weights.items() if isinstance(value, (int, float))})

    forbidden_count = 0
    fixed_assignment_violations = 0
    invalid_shift_count = 0
    shift_limit_violations = 0
    cover_under = 0
    cover_over = 0
    cover_under_penalty = 0.0
    cover_over_penalty = 0.0
    request_penalty = 0.0
    request_hits = 0

    shift_counts: Counter[tuple[int, int]] = Counter()
    employee_shift_counts: Counter[tuple[int, int]] = Counter()

    for (staff_index, day_index), assignment in assignment_map.items():
        shift_index = assignment["shift_index"]
        shift_counts[(day_index, shift_index)] += 1
        employee_shift_counts[(staff_index, shift_index)] += 1

        previous_shift_index = _previous_shift_index(prepared, assignment_map, staff_index, day_index)
        if previous_shift_index is not None and (previous_shift_index, shift_index) in prepared.forbidden_pairs:
            forbidden_count += 1

        contract = prepared.contracts_by_code.get(prepared.staff_lookup[staff_index]["contract_code"], {})
        valid_shift_codes = contract.get("valid_shift_codes") or ""
        valid_shift_set = {token.strip() for token in valid_shift_codes.split(",") if token.strip()}
        if valid_shift_set:
            shift_code = prepared.shift_lookup[shift_index]["shift_code"]
            if shift_code not in valid_shift_set:
                invalid_shift_count += 1

    add_result("H_FORBIDDEN_SUCCESSION", forbidden_count, float(forbidden_count), True)
    add_result("H_INVALID_SHIFT_FOR_CONTRACT", invalid_shift_count, float(invalid_shift_count), True)

    for key, fixed_row in prepared.fixed_assignments_by_staff_day.items():
        assignment = assignment_map.get(key)
        if fixed_row.get("is_off"):
            if assignment is not None:
                fixed_assignment_violations += 1
        else:
            if assignment is None or assignment["shift_index"] != fixed_row["shift_index"]:
                fixed_assignment_violations += 1
    add_result("H_FIXED_ASSIGNMENT", fixed_assignment_violations, float(fixed_assignment_violations), True)

    for key, max_assignments in prepared.employee_shift_limits.items():
        assigned = employee_shift_counts.get(key, 0)
        if assigned > max_assignments:
            shift_limit_violations += assigned - max_assignments
    add_result("H_EMPLOYEE_SHIFT_LIMIT", shift_limit_violations, float(shift_limit_violations), True)

    for coverage_row in prepared.coverage_rows:
        key = (coverage_row["day_index"], coverage_row["shift_index"])
        assigned = shift_counts.get(key, 0)
        under_gap = max(0, coverage_row["min_required"] - assigned)
        preferred_gap = max(0, coverage_row["preferred_required"] - assigned)
        over_gap = max(0, assigned - coverage_row["preferred_required"])
        cover_under += under_gap
        cover_over += over_gap
        cover_under_penalty += preferred_gap * coverage_row["under_weight"]
        cover_over_penalty += over_gap * coverage_row["over_weight"]
    add_result("S_COVER_UNDER", cover_under, cover_under_penalty, False)
    add_result("S_COVER_OVER", cover_over, cover_over_penalty, False)

    for key, request_rows in prepared.request_rows_by_staff_day.items():
        assignment = assignment_map.get(key)
        for request_row in request_rows:
            if request_row["request_type"] == REQUEST_TYPE_ON_SHIFT:
                if assignment is None or assignment["shift_index"] != request_row["shift_index"]:
                    request_hits += 1
                    request_penalty += request_row["weight"] * weights["request"]
            elif request_row["request_type"] == REQUEST_TYPE_OFF_SHIFT:
                if assignment is not None and assignment["shift_index"] == request_row["shift_index"]:
                    request_hits += 1
                    request_penalty += request_row["weight"] * weights["request"]
    add_result("S_REQUEST_WEIGHTED", request_hits, request_penalty, False)

    contract_penalties = _evaluate_scheduling_contract_penalties(prepared, assignment_map, weights)
    for code, count, penalty in contract_penalties:
        add_result(code, count, penalty, False)


def _evaluate_scheduling_contract_penalties(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    weights: dict[str, float],
) -> list[tuple[str, int, float]]:
    total_minutes_under = 0
    total_minutes_over = 0
    total_consecutive_shift = 0
    total_consecutive_off = 0
    total_working_weekends_over = 0

    for staff_row in prepared.staff:
        staff_index = staff_row["staff_index"]
        contract = prepared.contracts_by_code.get(staff_row["contract_code"], {})
        day_assignments = [
            assignment_map.get((staff_index, day["day_index"]))
            for day in sorted(prepared.days, key=lambda row: row["day_index"])
        ]
        shift_sequence = [None if row is None else row["shift_index"] for row in day_assignments]
        worked_flags = [shift is not None for shift in shift_sequence]
        total_minutes = sum(
            prepared.shift_lookup[shift]["duration_minutes"]
            for shift in shift_sequence
            if shift is not None
        )

        min_total_minutes = contract.get("min_total_minutes")
        max_total_minutes = contract.get("max_total_minutes")
        if min_total_minutes is not None:
            total_minutes_under += max(0, min_total_minutes - total_minutes)
        if max_total_minutes is not None:
            total_minutes_over += max(0, total_minutes - max_total_minutes)

        total_consecutive_shift += _evaluate_binary_runs(
            worked_flags,
            carry_in=0,
            min_allowed=contract.get("min_consecutive_shifts"),
            max_allowed=contract.get("max_consecutive_shifts"),
            partial_horizon=False,
        )
        total_consecutive_off += _evaluate_binary_runs(
            [not flag for flag in worked_flags],
            carry_in=0,
            min_allowed=contract.get("min_consecutive_days_off"),
            max_allowed=None,
            partial_horizon=False,
        )

        max_working_weekends = contract.get("max_working_weekends")
        if max_working_weekends is not None:
            total_working_weekends_over += max(0, _count_working_weekends(prepared, worked_flags) - max_working_weekends)

    return [
        (
            "S_TOTAL_MINUTES_MIN",
            total_minutes_under,
            total_minutes_under * weights["total_minutes"],
        ),
        (
            "S_TOTAL_MINUTES_MAX",
            total_minutes_over,
            total_minutes_over * weights["total_minutes"],
        ),
        (
            "S_CONSECUTIVE_SHIFTS",
            total_consecutive_shift,
            total_consecutive_shift * weights["consecutive_shifts"],
        ),
        (
            "S_CONSECUTIVE_DAYS_OFF",
            total_consecutive_off,
            total_consecutive_off * weights["consecutive_days_off"],
        ),
        (
            "S_WORKING_WEEKENDS_MAX",
            total_working_weekends_over,
            total_working_weekends_over * weights["working_weekends"],
        ),
    ]


def _build_metrics(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
) -> dict[str, Any]:
    coverage_counts: Counter[tuple[int, int, int | None]] = Counter()
    assignment_count = 0
    for assignment in assignment_map.values():
        assignment_count += 1
        coverage_counts[(assignment["day_index"], assignment["shift_index"], assignment.get("skill_index"))] += 1
    return {
        "assignment_count": assignment_count,
        "staff_count": len(prepared.staff),
        "day_count": len(prepared.days),
        "shift_count": len(prepared.shifts),
    }


def _previous_shift_index(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    staff_index: int,
    day_index: int,
) -> int | None:
    if day_index == 0:
        history_row = prepared.initial_history.get(staff_index)
        if history_row is None:
            return None
        return history_row.get("last_shift_index")
    previous_assignment = assignment_map.get((staff_index, day_index - 1))
    if previous_assignment is None:
        return None
    return previous_assignment["shift_index"]


def _evaluate_binary_runs(
    sequence: list[bool],
    *,
    carry_in: int,
    min_allowed: int | None,
    max_allowed: int | None,
    partial_horizon: bool,
) -> int:
    violation_units = 0
    current_run = carry_in if carry_in > 0 else 0
    previous_active = carry_in > 0
    for index, active in enumerate(sequence):
        if active:
            if index == 0 and previous_active:
                current_run = carry_in + 1
            elif index > 0 and sequence[index - 1]:
                current_run += 1
            else:
                current_run = 1
        else:
            if index > 0 and sequence[index - 1]:
                violation_units += _run_violation(current_run, min_allowed, max_allowed, ended=True)
            current_run = 0
        if active:
            previous_active = True
        elif index > 0:
            previous_active = sequence[index - 1]
    if sequence and sequence[-1]:
        violation_units += _run_violation(
            current_run,
            min_allowed,
            max_allowed,
            ended=not partial_horizon,
        )
    return violation_units


def _evaluate_shift_runs(
    sequence: list[int | None],
    *,
    carry_shift: int | None,
    carry_count: int,
    shift_min_max: dict[int, tuple[int | None, int | None]],
    partial_horizon: bool,
) -> int:
    violation_units = 0
    current_shift = carry_shift
    current_count = carry_count if carry_shift is not None else 0
    for index, shift_index in enumerate(sequence):
        if shift_index is None:
            if index > 0 and sequence[index - 1] is not None and current_shift is not None:
                min_allowed, max_allowed = shift_min_max.get(current_shift, (None, None))
                violation_units += _run_violation(current_count, min_allowed, max_allowed, ended=True)
            current_shift = None
            current_count = 0
            continue
        if index == 0 and carry_shift is not None and shift_index == carry_shift:
            current_shift = shift_index
            current_count = carry_count + 1
        elif current_shift == shift_index and index > 0 and sequence[index - 1] is not None:
            current_count += 1
        else:
            if current_shift is not None and index > 0:
                min_allowed, max_allowed = shift_min_max.get(current_shift, (None, None))
                violation_units += _run_violation(current_count, min_allowed, max_allowed, ended=True)
            current_shift = shift_index
            current_count = 1
    if sequence and sequence[-1] is not None and current_shift is not None:
        min_allowed, max_allowed = shift_min_max.get(current_shift, (None, None))
        violation_units += _run_violation(
            current_count,
            min_allowed,
            max_allowed,
            ended=not partial_horizon,
        )
    return violation_units


def _run_violation(length: int, min_allowed: int | None, max_allowed: int | None, *, ended: bool) -> int:
    if length <= 0:
        return 0
    violation_units = 0
    if max_allowed is not None and length > max_allowed:
        violation_units += length - max_allowed
    if ended and min_allowed is not None and length < min_allowed:
        violation_units += min_allowed - length
    return violation_units


def _build_weekend_groups(days: list[dict[str, Any]]) -> list[list[int]]:
    by_group: dict[str, list[int]] = defaultdict(list)
    for day_row in sorted(days, key=lambda row: row["day_index"]):
        if not day_row.get("is_weekend"):
            continue
        calendar_date = day_row.get("calendar_date")
        if calendar_date:
            current_date = date.fromisoformat(calendar_date)
            iso_year, iso_week, _ = current_date.isocalendar()
            group_key = f"{iso_year}-W{iso_week}"
        else:
            group_key = f"group-{day_row['day_index'] // 7}"
        by_group[group_key].append(day_row["day_index"])
    return list(by_group.values())


def _count_working_weekends(prepared: PreparedStaticBundle, worked_flags: list[bool]) -> int:
    total = 0
    for group in prepared.weekend_groups:
        if any(0 <= day_index < len(worked_flags) and worked_flags[day_index] for day_index in group):
            total += 1
    return total


def _count_incomplete_weekends(prepared: PreparedStaticBundle, worked_flags: list[bool]) -> int:
    total = 0
    for group in prepared.weekend_groups:
        worked_days = sum(1 for day_index in group if 0 <= day_index < len(worked_flags) and worked_flags[day_index])
        if worked_days == 1:
            total += 1
    return total
