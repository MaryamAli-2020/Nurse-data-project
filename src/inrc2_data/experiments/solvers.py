"""Constructive and population-based static solvers built on the shared bundle interface."""

from __future__ import annotations

import copy
import random
from dataclasses import dataclass
from typing import Any

from .evaluation import PreparedStaticBundle, evaluate_static_solution


@dataclass
class StaticSolverConfig:
    seed: int = 0
    candidate_pool_limit: int = 12
    fill_to_soft_target: bool = True
    population_size: int = 12
    generations: int = 18
    mutation_rate: float = 0.15
    tournament_size: int = 3
    elite_count: int = 2
    local_search_iterations: int = 0


def solve_static_bundle(
    prepared: PreparedStaticBundle,
    *,
    algorithm_name: str,
    variant_name: str,
    seed: int,
    config: StaticSolverConfig | None = None,
) -> dict[str, Any]:
    config = config or StaticSolverConfig(seed=seed)
    if algorithm_name == "greedy":
        return _solve_greedy(prepared, config)
    if algorithm_name == "ga":
        return _solve_genetic(prepared, config)
    raise ValueError(f"Unsupported static algorithm: {algorithm_name}")


def _solve_greedy(prepared: PreparedStaticBundle, config: StaticSolverConfig) -> dict[str, Any]:
    rng = random.Random(config.seed)
    assignment_map = _seed_assignment_map(prepared)
    assignment_map = _repair_min_coverage(prepared, assignment_map, rng, config.candidate_pool_limit)
    if config.fill_to_soft_target:
        assignment_map = _fill_soft_targets(prepared, assignment_map, rng, config.candidate_pool_limit)
    assignments = _map_to_assignment_rows(assignment_map)
    evaluation = evaluate_static_solution(prepared, assignments)
    return {"assignments": assignments, "evaluation": evaluation}


def _solve_genetic(prepared: PreparedStaticBundle, config: StaticSolverConfig) -> dict[str, Any]:
    rng = random.Random(config.seed)
    seed_solution = _solve_greedy(prepared, config)
    base_map = _rows_to_map(seed_solution["assignments"])
    population = [base_map]
    for index in range(max(0, config.population_size - 1)):
        candidate = _mutate_assignment_map(
            prepared,
            copy.deepcopy(base_map),
            rng,
            edit_count=1 + (index % 3),
        )
        candidate = _repair_min_coverage(prepared, candidate, rng, config.candidate_pool_limit)
        population.append(candidate)

    best_solution = seed_solution
    best_map = base_map
    best_score = seed_solution["evaluation"]["objective_value"]

    for _ in range(config.generations):
        scored_population = []
        for assignment_map in population:
            assignments = _map_to_assignment_rows(assignment_map)
            evaluation = evaluate_static_solution(prepared, assignments)
            scored_population.append((evaluation["objective_value"], assignment_map, evaluation))
            if evaluation["objective_value"] < best_score:
                best_score = evaluation["objective_value"]
                best_solution = {"assignments": assignments, "evaluation": evaluation}
                best_map = copy.deepcopy(assignment_map)
        scored_population.sort(key=lambda item: item[0])
        next_population = [copy.deepcopy(item[1]) for item in scored_population[: config.elite_count]]
        while len(next_population) < config.population_size:
            parent_a = _tournament_select(scored_population, rng, config.tournament_size)
            parent_b = _tournament_select(scored_population, rng, config.tournament_size)
            child = _crossover_assignment_maps(prepared, parent_a, parent_b, rng)
            child = _mutate_assignment_map(prepared, child, rng, edit_count=max(1, int(config.mutation_rate * len(prepared.days))))
            child = _repair_min_coverage(prepared, child, rng, config.candidate_pool_limit)
            if config.local_search_iterations > 0:
                child = _local_search(prepared, child, rng, config.local_search_iterations, config.candidate_pool_limit)
            next_population.append(child)
        population = next_population

    final_assignments = _map_to_assignment_rows(best_map)
    final_evaluation = evaluate_static_solution(prepared, final_assignments)
    return {"assignments": final_assignments, "evaluation": final_evaluation}


def _seed_assignment_map(prepared: PreparedStaticBundle) -> dict[tuple[int, int], dict[str, Any]]:
    assignment_map: dict[tuple[int, int], dict[str, Any]] = {}
    for fixed_row in prepared.fixed_assignments_by_staff_day.values():
        if fixed_row.get("is_off"):
            continue
        assignment_map[(fixed_row["staff_index"], fixed_row["day_index"])] = {
            "staff_index": fixed_row["staff_index"],
            "day_index": fixed_row["day_index"],
            "shift_index": fixed_row["shift_index"],
            "skill_index": fixed_row.get("skill_index"),
        }
    return assignment_map


def _repair_min_coverage(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    rng: random.Random,
    candidate_pool_limit: int,
) -> dict[tuple[int, int], dict[str, Any]]:
    assignment_map = _sanitize_assignment_map(prepared, assignment_map)
    target_rows = sorted(
        prepared.coverage_rows,
        key=lambda row: (
            -int(row.get("min_required", 0)),
            row["day_index"],
            row["shift_index"],
            row.get("skill_index", -1),
        ),
    )
    for target_row in target_rows:
        while _covered_count(assignment_map, target_row) < target_row["min_required"]:
            candidates = _rank_candidates_for_target(
                prepared,
                assignment_map,
                target_row,
                rng,
                candidate_pool_limit,
            )
            if not candidates:
                break
            best_candidate = min(candidates, key=lambda item: item[0])[1]
            assignment_map[(best_candidate["staff_index"], best_candidate["day_index"])] = best_candidate
    return assignment_map


def _fill_soft_targets(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    rng: random.Random,
    candidate_pool_limit: int,
) -> dict[tuple[int, int], dict[str, Any]]:
    for target_row in prepared.coverage_rows:
        soft_target = target_row.get("optimal_required", target_row.get("preferred_required"))
        if soft_target is None:
            continue
        while _covered_count(assignment_map, target_row) < soft_target:
            base_eval = evaluate_static_solution(prepared, _map_to_assignment_rows(assignment_map))
            candidates = _rank_candidates_for_target(
                prepared,
                assignment_map,
                target_row,
                rng,
                candidate_pool_limit,
            )
            if not candidates:
                break
            best_score, best_candidate = min(candidates, key=lambda item: item[0])
            if best_score[1] >= base_eval["objective_value"]:
                break
            assignment_map[(best_candidate["staff_index"], best_candidate["day_index"])] = best_candidate
    return assignment_map


def _rank_candidates_for_target(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    target_row: dict[str, Any],
    rng: random.Random,
    candidate_pool_limit: int,
) -> list[tuple[tuple[float, float, float, float], dict[str, Any]]]:
    heuristic_candidates: list[tuple[tuple[float, float, float], dict[str, Any]]] = []
    for staff_row in prepared.staff:
        staff_index = staff_row["staff_index"]
        day_index = target_row["day_index"]
        key = (staff_index, day_index)
        if key in assignment_map:
            continue
        fixed_row = prepared.fixed_assignments_by_staff_day.get(key)
        if fixed_row and fixed_row.get("is_off"):
            continue
        candidate = _candidate_from_target(prepared, staff_index, target_row)
        if candidate is None:
            continue
        workload = sum(1 for existing_key in assignment_map if existing_key[0] == staff_index)
        request_penalty = 0
        for request_row in prepared.request_rows_by_staff_day.get(key, []):
            if request_row["request_type"] == "OFF_ANY":
                request_penalty += 2
            elif request_row["request_type"] in {"OFF_SHIFT", "ON_SHIFT"}:
                request_penalty += 1
        heuristic_candidates.append(((workload, request_penalty, rng.random()), candidate))

    shortlist = [
        item[1]
        for item in sorted(heuristic_candidates, key=lambda item: item[0])[:candidate_pool_limit]
    ]
    scored_candidates: list[tuple[tuple[float, float, float, float], dict[str, Any]]] = []
    for candidate in shortlist:
        tentative_map = copy.deepcopy(assignment_map)
        tentative_map[(candidate["staff_index"], candidate["day_index"])] = candidate
        evaluation = evaluate_static_solution(prepared, _map_to_assignment_rows(tentative_map))
        score = (
            float(evaluation["hard_violation_count"]),
            float(evaluation["objective_value"]),
            float(sum(1 for key in tentative_map if key[0] == candidate["staff_index"])),
            rng.random(),
        )
        scored_candidates.append((score, candidate))
    return scored_candidates


def _candidate_from_target(
    prepared: PreparedStaticBundle,
    staff_index: int,
    target_row: dict[str, Any],
) -> dict[str, Any] | None:
    shift_index = target_row["shift_index"]
    skill_index = target_row.get("skill_index")
    if prepared.dataset_family == "INRC_II":
        if skill_index is None:
            return None
        if skill_index not in prepared.eligibility_by_staff.get(staff_index, set()):
            return None
        return {
            "staff_index": staff_index,
            "day_index": target_row["day_index"],
            "shift_index": shift_index,
            "skill_index": skill_index,
        }

    contract_code = prepared.staff_lookup[staff_index]["contract_code"]
    contract = prepared.contracts_by_code.get(contract_code, {})
    valid_shift_codes = {
        token.strip()
        for token in str(contract.get("valid_shift_codes") or "").split(",")
        if token.strip()
    }
    shift_code = prepared.shift_lookup[shift_index]["shift_code"]
    if valid_shift_codes and shift_code not in valid_shift_codes:
        return None
    return {
        "staff_index": staff_index,
        "day_index": target_row["day_index"],
        "shift_index": shift_index,
        "skill_index": None,
    }


def _sanitize_assignment_map(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
) -> dict[tuple[int, int], dict[str, Any]]:
    cleaned: dict[tuple[int, int], dict[str, Any]] = {}
    for key, assignment in assignment_map.items():
        fixed_row = prepared.fixed_assignments_by_staff_day.get(key)
        if fixed_row and fixed_row.get("is_off"):
            continue
        if fixed_row and not fixed_row.get("is_off"):
            cleaned[key] = {
                "staff_index": fixed_row["staff_index"],
                "day_index": fixed_row["day_index"],
                "shift_index": fixed_row["shift_index"],
                "skill_index": fixed_row.get("skill_index"),
            }
            continue
        cleaned[key] = assignment
    return cleaned


def _mutate_assignment_map(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    rng: random.Random,
    *,
    edit_count: int,
) -> dict[tuple[int, int], dict[str, Any]]:
    assignment_map = _sanitize_assignment_map(prepared, assignment_map)
    mutable_keys = [
        key for key in assignment_map
        if key not in prepared.fixed_assignments_by_staff_day
    ]
    for _ in range(edit_count):
        if mutable_keys and rng.random() < 0.5:
            key = rng.choice(mutable_keys)
            assignment_map.pop(key, None)
            mutable_keys = [candidate for candidate in mutable_keys if candidate != key]
            continue
        staff_row = rng.choice(prepared.staff)
        day_row = rng.choice(prepared.days)
        key = (staff_row["staff_index"], day_row["day_index"])
        fixed_row = prepared.fixed_assignments_by_staff_day.get(key)
        if fixed_row and fixed_row.get("is_off"):
            continue
        options = _enumerate_assignment_options(prepared, staff_row["staff_index"], day_row["day_index"])
        if not options:
            assignment_map.pop(key, None)
            continue
        assignment_map[key] = rng.choice(options)
        if key not in mutable_keys and key not in prepared.fixed_assignments_by_staff_day:
            mutable_keys.append(key)
    return assignment_map


def _enumerate_assignment_options(
    prepared: PreparedStaticBundle,
    staff_index: int,
    day_index: int,
) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    if prepared.dataset_family == "INRC_II":
        eligible_skills = sorted(prepared.eligibility_by_staff.get(staff_index, set()))
        for shift_row in prepared.shifts:
            for skill_index in eligible_skills:
                options.append(
                    {
                        "staff_index": staff_index,
                        "day_index": day_index,
                        "shift_index": shift_row["shift_index"],
                        "skill_index": skill_index,
                    }
                )
        return options

    contract_code = prepared.staff_lookup[staff_index]["contract_code"]
    contract = prepared.contracts_by_code.get(contract_code, {})
    valid_shift_codes = {
        token.strip()
        for token in str(contract.get("valid_shift_codes") or "").split(",")
        if token.strip()
    }
    for shift_row in prepared.shifts:
        if valid_shift_codes and shift_row["shift_code"] not in valid_shift_codes:
            continue
        options.append(
            {
                "staff_index": staff_index,
                "day_index": day_index,
                "shift_index": shift_row["shift_index"],
                "skill_index": None,
            }
        )
    return options


def _crossover_assignment_maps(
    prepared: PreparedStaticBundle,
    parent_a: dict[tuple[int, int], dict[str, Any]],
    parent_b: dict[tuple[int, int], dict[str, Any]],
    rng: random.Random,
) -> dict[tuple[int, int], dict[str, Any]]:
    child = _seed_assignment_map(prepared)
    for staff_row in prepared.staff:
        inherit_from_a = rng.random() < 0.5
        source = parent_a if inherit_from_a else parent_b
        for day_row in prepared.days:
            key = (staff_row["staff_index"], day_row["day_index"])
            if key in prepared.fixed_assignments_by_staff_day:
                continue
            if key in source:
                child[key] = copy.deepcopy(source[key])
    return child


def _local_search(
    prepared: PreparedStaticBundle,
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    rng: random.Random,
    iterations: int,
    candidate_pool_limit: int,
) -> dict[tuple[int, int], dict[str, Any]]:
    best_map = copy.deepcopy(assignment_map)
    best_eval = evaluate_static_solution(prepared, _map_to_assignment_rows(best_map))
    for _ in range(iterations):
        candidate_map = _mutate_assignment_map(prepared, copy.deepcopy(best_map), rng, edit_count=1)
        candidate_map = _repair_min_coverage(prepared, candidate_map, rng, candidate_pool_limit)
        candidate_eval = evaluate_static_solution(prepared, _map_to_assignment_rows(candidate_map))
        if candidate_eval["objective_value"] < best_eval["objective_value"]:
            best_map = candidate_map
            best_eval = candidate_eval
    return best_map


def _tournament_select(
    scored_population: list[tuple[float, dict[tuple[int, int], dict[str, Any]], dict[str, Any]]],
    rng: random.Random,
    tournament_size: int,
) -> dict[tuple[int, int], dict[str, Any]]:
    competitors = rng.sample(scored_population, k=min(tournament_size, len(scored_population)))
    competitors.sort(key=lambda item: item[0])
    return copy.deepcopy(competitors[0][1])


def _covered_count(
    assignment_map: dict[tuple[int, int], dict[str, Any]],
    target_row: dict[str, Any],
) -> int:
    total = 0
    for assignment in assignment_map.values():
        if assignment["day_index"] != target_row["day_index"]:
            continue
        if assignment["shift_index"] != target_row["shift_index"]:
            continue
        target_skill_index = target_row.get("skill_index")
        if target_skill_index is not None and assignment.get("skill_index") != target_skill_index:
            continue
        total += 1
    return total


def _rows_to_map(assignments: list[dict[str, Any]]) -> dict[tuple[int, int], dict[str, Any]]:
    return {
        (row["staff_index"], row["day_index"]): copy.deepcopy(row)
        for row in assignments
    }


def _map_to_assignment_rows(
    assignment_map: dict[tuple[int, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        assignment_map[key]
        for key in sorted(assignment_map, key=lambda pair: (pair[1], pair[0]))
    ]
