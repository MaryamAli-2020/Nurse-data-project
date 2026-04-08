"""SchedulingBenchmarks parsing, parity validation, and static feature helpers."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from .constants import REQUEST_TYPE_OFF_SHIFT, REQUEST_TYPE_ON_SHIFT
from .utils import stable_id
from .xml_utils import child_text, optional_int, parse_xml


def parse_case(
    *,
    case_code: str,
    ros_payload: str,
    txt_payload: str,
) -> dict[str, Any]:
    ros_root = parse_xml(ros_payload)
    parsed_txt = parse_text_source(txt_payload)

    start_date = datetime.strptime(child_text(ros_root, "StartDate"), "%Y-%m-%d").date()
    end_date = datetime.strptime(child_text(ros_root, "EndDate"), "%Y-%m-%d").date()
    horizon_days = (end_date - start_date).days + 1

    shift_definitions = parse_shift_types(case_code, ros_root)
    contract_definitions, global_min_rest_minutes = parse_contracts(case_code, ros_root, horizon_days)
    employee_contract_codes = parse_employee_contract_codes(case_code, ros_root, contract_definitions)
    fixed_assignments = parse_fixed_assignments(ros_root)
    request_rows = parse_requests(ros_root)
    cover_rows = parse_cover_requirements(case_code, ros_root)

    validate_parity(
        case_code=case_code,
        parsed_txt=parsed_txt,
        horizon_days=horizon_days,
        shift_definitions=shift_definitions,
        contract_definitions=contract_definitions,
        employee_contract_codes=employee_contract_codes,
        fixed_assignments=fixed_assignments,
        request_rows=request_rows,
        cover_rows=cover_rows,
        global_min_rest_minutes=global_min_rest_minutes,
    )

    day_rows = []
    for day_index in range(horizon_days):
        current_date = start_date + timedelta(days=day_index)
        day_rows.append(
            {
                "day_index": day_index,
                "calendar_date": current_date.isoformat(),
                "day_name": current_date.strftime("%A"),
                "day_of_week_index": current_date.weekday(),
                "is_weekend": int(current_date.weekday() >= 5),
            }
        )

    employee_rows = [
        {"employee_code": employee_code, "contract_code": contract_code}
        for employee_code, contract_code in sorted(employee_contract_codes.items())
    ]

    shift_limit_rows = []
    for employee_code, contract_code in sorted(employee_contract_codes.items()):
        contract_definition = contract_definitions[contract_code]
        valid_shift_codes = set(contract_definition["valid_shift_codes"])
        explicit_max_tot = contract_definition["max_tot_by_shift"]
        for shift_code in sorted(shift_definitions):
            max_assignments = explicit_max_tot.get(shift_code)
            if max_assignments is None:
                max_assignments = horizon_days if shift_code in valid_shift_codes else 0
            shift_limit_rows.append(
                {"employee_code": employee_code, "shift_code": shift_code, "max_assignments": max_assignments}
            )

    return {
        "case_code": case_code,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "horizon_days": horizon_days,
        "global_min_rest_minutes": global_min_rest_minutes,
        "shift_definitions": shift_definitions,
        "contract_definitions": contract_definitions,
        "employee_rows": employee_rows,
        "employee_contract_codes": employee_contract_codes,
        "shift_limit_rows": shift_limit_rows,
        "day_rows": day_rows,
        "fixed_assignments": fixed_assignments,
        "request_rows": request_rows,
        "cover_rows": cover_rows,
        "parsed_txt": parsed_txt,
    }


def parse_shift_types(case_code: str, root: Any) -> dict[str, dict[str, Any]]:
    shift_definitions: dict[str, dict[str, Any]] = {}
    for shift_node in root.find("ShiftTypes").findall("Shift"):
        shift_code = shift_node.attrib["ID"]
        start_time = child_text(shift_node, "StartTime", required=False)
        duration_minutes = int(child_text(shift_node, "Duration"))
        shift_definitions[shift_code] = {
            "color": child_text(shift_node, "Color", required=False),
            "start_time": start_time,
            "duration_minutes": duration_minutes,
            "end_time": compute_end_time(start_time, duration_minutes),
        }
    if not shift_definitions:
        raise ValueError(f"SchedulingBenchmarks case {case_code} defines no shift types.")
    return shift_definitions


def parse_contracts(
    case_code: str,
    root: Any,
    horizon_days: int,
) -> tuple[dict[str, dict[str, Any]], int | None]:
    contract_definitions: dict[str, dict[str, Any]] = {}
    global_min_rest_minutes: int | None = None

    for contract_node in root.find("Contracts").findall("Contract"):
        contract_code = contract_node.attrib["ID"]
        min_rest_minutes = optional_int(child_text(contract_node, "MinRestTime", required=False))
        if min_rest_minutes is not None:
            global_min_rest_minutes = max(global_min_rest_minutes or 0, min_rest_minutes)

        max_total_minutes = workload_count(contract_node, "Max")
        min_total_minutes = workload_count(contract_node, "Min")
        max_consecutive_shifts = attr_int(contract_node, "MaxSeq", shift_filter="$", default=horizon_days)
        min_consecutive_shifts = attr_int(contract_node, "MinSeq", shift_filter="$", default=1)
        min_consecutive_days_off = attr_int(contract_node, "MinSeq", shift_filter="-", default=1)
        max_working_weekends = optional_nested_int(contract_node, ("Patterns", "Match", "Max", "Count"))

        valid_shift_codes = (
            parse_comma_list(contract_node.find("ValidShifts").attrib.get("shift", ""))
            if contract_node.find("ValidShifts") is not None
            else []
        )
        max_tot_by_shift: dict[str, int] = {}
        for max_tot_node in contract_node.findall("MaxTot"):
            shift_code = max_tot_node.attrib["shift"]
            max_tot_by_shift[shift_code] = int(max_tot_node.attrib["value"])
            if shift_code not in valid_shift_codes:
                valid_shift_codes.append(shift_code)

        is_meta_contract = bool(min_rest_minutes is not None and max_total_minutes is None and not valid_shift_codes)
        contract_definitions[contract_code] = {
            "contract_code": contract_code,
            "min_rest_minutes": min_rest_minutes,
            "min_total_minutes": min_total_minutes,
            "max_total_minutes": max_total_minutes,
            "min_consecutive_shifts": min_consecutive_shifts,
            "max_consecutive_shifts": max_consecutive_shifts,
            "min_consecutive_days_off": min_consecutive_days_off,
            "max_working_weekends": max_working_weekends,
            "valid_shift_codes": tuple(valid_shift_codes),
            "max_tot_by_shift": max_tot_by_shift,
            "is_meta_contract": is_meta_contract,
            "stable_contract_id": stable_id("sb_contract", case_code, contract_code),
        }
    if not contract_definitions:
        raise ValueError(f"SchedulingBenchmarks case {case_code} defines no contracts.")
    return contract_definitions, global_min_rest_minutes


def parse_employee_contract_codes(
    case_code: str,
    root: Any,
    contract_definitions: dict[str, dict[str, Any]],
) -> dict[str, str]:
    employee_contract_codes: dict[str, str] = {}
    meta_contracts = {code for code, definition in contract_definitions.items() if definition["is_meta_contract"]}

    for employee_node in root.find("Employees").findall("Employee"):
        employee_code = employee_node.attrib["ID"]
        contract_codes = [node.text.strip() for node in employee_node.findall("ContractID") if node.text and node.text.strip()]
        specific_contracts = [code for code in contract_codes if code not in meta_contracts]
        if len(specific_contracts) != 1:
            raise ValueError(
                f"SchedulingBenchmarks employee {employee_code} in {case_code} must have exactly one non-meta contract; "
                f"found {specific_contracts or contract_codes}."
            )
        employee_contract_codes[employee_code] = specific_contracts[0]

    if not employee_contract_codes:
        raise ValueError(f"SchedulingBenchmarks case {case_code} defines no employees.")
    return employee_contract_codes


def parse_fixed_assignments(root: Any) -> list[dict[str, Any]]:
    fixed_rows: list[dict[str, Any]] = []
    fixed_root = root.find("FixedAssignments")
    if fixed_root is None:
        return fixed_rows
    for employee_node in fixed_root.findall("Employee"):
        employee_code = child_text(employee_node, "EmployeeID")
        for assign_node in employee_node.findall("Assign"):
            fixed_rows.append(
                {
                    "employee_code": employee_code,
                    "shift_code": child_text(assign_node, "Shift"),
                    "day_index": int(child_text(assign_node, "Day")),
                }
            )
    return fixed_rows


def parse_requests(root: Any) -> list[dict[str, Any]]:
    request_rows: list[dict[str, Any]] = []
    for request_type, container_name, element_name in (
        (REQUEST_TYPE_OFF_SHIFT, "ShiftOffRequests", "ShiftOff"),
        (REQUEST_TYPE_ON_SHIFT, "ShiftOnRequests", "ShiftOn"),
    ):
        container = root.find(container_name)
        if container is None:
            continue
        for request_node in container.findall(element_name):
            request_rows.append(
                {
                    "request_type": request_type,
                    "employee_code": child_text(request_node, "EmployeeID"),
                    "day_index": int(child_text(request_node, "Day")),
                    "shift_code": child_text(request_node, "Shift"),
                    "weight": int(request_node.attrib["weight"]),
                }
            )
    return request_rows


def parse_cover_requirements(case_code: str, root: Any) -> list[dict[str, Any]]:
    cover_rows: list[dict[str, Any]] = []
    cover_root = root.find("CoverRequirements")
    if cover_root is None:
        raise ValueError(f"SchedulingBenchmarks case {case_code} is missing CoverRequirements.")
    for day_cover_node in cover_root.findall("DateSpecificCover"):
        day_index = int(child_text(day_cover_node, "Day"))
        for cover_node in day_cover_node.findall("Cover"):
            min_node = cover_node.find("Min")
            max_node = cover_node.find("Max")
            if min_node is None or max_node is None:
                raise ValueError(f"SchedulingBenchmarks case {case_code} has a cover row missing Min/Max.")
            cover_rows.append(
                {
                    "day_index": day_index,
                    "shift_code": child_text(cover_node, "Shift"),
                    "min_required": int((min_node.text or "0").strip()),
                    "preferred_required": int((max_node.text or "0").strip()),
                    "under_weight": int(min_node.attrib["weight"]),
                    "over_weight": int(max_node.attrib["weight"]),
                }
            )
    if not cover_rows:
        raise ValueError(f"SchedulingBenchmarks case {case_code} defines no cover requirements.")
    return cover_rows


def parse_text_source(raw_payload: str) -> dict[str, Any]:
    sections: dict[str, list[str]] = {}
    current_section: str | None = None
    for raw_line in raw_payload.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("SECTION_"):
            current_section = line
            sections[current_section] = []
            continue
        if current_section is None:
            raise ValueError("SchedulingBenchmarks text source contains data before the first SECTION_ header.")
        sections[current_section].append(line)

    required_sections = {
        "SECTION_HORIZON",
        "SECTION_SHIFTS",
        "SECTION_STAFF",
        "SECTION_DAYS_OFF",
        "SECTION_SHIFT_ON_REQUESTS",
        "SECTION_SHIFT_OFF_REQUESTS",
        "SECTION_COVER",
    }
    missing_sections = sorted(required_sections.difference(sections))
    if missing_sections:
        raise ValueError(f"SchedulingBenchmarks text source is missing sections: {', '.join(missing_sections)}.")

    parsed_shifts: dict[str, dict[str, Any]] = {}
    for line in sections["SECTION_SHIFTS"]:
        shift_code, duration_text, forbidden_text = (part.strip() for part in line.split(",", 2))
        parsed_shifts[shift_code] = {
            "duration_minutes": int(duration_text),
            "forbidden_successors": set(filter(None, forbidden_text.split("|"))),
        }

    parsed_staff: dict[str, dict[str, Any]] = {}
    for line in sections["SECTION_STAFF"]:
        employee_code, max_shifts_text, max_total_text, min_total_text, max_seq_text, min_seq_text, min_off_text, max_weekends_text = (
            part.strip() for part in line.split(",", 7)
        )
        max_shift_map: dict[str, int] = {}
        for token in max_shifts_text.split("|"):
            shift_code, value_text = token.split("=", 1)
            max_shift_map[shift_code] = int(value_text)
        parsed_staff[employee_code] = {
            "max_shift_map": max_shift_map,
            "max_total_minutes": int(max_total_text),
            "min_total_minutes": int(min_total_text),
            "max_consecutive_shifts": int(max_seq_text),
            "min_consecutive_shifts": int(min_seq_text),
            "min_consecutive_days_off": int(min_off_text),
            "max_weekends": int(max_weekends_text),
        }

    parsed_days_off: dict[str, set[int]] = {}
    for line in sections["SECTION_DAYS_OFF"]:
        parts = [part.strip() for part in line.split(",") if part.strip()]
        employee_code = parts[0]
        parsed_days_off[employee_code] = {int(part) for part in parts[1:]}

    def parse_request_lines(lines: list[str], request_type: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for line in lines:
            employee_code, day_text, shift_code, weight_text = (part.strip() for part in line.split(",", 3))
            rows.append(
                {
                    "request_type": request_type,
                    "employee_code": employee_code,
                    "day_index": int(day_text),
                    "shift_code": shift_code,
                    "weight": int(weight_text),
                }
            )
        return rows

    parsed_requests = parse_request_lines(sections["SECTION_SHIFT_ON_REQUESTS"], REQUEST_TYPE_ON_SHIFT) + parse_request_lines(
        sections["SECTION_SHIFT_OFF_REQUESTS"], REQUEST_TYPE_OFF_SHIFT
    )
    parsed_cover_rows = []
    for line in sections["SECTION_COVER"]:
        day_text, shift_code, requirement_text, under_text, over_text = (part.strip() for part in line.split(",", 4))
        parsed_cover_rows.append(
            {
                "day_index": int(day_text),
                "shift_code": shift_code,
                "min_required": int(requirement_text),
                "preferred_required": int(requirement_text),
                "under_weight": int(under_text),
                "over_weight": int(over_text),
            }
        )

    return {
        "horizon_days": int(sections["SECTION_HORIZON"][0]),
        "shifts": parsed_shifts,
        "staff": parsed_staff,
        "days_off": parsed_days_off,
        "requests": parsed_requests,
        "cover_rows": parsed_cover_rows,
    }


def validate_parity(
    *,
    case_code: str,
    parsed_txt: dict[str, Any],
    horizon_days: int,
    shift_definitions: dict[str, dict[str, Any]],
    contract_definitions: dict[str, dict[str, Any]],
    employee_contract_codes: dict[str, str],
    fixed_assignments: list[dict[str, Any]],
    request_rows: list[dict[str, Any]],
    cover_rows: list[dict[str, Any]],
    global_min_rest_minutes: int | None,
) -> None:
    if parsed_txt["horizon_days"] != horizon_days:
        raise ValueError(
            f"SchedulingBenchmarks case {case_code} has a horizon mismatch: "
            f".ros={horizon_days}, .txt={parsed_txt['horizon_days']}."
        )

    ros_shift_codes = set(shift_definitions)
    txt_shift_codes = set(parsed_txt["shifts"])
    if ros_shift_codes != txt_shift_codes:
        raise ValueError(f"SchedulingBenchmarks case {case_code} has mismatched shift codes between .ros and .txt.")

    ros_employee_codes = set(employee_contract_codes)
    txt_employee_codes = set(parsed_txt["staff"])
    if ros_employee_codes != txt_employee_codes:
        raise ValueError(f"SchedulingBenchmarks case {case_code} has mismatched employee codes between .ros and .txt.")

    derived_forbidden = derive_forbidden_successions(shift_definitions, global_min_rest_minutes or 0)
    txt_forbidden = {shift_code: definition["forbidden_successors"] for shift_code, definition in parsed_txt["shifts"].items()}
    if derived_forbidden != txt_forbidden:
        raise ValueError(f"SchedulingBenchmarks case {case_code} has mismatched forbidden-successor semantics between .ros and .txt.")

    for employee_code, contract_code in employee_contract_codes.items():
        contract_definition = contract_definitions[contract_code]
        txt_staff = parsed_txt["staff"][employee_code]
        comparisons = (
            ("max_total_minutes", contract_definition["max_total_minutes"], txt_staff["max_total_minutes"]),
            ("min_total_minutes", contract_definition["min_total_minutes"], txt_staff["min_total_minutes"]),
            ("max_consecutive_shifts", contract_definition["max_consecutive_shifts"], txt_staff["max_consecutive_shifts"]),
            ("min_consecutive_shifts", contract_definition["min_consecutive_shifts"], txt_staff["min_consecutive_shifts"]),
            ("min_consecutive_days_off", contract_definition["min_consecutive_days_off"], txt_staff["min_consecutive_days_off"]),
            ("max_working_weekends", contract_definition["max_working_weekends"], txt_staff["max_weekends"]),
        )
        for label, ros_value, txt_value in comparisons:
            if ros_value != txt_value:
                raise ValueError(
                    f"SchedulingBenchmarks case {case_code} has a {label} mismatch for employee {employee_code}."
                )

        valid_shift_codes = set(contract_definition["valid_shift_codes"])
        explicit_max_tot = contract_definition["max_tot_by_shift"]
        derived_shift_limits = {}
        for shift_code in sorted(shift_definitions):
            max_assignments = explicit_max_tot.get(shift_code)
            if max_assignments is None:
                max_assignments = horizon_days if shift_code in valid_shift_codes else 0
            derived_shift_limits[shift_code] = max_assignments
        if derived_shift_limits != txt_staff["max_shift_map"]:
            raise ValueError(f"SchedulingBenchmarks case {case_code} has per-shift max mismatch for {employee_code}.")

    ros_days_off = defaultdict(set)
    for fixed_row in fixed_assignments:
        if fixed_row["shift_code"] == "-":
            ros_days_off[fixed_row["employee_code"]].add(fixed_row["day_index"])
    if {key: set(value) for key, value in ros_days_off.items()} != parsed_txt["days_off"]:
        raise ValueError(f"SchedulingBenchmarks case {case_code} has mismatched fixed off-days between .ros and .txt.")

    ros_requests = {
        (row["request_type"], row["employee_code"], row["day_index"], row["shift_code"], row["weight"])
        for row in request_rows
    }
    txt_requests = {
        (row["request_type"], row["employee_code"], row["day_index"], row["shift_code"], row["weight"])
        for row in parsed_txt["requests"]
    }
    if ros_requests != txt_requests:
        raise ValueError(f"SchedulingBenchmarks case {case_code} has mismatched request rows between .ros and .txt.")

    ros_cover = {
        (row["day_index"], row["shift_code"], row["min_required"], row["preferred_required"], row["under_weight"], row["over_weight"])
        for row in cover_rows
    }
    txt_cover = {
        (row["day_index"], row["shift_code"], row["min_required"], row["preferred_required"], row["under_weight"], row["over_weight"])
        for row in parsed_txt["cover_rows"]
    }
    if ros_cover != txt_cover:
        raise ValueError(f"SchedulingBenchmarks case {case_code} has mismatched cover rows between .ros and .txt.")


def optional_nested_int(node: Any, path: tuple[str, ...]) -> int | None:
    current = node
    for part in path:
        if current is None:
            return None
        current = current.find(part)
    if current is None or current.text is None:
        return None
    return int(current.text.strip())


def workload_count(contract_node: Any, bound_tag: str) -> int | None:
    workload_node = contract_node.find("Workload")
    if workload_node is None:
        return None
    for time_units_node in workload_node.findall("TimeUnits"):
        bound_node = time_units_node.find(bound_tag)
        if bound_node is None:
            continue
        count_node = bound_node.find("Count")
        if count_node is not None and count_node.text is not None:
            return int(count_node.text.strip())
    return None


def attr_int(node: Any, tag_name: str, *, shift_filter: str, default: int | None) -> int | None:
    for child in node.findall(tag_name):
        if child.attrib.get("shift") == shift_filter:
            return int(child.attrib["value"])
    return default


def parse_comma_list(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def time_to_minutes(value: str | None) -> int | None:
    if value is None:
        return None
    hour_text, minute_text = value.split(":", 1)
    return (int(hour_text) * 60) + int(minute_text)


def compute_end_time(start_time: str | None, duration_minutes: int) -> str | None:
    start_minutes = time_to_minutes(start_time)
    if start_minutes is None:
        return None
    total_minutes = (start_minutes + duration_minutes) % (24 * 60)
    return f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"


def derive_forbidden_successions(
    shift_definitions: dict[str, dict[str, Any]],
    min_rest_minutes: int,
) -> dict[str, set[str]]:
    forbidden: dict[str, set[str]] = {}
    for prev_shift_code, prev_definition in shift_definitions.items():
        prev_start = time_to_minutes(prev_definition["start_time"])
        if prev_start is None:
            raise ValueError(f"Cannot derive shift successions without a start time for shift {prev_shift_code}.")
        prev_end_absolute = prev_start + int(prev_definition["duration_minutes"])
        forbidden_targets: set[str] = set()
        for next_shift_code, next_definition in shift_definitions.items():
            next_start = time_to_minutes(next_definition["start_time"])
            if next_start is None:
                raise ValueError(f"Cannot derive shift successions without a start time for shift {next_shift_code}.")
            rest_minutes = (24 * 60 + next_start) - prev_end_absolute
            if rest_minutes < min_rest_minutes:
                forbidden_targets.add(next_shift_code)
        forbidden[prev_shift_code] = forbidden_targets
    return forbidden
