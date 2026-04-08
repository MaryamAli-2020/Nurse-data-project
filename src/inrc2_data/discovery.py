"""Dataset-root discovery and source-file cataloging for supported roster families."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

from .constants import (
    DATASET_FAMILY_INRC_II,
    DATASET_FAMILY_SCHEDULING_BENCHMARKS,
    PROBLEM_MODE_STATIC,
    PROBLEM_MODE_STATIC_AND_MULTISTAGE,
    ROOT_TAG_TO_ROLE,
    SOURCE_FORMAT_TEXT,
    SOURCE_FORMAT_XML,
)
from .models import DatasetRoot, DiscoveredInstanceDir, DiscoveredSourceFile, DiscoveredStaticCase
from .utils import as_posix_path, normalize_token, stable_id
from .xml_utils import child_text, parse_xml, sha256_text, strip_namespace

SCENARIO_FILE_RE = re.compile(r"^Sc-(?P<scenario>.+)\.xml$", re.IGNORECASE)
HISTORY_FILE_RE = re.compile(r"^H0-(?P<scenario>.+)-(?P<variant>\d+)\.xml$", re.IGNORECASE)
WEEK_FILE_RE = re.compile(r"^WD-(?P<scenario>.+)-(?P<week_code>\d+)\.xml$", re.IGNORECASE)
SOLUTION_FILE_RE = re.compile(
    r"^Sol-(?P<scenario>.+)-(?P<week_code>\d+)-(?P<stage_index>\d+)\.xml$",
    re.IGNORECASE,
)
INSTANCE_DIR_RE = re.compile(r"^Solution_H_(?P<history>\d+)-WD_(?P<sequence>\d+(?:-\d+)*)$")
SB_CASE_FILE_RE = re.compile(r"^(?P<case_code>Instance\d+)\.(?P<extension>ros|txt)$", re.IGNORECASE)


def discover_dataset_roots(project_root: Path) -> list[DatasetRoot]:
    project_root = project_root.resolve()
    candidates = [child for child in project_root.iterdir() if child.is_dir()]
    discovered: list[DatasetRoot] = []

    for child in candidates:
        token = normalize_token(child.name)
        if "inrcii" in token and "datasets" in token:
            is_test = "test" in token
            dataset_code = "test" if is_test else "benchmark"
            scan_root_path = _find_inrc_scan_root(child)
            discovered.append(
                DatasetRoot(
                    dataset_id=stable_id("dataset", dataset_code),
                    dataset_code=dataset_code,
                    dataset_family=DATASET_FAMILY_INRC_II,
                    problem_mode=PROBLEM_MODE_STATIC_AND_MULTISTAGE,
                    display_name=("INRC-II test datasets" if is_test else "INRC-II benchmark datasets"),
                    root_path=child.resolve(),
                    scan_root_path=scan_root_path.resolve(),
                    is_test=is_test,
                )
            )
            continue

        if "schedulingbenchmarks" in token:
            scan_root_path = _find_scheduling_benchmarks_scan_root(child)
            discovered.append(
                DatasetRoot(
                    dataset_id=stable_id("dataset", "scheduling_benchmarks"),
                    dataset_code="scheduling_benchmarks",
                    dataset_family=DATASET_FAMILY_SCHEDULING_BENCHMARKS,
                    problem_mode=PROBLEM_MODE_STATIC,
                    display_name="SchedulingBenchmarks static dataset family",
                    root_path=child.resolve(),
                    scan_root_path=scan_root_path.resolve(),
                    is_test=False,
                )
            )

    if not discovered:
        raise FileNotFoundError(
            "Could not locate supported dataset roots under the project root. "
            "Expected INRC-II directories and/or a SchedulingBenchmarks directory."
        )
    return sorted(discovered, key=lambda item: (item.dataset_family, item.dataset_code))


def _find_inrc_scan_root(root_path: Path) -> Path:
    nested_xml_dirs = [
        subdir for subdir in root_path.iterdir() if subdir.is_dir() and normalize_token(subdir.name) == "datasetsxml"
    ]
    return nested_xml_dirs[0] if nested_xml_dirs else root_path


def _find_scheduling_benchmarks_scan_root(root_path: Path) -> Path:
    if any(SB_CASE_FILE_RE.match(path.name) for path in root_path.iterdir() if path.is_file()):
        return root_path
    matching_subdirs = [subdir for subdir in root_path.iterdir() if subdir.is_dir() and any(SB_CASE_FILE_RE.match(path.name) for path in subdir.iterdir() if path.is_file())]
    if matching_subdirs:
        return matching_subdirs[0]
    raise FileNotFoundError(
        f"Could not locate a SchedulingBenchmarks scan directory under {root_path}. "
        "Expected Instance*.ros and Instance*.txt files."
    )


def discover_source_files(dataset_root: DatasetRoot) -> list[DiscoveredSourceFile]:
    if dataset_root.dataset_family == DATASET_FAMILY_INRC_II:
        return _discover_inrc_source_files(dataset_root)
    if dataset_root.dataset_family == DATASET_FAMILY_SCHEDULING_BENCHMARKS:
        return _discover_scheduling_benchmarks_source_files(dataset_root)
    raise ValueError(f"Unsupported dataset family {dataset_root.dataset_family}.")


def discover_instance_dirs(dataset_root: DatasetRoot) -> list[DiscoveredInstanceDir]:
    if dataset_root.dataset_family != DATASET_FAMILY_INRC_II:
        return []

    instances: list[DiscoveredInstanceDir] = []
    for scenario_folder in sorted(
        (folder for folder in dataset_root.scan_root_path.iterdir() if folder.is_dir()),
        key=lambda item: item.name,
    ):
        for child_dir in sorted((entry for entry in scenario_folder.iterdir() if entry.is_dir()), key=lambda item: item.name):
            match = INSTANCE_DIR_RE.match(child_dir.name)
            if not match:
                continue
            sequence = tuple(int(part) for part in match.group("sequence").split("-"))
            solution_file_count = len(list(child_dir.glob("*.xml")))
            instances.append(
                DiscoveredInstanceDir(
                    dataset_id=dataset_root.dataset_id,
                    dataset_code=dataset_root.dataset_code,
                    dataset_family=dataset_root.dataset_family,
                    scenario_folder_code=scenario_folder.name,
                    scenario_folder_path=scenario_folder.resolve(),
                    instance_dir_name=child_dir.name,
                    instance_dir_path=child_dir.resolve(),
                    initial_history_variant=int(match.group("history")),
                    week_sequence=sequence,
                    solution_file_count=solution_file_count,
                )
            )
    return instances


def discover_static_cases(
    dataset_root: DatasetRoot,
    source_files: list[DiscoveredSourceFile],
) -> list[DiscoveredStaticCase]:
    if dataset_root.dataset_family != DATASET_FAMILY_SCHEDULING_BENCHMARKS:
        return []

    grouped: dict[str, dict[str, DiscoveredSourceFile]] = defaultdict(dict)
    for source_file in source_files:
        if source_file.native_case_code is None:
            continue
        grouped[source_file.native_case_code][source_file.source_format] = source_file

    cases: list[DiscoveredStaticCase] = []
    for case_code, members in sorted(grouped.items()):
        ros_member = members.get(SOURCE_FORMAT_XML)
        txt_member = members.get(SOURCE_FORMAT_TEXT)
        if ros_member is None or txt_member is None:
            raise ValueError(
                f"SchedulingBenchmarks case {case_code} is missing its paired source files. "
                f"Found formats: {sorted(members.keys())}."
            )
        cases.append(
            DiscoveredStaticCase(
                dataset_id=dataset_root.dataset_id,
                dataset_code=dataset_root.dataset_code,
                dataset_family=dataset_root.dataset_family,
                case_code=case_code,
                ros_file=ros_member.source_file,
                txt_file=txt_member.source_file,
                scan_dir_path=dataset_root.scan_root_path,
            )
        )
    return cases


def _discover_inrc_source_files(dataset_root: DatasetRoot) -> list[DiscoveredSourceFile]:
    discovered: list[DiscoveredSourceFile] = []
    for source_file in sorted(dataset_root.scan_root_path.rglob("*.xml")):
        relative_path = str(source_file.resolve().relative_to(dataset_root.scan_root_path)).replace("\\", "/")
        raw_payload, content_sha256 = _read_text_file(source_file)
        root_tag = strip_namespace(parse_xml(raw_payload).tag)
        file_role = _infer_inrc_role(source_file.name, root_tag)
        parts = _relative_parts(dataset_root, source_file)
        scenario_token_in_name, file_code = _extract_inrc_file_name_metadata(source_file.name)
        discovered.append(
            DiscoveredSourceFile(
                raw_document_id=stable_id("raw_document", dataset_root.dataset_code, relative_path),
                dataset_id=dataset_root.dataset_id,
                dataset_code=dataset_root.dataset_code,
                dataset_family=dataset_root.dataset_family,
                problem_mode=dataset_root.problem_mode,
                source_file=source_file.resolve(),
                relative_path=relative_path,
                source_format=SOURCE_FORMAT_XML,
                source_group_code=None,
                preferred_canonical_source=True,
                paired_raw_document_id=None,
                file_role=file_role,
                root_tag=root_tag,
                scenario_folder_code=_scenario_folder_code(parts),
                instance_dir_name=_instance_dir_name(parts),
                scenario_id_in_xml=_scenario_id_from_root(root_tag, raw_payload),
                scenario_token_in_name=scenario_token_in_name,
                file_code=file_code,
                native_case_code=None,
                content_sha256=content_sha256,
                raw_payload=raw_payload,
            )
        )
    return discovered


def _discover_scheduling_benchmarks_source_files(dataset_root: DatasetRoot) -> list[DiscoveredSourceFile]:
    file_map: dict[str, dict[str, Path]] = defaultdict(dict)
    for source_file in sorted(dataset_root.scan_root_path.iterdir(), key=lambda item: item.name):
        if not source_file.is_file():
            continue
        match = SB_CASE_FILE_RE.match(source_file.name)
        if not match:
            continue
        case_code = match.group("case_code")
        extension = match.group("extension").lower()
        if extension in file_map[case_code]:
            raise ValueError(f"Duplicate SchedulingBenchmarks file for case {case_code} and extension {extension}.")
        file_map[case_code][extension] = source_file

    discovered: list[DiscoveredSourceFile] = []
    for case_code, members in sorted(file_map.items()):
        if {"ros", "txt"} != set(members):
            raise ValueError(
                f"SchedulingBenchmarks case {case_code} must have both .ros and .txt files. "
                f"Found {sorted(members)}."
            )
        ros_file = members["ros"]
        txt_file = members["txt"]
        ros_relative_path = str(ros_file.resolve().relative_to(dataset_root.scan_root_path)).replace("\\", "/")
        txt_relative_path = str(txt_file.resolve().relative_to(dataset_root.scan_root_path)).replace("\\", "/")
        ros_document_id = stable_id("raw_document", dataset_root.dataset_code, ros_relative_path)
        txt_document_id = stable_id("raw_document", dataset_root.dataset_code, txt_relative_path)

        ros_payload, ros_sha = _read_text_file(ros_file)
        txt_payload, txt_sha = _read_text_file(txt_file)
        ros_root_tag = strip_namespace(parse_xml(ros_payload).tag)
        if ros_root_tag != "SchedulingPeriod":
            raise ValueError(f"Expected SchedulingBenchmarks .ros root tag SchedulingPeriod, found {ros_root_tag}.")

        discovered.extend(
            [
                DiscoveredSourceFile(
                    raw_document_id=ros_document_id,
                    dataset_id=dataset_root.dataset_id,
                    dataset_code=dataset_root.dataset_code,
                    dataset_family=dataset_root.dataset_family,
                    problem_mode=dataset_root.problem_mode,
                    source_file=ros_file.resolve(),
                    relative_path=ros_relative_path,
                    source_format=SOURCE_FORMAT_XML,
                    source_group_code=case_code,
                    preferred_canonical_source=True,
                    paired_raw_document_id=txt_document_id,
                    file_role=ROOT_TAG_TO_ROLE["SchedulingPeriod"],
                    root_tag=ros_root_tag,
                    scenario_folder_code=None,
                    instance_dir_name=None,
                    scenario_id_in_xml=None,
                    scenario_token_in_name=None,
                    file_code=None,
                    native_case_code=case_code,
                    content_sha256=ros_sha,
                    raw_payload=ros_payload,
                ),
                DiscoveredSourceFile(
                    raw_document_id=txt_document_id,
                    dataset_id=dataset_root.dataset_id,
                    dataset_code=dataset_root.dataset_code,
                    dataset_family=dataset_root.dataset_family,
                    problem_mode=dataset_root.problem_mode,
                    source_file=txt_file.resolve(),
                    relative_path=txt_relative_path,
                    source_format=SOURCE_FORMAT_TEXT,
                    source_group_code=case_code,
                    preferred_canonical_source=False,
                    paired_raw_document_id=ros_document_id,
                    file_role="STATIC_CASE_TEXT",
                    root_tag=None,
                    scenario_folder_code=None,
                    instance_dir_name=None,
                    scenario_id_in_xml=None,
                    scenario_token_in_name=None,
                    file_code=None,
                    native_case_code=case_code,
                    content_sha256=txt_sha,
                    raw_payload=txt_payload,
                ),
            ]
        )
    if not discovered:
        raise FileNotFoundError(
            f"No SchedulingBenchmarks source files were discovered under {dataset_root.scan_root_path}."
        )
    return discovered


def _read_text_file(path: Path) -> tuple[str, str]:
    raw_payload = path.read_text(encoding="utf-8")
    return raw_payload, sha256_text(raw_payload)


def _relative_parts(dataset_root: DatasetRoot, source_file: Path) -> tuple[str, ...]:
    return source_file.resolve().relative_to(dataset_root.scan_root_path).parts


def _scenario_folder_code(parts: tuple[str, ...]) -> str | None:
    if parts and parts[0].lower().startswith("n"):
        return parts[0]
    return None


def _instance_dir_name(parts: tuple[str, ...]) -> str | None:
    if len(parts) >= 2 and parts[1].startswith("Solution_"):
        return parts[1]
    return None


def _infer_inrc_role(file_name: str, root_tag: str) -> str:
    if file_name.startswith("Sc-"):
        expected = "Scenario"
    elif file_name.startswith("H0-"):
        expected = "History"
    elif file_name.startswith("WD-"):
        expected = "WeekData"
    elif file_name.startswith("Sol-"):
        expected = "Solution"
    else:
        expected = root_tag
    if strip_namespace(expected) != strip_namespace(root_tag):
        raise ValueError(f"File-role mismatch for {file_name}: name suggests <{expected}> but XML root is <{root_tag}>.")
    role = ROOT_TAG_TO_ROLE.get(strip_namespace(root_tag))
    if role is None:
        raise ValueError(f"Unsupported XML root tag <{root_tag}> in {file_name}.")
    return role


def _extract_inrc_file_name_metadata(file_name: str) -> tuple[str | None, int | None]:
    for regex in (SCENARIO_FILE_RE, HISTORY_FILE_RE, WEEK_FILE_RE, SOLUTION_FILE_RE):
        match = regex.match(file_name)
        if match:
            scenario_token = match.groupdict().get("scenario")
            numeric_token = match.groupdict().get("variant") or match.groupdict().get("week_code")
            return scenario_token, (int(numeric_token) if numeric_token is not None else None)
    return None, None


def _scenario_id_from_root(root_tag: str, raw_payload: str) -> str | None:
    root = parse_xml(raw_payload)
    if root_tag == "Scenario":
        return root.attrib.get("Id")
    if root_tag in {"History", "WeekData", "Solution"}:
        return child_text(root, "Scenario")
    return None


def build_scan_summary(
    dataset_roots: Iterable[DatasetRoot],
    source_files: Iterable[DiscoveredSourceFile],
    instance_dirs: Iterable[DiscoveredInstanceDir],
    static_cases: Iterable[DiscoveredStaticCase],
) -> dict[str, object]:
    source_rows = list(source_files)
    instance_rows = list(instance_dirs)
    static_case_rows = list(static_cases)
    root_rows: list[dict[str, object]] = []

    for dataset_root in dataset_roots:
        dataset_files = [row for row in source_rows if row.dataset_id == dataset_root.dataset_id]
        dataset_instances = [row for row in instance_rows if row.dataset_id == dataset_root.dataset_id]
        dataset_static_cases = [row for row in static_case_rows if row.dataset_id == dataset_root.dataset_id]
        format_counts = Counter(row.source_format for row in dataset_files)

        row: dict[str, object] = {
            "dataset_id": dataset_root.dataset_id,
            "dataset_code": dataset_root.dataset_code,
            "dataset_family": dataset_root.dataset_family,
            "problem_mode": dataset_root.problem_mode,
            "root_path": as_posix_path(dataset_root.root_path),
            "scan_root_path": as_posix_path(dataset_root.scan_root_path),
            "source_file_count": len(dataset_files),
            "source_formats": dict(sorted(format_counts.items())),
        }

        if dataset_root.dataset_family == DATASET_FAMILY_INRC_II:
            folder_codes = sorted({row.scenario_folder_code for row in dataset_files if row.scenario_folder_code})
            folder_summary: list[dict[str, object]] = []
            for folder_code in folder_codes:
                folder_files = [row for row in dataset_files if row.scenario_folder_code == folder_code]
                scenario_rows = [row for row in folder_files if row.file_role == "SCENARIO"]
                history_rows = [row for row in folder_files if row.file_role == "HISTORY"]
                week_rows = [row for row in folder_files if row.file_role == "WEEK"]
                solution_rows = [row for row in folder_files if row.file_role == "SOLUTION"]
                folder_instances = [row for row in dataset_instances if row.scenario_folder_code == folder_code]
                folder_summary.append(
                    {
                        "scenario_folder_code": folder_code,
                        "scenario_xml_count": len(scenario_rows),
                        "history_xml_count": len(history_rows),
                        "week_xml_count": len(week_rows),
                        "solution_xml_count": len(solution_rows),
                        "solution_dir_count": len(folder_instances),
                        "solution_dir_names": [row.instance_dir_name for row in folder_instances],
                        "scenario_ids_in_xml": sorted({row.scenario_id_in_xml for row in folder_files if row.scenario_id_in_xml}),
                    }
                )
            row.update(
                {
                    "scenario_folder_count": len(folder_codes),
                    "instance_dir_count": len(dataset_instances),
                    "scenario_folders": folder_summary,
                }
            )
        else:
            case_rows = []
            files_by_case: dict[str, list[DiscoveredSourceFile]] = defaultdict(list)
            for source_file in dataset_files:
                if source_file.native_case_code:
                    files_by_case[source_file.native_case_code].append(source_file)
            for static_case in dataset_static_cases:
                members = files_by_case.get(static_case.case_code, [])
                case_rows.append(
                    {
                        "case_code": static_case.case_code,
                        "ros_present": any(member.source_format == SOURCE_FORMAT_XML for member in members),
                        "txt_present": any(member.source_format == SOURCE_FORMAT_TEXT for member in members),
                        "file_count": len(members),
                    }
                )
            row.update({"case_count": len(dataset_static_cases), "cases": case_rows})

        root_rows.append(row)
    return {"dataset_roots": root_rows}
