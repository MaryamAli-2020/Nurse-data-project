"""Typed records used by discovery, parsing, validation, and bundle generation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DatasetRoot:
    dataset_id: str
    dataset_code: str
    dataset_family: str
    problem_mode: str
    display_name: str
    root_path: Path
    scan_root_path: Path
    is_test: bool


@dataclass(frozen=True)
class DiscoveredSourceFile:
    raw_document_id: str
    dataset_id: str
    dataset_code: str
    dataset_family: str
    problem_mode: str
    source_file: Path
    relative_path: str
    source_format: str
    source_group_code: str | None
    preferred_canonical_source: bool
    paired_raw_document_id: str | None
    file_role: str
    root_tag: str | None
    scenario_folder_code: str | None
    instance_dir_name: str | None
    scenario_id_in_xml: str | None
    scenario_token_in_name: str | None
    file_code: int | None
    native_case_code: str | None
    content_sha256: str
    raw_payload: str


@dataclass(frozen=True)
class DiscoveredInstanceDir:
    dataset_id: str
    dataset_code: str
    dataset_family: str
    scenario_folder_code: str
    scenario_folder_path: Path
    instance_dir_name: str
    instance_dir_path: Path
    initial_history_variant: int
    week_sequence: tuple[int, ...]
    solution_file_count: int


@dataclass(frozen=True)
class DiscoveredStaticCase:
    dataset_id: str
    dataset_code: str
    dataset_family: str
    case_code: str
    ros_file: Path
    txt_file: Path
    scan_dir_path: Path


@dataclass
class StaticInstanceBundle:
    bundle_id: str
    dataset_family: str
    problem_mode: str
    instance_id: str | None
    instance_code: str
    native_case_code: str | None
    counts: dict[str, int]
    days: list[dict[str, Any]]
    staff: list[dict[str, Any]]
    shifts: list[dict[str, Any]]
    skills: list[dict[str, Any]]
    constraints: dict[str, Any]
    objective_weights: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationIssue:
    level: str
    code: str
    message: str
    context: dict[str, str | int | None] = field(default_factory=dict)


@dataclass
class ValidationReport:
    checks: list[dict[str, object]] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)

    def add_check(self, name: str, passed: bool, details: str, count: int | None = None) -> None:
        entry: dict[str, object] = {"name": name, "passed": passed, "details": details}
        if count is not None:
            entry["count"] = count
        self.checks.append(entry)

    def add_issue(
        self,
        level: str,
        code: str,
        message: str,
        context: dict[str, str | int | None] | None = None,
    ) -> None:
        self.issues.append(ValidationIssue(level=level, code=code, message=message, context=context or {}))

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.level.upper() == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.level.upper() == "WARNING")
