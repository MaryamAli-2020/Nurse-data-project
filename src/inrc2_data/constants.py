"""Shared constants for multi-family rostering dataset parsing and feature generation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DaySpec:
    day_index: int
    long_name: str
    short_name: str
    requirement_tag: str
    is_weekend: bool


DAY_SPECS: tuple[DaySpec, ...] = (
    DaySpec(0, "Monday", "Mon", "RequirementOnMonday", False),
    DaySpec(1, "Tuesday", "Tue", "RequirementOnTuesday", False),
    DaySpec(2, "Wednesday", "Wed", "RequirementOnWednesday", False),
    DaySpec(3, "Thursday", "Thu", "RequirementOnThursday", False),
    DaySpec(4, "Friday", "Fri", "RequirementOnFriday", False),
    DaySpec(5, "Saturday", "Sat", "RequirementOnSaturday", True),
    DaySpec(6, "Sunday", "Sun", "RequirementOnSunday", True),
)

DAY_BY_LONG_NAME = {day.long_name: day for day in DAY_SPECS}
DAY_BY_SHORT_NAME = {day.short_name: day for day in DAY_SPECS}
DAY_BY_ANY_NAME = {
    **DAY_BY_LONG_NAME,
    **DAY_BY_SHORT_NAME,
    "Monday": DAY_BY_LONG_NAME["Monday"],
    "Tuesday": DAY_BY_LONG_NAME["Tuesday"],
    "Wednesday": DAY_BY_LONG_NAME["Wednesday"],
    "Thursday": DAY_BY_LONG_NAME["Thursday"],
    "Friday": DAY_BY_LONG_NAME["Friday"],
    "Saturday": DAY_BY_LONG_NAME["Saturday"],
    "Sunday": DAY_BY_LONG_NAME["Sunday"],
}

RAW_LAYER = "raw import layer"
CANONICAL_LAYER = "canonical relational layer"
FEATURE_LAYER = "algorithm feature layer"

DATASET_FAMILY_INRC_II = "INRC_II"
DATASET_FAMILY_SCHEDULING_BENCHMARKS = "SCHEDULING_BENCHMARKS"

PROBLEM_MODE_STATIC = "STATIC"
PROBLEM_MODE_MULTISTAGE = "MULTISTAGE"
PROBLEM_MODE_STATIC_AND_MULTISTAGE = "STATIC_AND_MULTISTAGE"

SOURCE_ROLE_SCENARIO = "SCENARIO"
SOURCE_ROLE_HISTORY = "HISTORY"
SOURCE_ROLE_WEEK = "WEEK"
SOURCE_ROLE_SOLUTION = "SOLUTION"
SOURCE_ROLE_STATIC_CASE_ROS = "STATIC_CASE_ROS"
SOURCE_ROLE_STATIC_CASE_TEXT = "STATIC_CASE_TEXT"

SOURCE_FORMAT_XML = "xml"
SOURCE_FORMAT_TEXT = "text"

SNAPSHOT_TYPE_INITIAL_SOURCE = "INITIAL_SOURCE"
SNAPSHOT_TYPE_INSTANCE_INITIAL = "INSTANCE_INITIAL"
SNAPSHOT_TYPE_DERIVED_POST_WEEK = "DERIVED_POST_WEEK"

ASSIGNMENT_SOURCE_REFERENCE = "REFERENCE_SOLUTION"
ASSIGNMENT_SOURCE_SOLVER = "SOLVER_OUTPUT"

REQUEST_TYPE_OFF_ANY = "OFF_ANY"
REQUEST_TYPE_OFF_SHIFT = "OFF_SHIFT"
REQUEST_TYPE_ON_SHIFT = "ON_SHIFT"

ROOT_TAG_TO_ROLE = {
    "Scenario": SOURCE_ROLE_SCENARIO,
    "History": SOURCE_ROLE_HISTORY,
    "WeekData": SOURCE_ROLE_WEEK,
    "Solution": SOURCE_ROLE_SOLUTION,
    "SchedulingPeriod": SOURCE_ROLE_STATIC_CASE_ROS,
}
