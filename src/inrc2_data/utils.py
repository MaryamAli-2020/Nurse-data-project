"""General helpers for stable ids and filesystem-safe normalization."""

from __future__ import annotations

import re
from pathlib import Path


def normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def stable_id(*parts: object) -> str:
    return "::".join(str(part) for part in parts)


def as_posix_path(path: Path) -> str:
    return str(path.resolve())


def scenario_folder_sort_key(name: str) -> tuple[int, str]:
    match = re.search(r"(\d+)", name)
    if match:
        return (int(match.group(1)), name)
    return (10**9, name)
