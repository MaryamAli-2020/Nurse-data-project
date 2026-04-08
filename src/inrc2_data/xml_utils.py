"""XML helpers that normalize namespace differences across benchmark files."""

from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET


def strip_namespace(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def strip_namespaces_in_place(element: ET.Element) -> ET.Element:
    for node in element.iter():
        node.tag = strip_namespace(node.tag)
    return element


def parse_xml(raw_xml: str) -> ET.Element:
    root = ET.fromstring(raw_xml)
    return strip_namespaces_in_place(root)


def child_text(element: ET.Element, child_name: str, *, required: bool = True, default: str | None = None) -> str | None:
    child = element.find(child_name)
    if child is None or child.text is None:
        if required:
            raise ValueError(f"Missing XML child <{child_name}> under <{element.tag}>.")
        return default
    return child.text.strip()


def optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
