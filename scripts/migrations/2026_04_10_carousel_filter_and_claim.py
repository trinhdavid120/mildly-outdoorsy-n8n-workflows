#!/usr/bin/env python3
"""
One-shot migration that fixes the carousel pipeline:

1. Get many rows3 now filters by content_type=product_carousel and excludes
   shirt_name=newsapi (defense against future schema drift).
2. A new "Check In-Progress" Supabase node + "If Has In-Progress" If node
   sit between Schedule Trigger1 and Get many rows3. If any product_carousel
   row is already in status='processing', the run exits without claiming a
   new pending row.
3. A new "Claim Row" Supabase update node sits between Get many rows3 and
   Code in JavaScript5. It flips the selected row to status='processing'
   before any downstream work happens, so a re-run never starts a second
   pending row while one is still in flight.
4. Get many rows4 is removed from the true branch. JS7 never read its
   output (verified manually) and Get many rows4 returning 0 rows was
   what halted the pipeline previously. JS6 now wires straight into JS7.
5. Get many rows5 has Always Output Data enabled so JS9 still runs when
   the product_images lookup misses (JS9 already handles a missing
   product_image_url via `productImage || null`).

This script is idempotent: re-running it on an already-migrated workflow
is a no-op (it detects existing nodes by name).

Usage:
    python3 scripts/migrations/2026_04_10_carousel_filter_and_claim.py
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / "Mildly Outdoorsy - Carousel Pipeline - SAFE #6 (1).json"

CHECK_IN_PROGRESS_NAME = "Check In-Progress"
IF_HAS_IN_PROGRESS_NAME = "If Has In-Progress"
CLAIM_ROW_NAME = "Claim Row"


def load_workflow() -> Dict[str, Any]:
    return json.loads(WORKFLOW_PATH.read_text(encoding="utf-8"))


def save_workflow(workflow: Dict[str, Any]) -> None:
    WORKFLOW_PATH.write_text(
        json.dumps(workflow, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def find_node(nodes: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    for node in nodes:
        if node.get("name") == name:
            return node
    raise KeyError(f"node not found: {name}")


def has_node(nodes: List[Dict[str, Any]], name: str) -> bool:
    return any(n.get("name") == name for n in nodes)


def remove_node(workflow: Dict[str, Any], name: str) -> None:
    workflow["nodes"] = [n for n in workflow["nodes"] if n.get("name") != name]
    workflow["connections"].pop(name, None)
    for src, src_conns in list(workflow["connections"].items()):
        new_main: List[List[Dict[str, Any]]] = []
        for output in src_conns.get("main", []):
            new_output = [link for link in output if link.get("node") != name]
            new_main.append(new_output)
        src_conns["main"] = new_main


def set_connection(
    workflow: Dict[str, Any],
    source: str,
    source_output: int,
    target: str,
    target_input: int = 0,
) -> None:
    """Replace whatever was at source[source_output] with a single link to target."""
    conns = workflow["connections"].setdefault(source, {"main": []})
    main = conns.setdefault("main", [])
    while len(main) <= source_output:
        main.append([])
    main[source_output] = [
        {"node": target, "type": "main", "index": target_input}
    ]


def update_carousel_filter(workflow: Dict[str, Any]) -> None:
    node = find_node(workflow["nodes"], "Get many rows3")
    params = node.setdefault("parameters", {})
    params["filters"] = {
        "conditions": [
            {"keyName": "status", "condition": "eq", "keyValue": "pending"},
            {
                "keyName": "content_type",
                "condition": "eq",
                "keyValue": "product_carousel",
            },
            {"keyName": "shirt_name", "condition": "neq", "keyValue": "newsapi"},
        ]
    }


def add_check_in_progress(workflow: Dict[str, Any]) -> None:
    if has_node(workflow["nodes"], CHECK_IN_PROGRESS_NAME):
        return
    template = find_node(workflow["nodes"], "Get many rows3")
    node = {
        "parameters": {
            "operation": "getAll",
            "tableId": "carousel_queue",
            "limit": 1,
            "filters": {
                "conditions": [
                    {
                        "keyName": "status",
                        "condition": "eq",
                        "keyValue": "processing",
                    },
                    {
                        "keyName": "content_type",
                        "condition": "eq",
                        "keyValue": "product_carousel",
                    },
                ]
            },
        },
        "type": template["type"],
        "typeVersion": template.get("typeVersion", 1),
        "position": [432, -1264],
        "id": str(uuid.uuid4()),
        "name": CHECK_IN_PROGRESS_NAME,
        "credentials": template.get("credentials", {}),
        "alwaysOutputData": True,
    }
    workflow["nodes"].append(node)


def add_if_has_in_progress(workflow: Dict[str, Any]) -> None:
    if has_node(workflow["nodes"], IF_HAS_IN_PROGRESS_NAME):
        return
    if_template = find_node(workflow["nodes"], "If1")
    node = {
        "parameters": {
            "conditions": {
                "options": {
                    "caseSensitive": True,
                    "leftValue": "",
                    "typeValidation": "strict",
                    "version": 3,
                },
                "conditions": [
                    {
                        "id": str(uuid.uuid4()),
                        "leftValue": "={{ $json.id }}",
                        "rightValue": "",
                        "operator": {
                            "type": "string",
                            "operation": "notEmpty",
                            "singleValue": True,
                        },
                    }
                ],
                "combinator": "and",
            },
            "options": {},
        },
        "type": if_template["type"],
        "typeVersion": if_template.get("typeVersion", 2),
        "position": [656, -1264],
        "id": str(uuid.uuid4()),
        "name": IF_HAS_IN_PROGRESS_NAME,
    }
    workflow["nodes"].append(node)


def add_claim_row(workflow: Dict[str, Any]) -> None:
    if has_node(workflow["nodes"], CLAIM_ROW_NAME):
        return
    template = find_node(workflow["nodes"], "Mark Queue Row Published1")
    node = {
        "parameters": {
            "operation": "update",
            "tableId": "carousel_queue",
            "filters": {
                "conditions": [
                    {
                        "keyName": "id",
                        "condition": "eq",
                        "keyValue": "={{ $json.id }}",
                    }
                ]
            },
            "fieldsUi": {
                "fieldValues": [
                    {"fieldId": "status", "fieldValue": "processing"}
                ]
            },
        },
        "type": template["type"],
        "typeVersion": template.get("typeVersion", 1),
        "position": [1104, -1264],
        "id": str(uuid.uuid4()),
        "name": CLAIM_ROW_NAME,
        "credentials": template.get("credentials", {}),
    }
    workflow["nodes"].append(node)


def shift_existing_nodes(workflow: Dict[str, Any]) -> None:
    """Shift downstream nodes right to make room for inserted nodes."""
    # Schedule Trigger1 stays at x=208. Insert 3 new nodes (Check In-Progress,
    # If Has In-Progress, Claim Row). Get many rows3 needs to land at x=880,
    # everything from JS5 onward shifts +672 (3 slot widths).
    shifts = {
        "Get many rows3": 448,
        "Code in JavaScript5": 672,
        "If1": 672,
        "HTTP Request1": 672,
        "Code in JavaScript6": 672,
        "Code in JavaScript7": 448,  # net +672 -224 since we removed Get many rows4
        "Code in JavaScript8": 672,
        "Get many rows5": 672,
        "Code in JavaScript9": 672,
        "Merge1": 672,
        "Sort1": 672,
        "Create an image from a template1": 672,
        "Prepare Instagram Carousel1": 672,
        "Instagram Create Carousel Item1": 672,
        "Prepare Instagram Carousel Publish1": 672,
        "Instagram Create Carousel Container1": 672,
        "Prepare Instagram Publish1": 672,
        "Instagram Publish Carousel1": 672,
        "Prepare Publish Result1": 672,
        "Mark Queue Row Published1": 672,
    }
    for node in workflow["nodes"]:
        delta = shifts.get(node.get("name"))
        if delta is None:
            continue
        if node.get("_shifted_by_migration"):
            continue
        position = node.get("position") or [0, 0]
        node["position"] = [position[0] + delta, position[1]]
        node["_shifted_by_migration"] = True


def cleanup_shift_markers(workflow: Dict[str, Any]) -> None:
    for node in workflow["nodes"]:
        node.pop("_shifted_by_migration", None)


def enable_always_output_get_many_rows5(workflow: Dict[str, Any]) -> None:
    node = find_node(workflow["nodes"], "Get many rows5")
    node["alwaysOutputData"] = True


def remove_dead_get_many_rows4(workflow: Dict[str, Any]) -> None:
    if not has_node(workflow["nodes"], "Get many rows4"):
        return
    remove_node(workflow, "Get many rows4")
    set_connection(workflow, "Code in JavaScript6", 0, "Code in JavaScript7")


def rewire_pre_check(workflow: Dict[str, Any]) -> None:
    set_connection(workflow, "Schedule Trigger1", 0, CHECK_IN_PROGRESS_NAME)
    set_connection(workflow, CHECK_IN_PROGRESS_NAME, 0, IF_HAS_IN_PROGRESS_NAME)
    # If Has In-Progress: output 0 = true (id present, exit), output 1 = false
    # (continue to Get many rows3). Setting output 0 to an empty list keeps
    # the true branch terminating.
    workflow["connections"][IF_HAS_IN_PROGRESS_NAME] = {"main": [[], []]}
    set_connection(workflow, IF_HAS_IN_PROGRESS_NAME, 1, "Get many rows3")


def rewire_claim(workflow: Dict[str, Any]) -> None:
    set_connection(workflow, "Get many rows3", 0, CLAIM_ROW_NAME)
    set_connection(workflow, CLAIM_ROW_NAME, 0, "Code in JavaScript5")


def main() -> None:
    workflow = load_workflow()

    update_carousel_filter(workflow)
    add_check_in_progress(workflow)
    add_if_has_in_progress(workflow)
    add_claim_row(workflow)
    remove_dead_get_many_rows4(workflow)
    enable_always_output_get_many_rows5(workflow)
    shift_existing_nodes(workflow)
    rewire_pre_check(workflow)
    rewire_claim(workflow)
    cleanup_shift_markers(workflow)

    save_workflow(workflow)
    print(f"migrated: {WORKFLOW_PATH}")


if __name__ == "__main__":
    main()
