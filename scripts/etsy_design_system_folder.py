#!/usr/bin/env python3
"""Keep the Etsy Design System workflows together in n8n: one tag, and one folder when licensed.

Runs on the self-hosted runner right after the Etsy Design System deploy, with the same
N8N_BASE_URL / N8N_API_KEY. For every workflow listed in the manifest's "folder" block (the managed
entries plus "also_include") it:
  1. applies the tag named there (tags are a community feature; this never rewrites a workflow), and
  2. moves the workflow into the folder named there, if the public folder API is licensed on the
     target. A move is a PUT of the workflow's own content plus parentFolderId, verified with a GET.
     A workflow whose settings the API whitelist would narrow is never PUT; it is reported as a
     WARNING so it can be moved by hand in the UI.
Every refusal is printed, never hidden. The shared deploy script is imported, not modified.
"""
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from deploy_workflows_to_n8n import (  # noqa: E402
    ALLOWED_SETTINGS_FIELDS, N8NClient, build_payload, fail,
)


def _items(data: Any) -> list:
    if isinstance(data, dict) and "data" in data:
        return data["data"] or []
    return data or []


def _unwrap(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
        return data["data"]
    return data


# ---------------------------------------------------------------- tag (community feature)

def ensure_tag(client: N8NClient, name: str, dry_run: bool) -> Optional[str]:
    for tag in _items(client._request("GET", "/api/v1/tags?limit=100")):
        if tag.get("name") == name:
            print(f"[tag] '{name}' exists -> {tag.get('id')}")
            return str(tag.get("id"))
    if dry_run:
        print(f"[tag] '{name}' would be created")
        return None
    created = _unwrap(client._request("POST", "/api/v1/tags", {"name": name}))
    print(f"[tag] created '{name}' -> {created.get('id')}")
    return str(created.get("id"))


def apply_tag(client: N8NClient, wf: Dict[str, Any], tag_id: str, dry_run: bool) -> str:
    wid, name = str(wf["id"]), wf["name"]
    current = _items(client._request("GET", f"/api/v1/workflows/{wid}/tags"))
    ids: List[str] = [str(t.get("id")) for t in current]
    if tag_id in ids:
        print(f"[tag ok] {name}")
        return "ok"
    if dry_run:
        print(f"[tag] {name} would be tagged (dry run)")
        return "dry"
    client._request("PUT", f"/api/v1/workflows/{wid}/tags", [{"id": i} for i in ids + [tag_id]])
    after = [str(t.get("id")) for t in _items(client._request("GET", f"/api/v1/workflows/{wid}/tags"))]
    if tag_id in after:
        print(f"[tagged] {name}")
        return "tagged"
    print(f"WARNING [untagged] {name}: PUT accepted but the tag is not on the workflow")
    return "failed"


# ---------------------------------------------------------------- folder (licensed feature)

def ensure_folder(client: N8NClient, project_id: str, name: str, dry_run: bool) -> Optional[str]:
    """-> folder id, or None when the API is not licensed / dry run. Prints why."""
    try:
        folders = _items(client._request("GET", f"/api/v1/projects/{project_id}/folders"))
    except SystemExit:
        print(f"WARNING [folder] listing folders failed (the HTTP status and body are printed above; "
              f"403 means the folder API needs the feat:folders license) — create the folder '{name}' "
              f"in the UI and drag the tagged workflows in")
        return None
    for folder in folders:
        if folder.get("name") == name:
            print(f"[folder] '{name}' exists -> {folder.get('id')}")
            return str(folder.get("id"))
    if dry_run:
        print(f"[folder] '{name}' would be created")
        return None
    created = _unwrap(client._request("POST", f"/api/v1/projects/{project_id}/folders", {"name": name}))
    print(f"[folder] created '{name}' -> {created.get('id')}")
    return str(created.get("id"))


def move(client: N8NClient, wf: Dict[str, Any], folder_id: str, export_json: Dict[str, Any], dry_run: bool) -> str:
    wid, name = str(wf["id"]), wf["name"]
    existing = client.get_workflow(wid)
    if existing.get("parentFolderId") == folder_id:
        print(f"[folder ok] {name}")
        return "ok"
    dropped = sorted(k for k in (existing.get("settings") or {}) if k not in ALLOWED_SETTINGS_FIELDS)
    if dropped:
        print(f"WARNING [skip] {name}: settings {dropped} would be lost in an API PUT — move it in the n8n UI")
        return "skip"
    payload = build_payload(export_json, existing, wid)
    payload["parentFolderId"] = folder_id
    if dry_run:
        print(f"[move] {name} -> folder (dry run)")
        return "dry"
    try:
        client.update_workflow(wid, payload)
    except SystemExit:
        print(f"WARNING [unmoved] {name}: the API refused parentFolderId on PUT — move it in the n8n UI")
        return "refused"
    if client.get_workflow(wid).get("parentFolderId") == folder_id:
        print(f"[moved] {name}")
        return "moved"
    print(f"WARNING [unmoved] {name}: PUT accepted but parentFolderId did not change — move it in the n8n UI")
    return "ignored"


# ---------------------------------------------------------------- main

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", default="etsy-design-system-manifest.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    base_url, api_key = os.environ.get("N8N_BASE_URL"), os.environ.get("N8N_API_KEY")
    if not base_url or not api_key:
        fail("N8N_BASE_URL and N8N_API_KEY are required")
    manifest = json.load(open(args.manifest, encoding="utf-8"))
    block = manifest.get("folder") or {}
    if not (block.get("name") or block.get("tag")):
        print("no folder/tag block in the manifest; nothing to do")
        return
    managed = manifest.get("workflows", [])
    names = [w.get("live_name") or w["name"] for w in managed] + list(block.get("also_include", []))
    exports = {(w.get("live_name") or w["name"]): w.get("file") for w in managed}

    client = N8NClient(base_url, api_key)
    live = {wf.get("name"): wf for wf in client.list_workflows()}
    tag_id = ensure_tag(client, block["tag"], args.dry_run) if block.get("tag") else None
    folder_id = None
    if block.get("name"):
        project_id = block.get("project_id")
        if not project_id:
            print("WARNING [folder] no project_id in the manifest (listing projects needs an enterprise "
                  "license) — read it from the n8n URL of the project and add folder.project_id")
        else:
            folder_id = ensure_folder(client, str(project_id), block["name"], args.dry_run)

    tag_results: Dict[str, int] = {}
    folder_results: Dict[str, int] = {}
    for name in names:
        wf = live.get(name)
        if not wf:
            print(f"WARNING [missing] {name}: not on this n8n")
            tag_results["missing"] = tag_results.get("missing", 0) + 1
            continue
        if tag_id:
            verdict = apply_tag(client, wf, tag_id, args.dry_run)
            tag_results[verdict] = tag_results.get(verdict, 0) + 1
        if folder_id:
            export_json: Dict[str, Any] = {}
            export_file = exports.get(name)
            if export_file and Path(export_file).exists():
                export_json = json.load(open(export_file, encoding="utf-8"))
            verdict = move(client, wf, folder_id, export_json, args.dry_run)
            folder_results[verdict] = folder_results.get(verdict, 0) + 1
    fmt = lambda d: ", ".join(f"{k}={v}" for k, v in sorted(d.items())) or "n/a"
    print(f"summary: tag[{fmt(tag_results)}] folder[{fmt(folder_results)}]")


if __name__ == "__main__":
    main()
