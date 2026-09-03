#!/usr/bin/env python3
"""Keep the Etsy Design System workflows together in one n8n folder.

Runs on the self-hosted runner right after the Etsy Design System deploy, with the same
N8N_BASE_URL / N8N_API_KEY. It creates the folder named in the manifest's "folder" block when it
is missing and moves every listed workflow (the managed entries plus "also_include") into it.
A move is a PUT of the workflow's own content plus parentFolderId, verified with a GET afterwards.
A workflow whose settings would be narrowed by the API whitelist is never touched; it is reported
as a WARNING so it can be moved by hand in the UI. Nothing here edits the shared deploy script:
its client and whitelist are imported, not changed.
"""
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from deploy_workflows_to_n8n import (  # noqa: E402
    ALLOWED_SETTINGS_FIELDS, N8NClient, build_payload, fail,
)


def _items(data: Any) -> list:
    if isinstance(data, dict) and "data" in data:
        return data["data"] or []
    return data or []


def personal_project(client: N8NClient) -> Dict[str, Any]:
    projects = _items(client._request("GET", "/api/v1/projects?limit=50"))
    for project in projects:
        if project.get("type") == "personal":
            return project
    if projects:
        return projects[0]
    fail("no project is visible to this API key")


def ensure_folder(client: N8NClient, project_id: str, name: str, dry_run: bool) -> Optional[str]:
    for folder in _items(client._request("GET", f"/api/v1/projects/{project_id}/folders?limit=100")):
        if folder.get("name") == name:
            print(f"[folder] '{name}' exists -> {folder.get('id')}")
            return str(folder.get("id"))
    if dry_run:
        print(f"[folder] '{name}' would be created")
        return None
    created = client._request("POST", f"/api/v1/projects/{project_id}/folders", {"name": name})
    if isinstance(created, dict) and "data" in created:
        created = created["data"]
    print(f"[folder] created '{name}' -> {created.get('id')}")
    return str(created.get("id"))


def move(client: N8NClient, wf: Dict[str, Any], folder_id: str, export_json: Dict[str, Any], dry_run: bool) -> str:
    wid, name = str(wf["id"]), wf["name"]
    existing = client.get_workflow(wid)
    if existing.get("parentFolderId") == folder_id:
        print(f"[ok] {name} is already in the folder")
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
        print(f"[move] {name} -> folder")
        return "moved"
    print(f"WARNING [unmoved] {name}: PUT accepted but parentFolderId did not change — move it in the n8n UI")
    return "ignored"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", default="etsy-design-system-manifest.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    base_url, api_key = os.environ.get("N8N_BASE_URL"), os.environ.get("N8N_API_KEY")
    if not base_url or not api_key:
        fail("N8N_BASE_URL and N8N_API_KEY are required")
    manifest = json.load(open(args.manifest, encoding="utf-8"))
    folder = manifest.get("folder") or {}
    if not folder.get("name"):
        print("no folder block in the manifest; nothing to do")
        return
    managed = manifest.get("workflows", [])
    names = [w.get("live_name") or w["name"] for w in managed] + list(folder.get("also_include", []))
    exports = {(w.get("live_name") or w["name"]): w.get("file") for w in managed}

    client = N8NClient(base_url, api_key)
    folder_id = ensure_folder(client, str(personal_project(client)["id"]), folder["name"], args.dry_run)
    live = {wf.get("name"): wf for wf in client.list_workflows()}
    results: Dict[str, str] = {}
    for name in names:
        wf = live.get(name)
        if not wf:
            print(f"WARNING [missing] {name}: not on this n8n")
            results[name] = "missing"
            continue
        export_json: Dict[str, Any] = {}
        export_file = exports.get(name)
        if export_file and Path(export_file).exists():
            export_json = json.load(open(export_file, encoding="utf-8"))
        if folder_id is None:
            print(f"[move] {name} -> folder (dry run; folder not created yet)")
            results[name] = "dry"
            continue
        results[name] = move(client, wf, folder_id, export_json, args.dry_run)
    counts: Dict[str, int] = {}
    for verdict in results.values():
        counts[verdict] = counts.get(verdict, 0) + 1
    print("summary:", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))


if __name__ == "__main__":
    main()
