#!/usr/bin/env python3
"""
Sync exported n8n workflow JSON files into a live n8n instance.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib import error, request


def fail(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


class N8NClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        allow_not_found: bool = False,
    ) -> Any:
        url = f"{self.base_url}{path}"
        headers = {
            "Accept": "application/json",
            "X-N8N-API-KEY": self.api_key,
        }
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(url, data=body, headers=headers, method=method)
        try:
            with request.urlopen(req) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else None
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            if allow_not_found and exc.code == 404:
                return None
            msg = f"{method} {url} -> HTTP {exc.code}"
            if raw:
                msg += f"\n{raw}"
            fail(msg)

    def list_workflows(self) -> List[Dict[str, Any]]:
        for path in ("/api/v1/workflows?limit=250", "/rest/workflows"):
            try:
                data = self._request("GET", path)
                if isinstance(data, dict) and "data" in data:
                    return data["data"]
                if isinstance(data, list):
                    return data
            except SystemExit:
                continue
        fail("Unable to list workflows from n8n using known endpoints")

    def get_workflow(self, workflow_id: str) -> Dict[str, Any]:
        for path in (f"/api/v1/workflows/{workflow_id}", f"/rest/workflows/{workflow_id}"):
            data = self._request("GET", path, allow_not_found=True)
            if data is not None:
                return data["data"] if isinstance(data, dict) and "data" in data else data
        fail(f"Workflow {workflow_id} not found")

    def update_workflow(self, workflow_id: str, payload: Dict[str, Any]) -> Any:
        attempts: List[Tuple[str, str]] = [
            ("PUT", f"/api/v1/workflows/{workflow_id}"),
            ("PATCH", f"/api/v1/workflows/{workflow_id}"),
            ("PATCH", f"/rest/workflows/{workflow_id}"),
        ]
        last_error = None
        for method, path in attempts:
            try:
                return self._request(method, path, payload)
            except SystemExit as exc:
                last_error = exc
        if last_error:
            raise last_error
        fail(f"Unable to update workflow {workflow_id}")


def resolve_live_workflow(
    client: N8NClient, workflow_id: str, live_name: str
) -> Tuple[str, Dict[str, Any]]:
    workflow_id = workflow_id.strip()
    if workflow_id:
        return workflow_id, client.get_workflow(workflow_id)

    matches = [wf for wf in client.list_workflows() if wf.get("name") == live_name]
    if not matches:
        fail(
            f"No live workflow matched live_name={live_name!r}. "
            "Fill n8n_workflow_id in workflow-manifest.json."
        )
    if len(matches) > 1:
        ids = ", ".join(str(wf.get("id")) for wf in matches)
        fail(
            f"Multiple live workflows matched live_name={live_name!r}: {ids}. "
            "Fill n8n_workflow_id in workflow-manifest.json."
        )
    workflow = matches[0]
    return str(workflow.get("id")), workflow


def build_payload(
    export_json: Dict[str, Any],
    existing_workflow: Dict[str, Any],
    workflow_id: str,
    activate_after_sync: bool,
) -> Dict[str, Any]:
    payload = dict(export_json)
    payload["id"] = workflow_id
    payload["active"] = activate_after_sync
    for field in ("versionId", "tags", "settings", "staticData", "parentFolderId"):
        if field not in payload and field in existing_workflow:
            payload[field] = existing_workflow[field]
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="workflow-manifest.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    base_url = os.environ.get("N8N_BASE_URL")
    api_key = os.environ.get("N8N_API_KEY")
    if not base_url:
        fail("Missing N8N_BASE_URL")
    if not api_key:
        fail("Missing N8N_API_KEY")

    repo_root = Path(__file__).resolve().parents[1]
    manifest = load_json((repo_root / args.manifest).resolve())

    defaults = manifest.get("defaults", {})
    workflows = manifest.get("workflows", [])
    if not workflows:
        fail("No workflows found in manifest")

    client = N8NClient(base_url=base_url, api_key=api_key)

    for workflow in workflows:
        export_path = repo_root / workflow["file"]
        if not export_path.exists():
            fail(f"Missing workflow export: {export_path}")

        export_json = load_json(export_path)
        live_name = workflow.get("live_name") or export_json.get("name") or workflow["name"]
        workflow_id = str(workflow.get("n8n_workflow_id") or "")
        activate_after_sync = bool(
            workflow.get("activate_after_sync", defaults.get("activate_after_sync", True))
        )

        resolved_id, existing = resolve_live_workflow(client, workflow_id, live_name)
        print(f"[resolve] {workflow['name']} -> {resolved_id} ({live_name})")
        if args.dry_run:
            continue

        payload = build_payload(
            export_json=export_json,
            existing_workflow=existing,
            workflow_id=resolved_id,
            activate_after_sync=activate_after_sync,
        )
        client.update_workflow(resolved_id, payload)
        print(f"[update] synced {workflow['file']}")


if __name__ == "__main__":
    main()
