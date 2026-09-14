#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sanida_fiscal.human_review_v1 import render_review_markdown


CURRENT_MARKER = "<!-- sanida-fiscal-review-current -->"
KEY_PREFIX = "<!-- sanida-fiscal-review-key:"


class GitHubIssueError(RuntimeError):
    pass


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _api(method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repository = os.environ.get("GITHUB_REPOSITORY", "").strip()
    api_root = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    if not token or not repository:
        raise GitHubIssueError("GITHUB_TOKEN and GITHUB_REPOSITORY are required")
    url = f"{api_root}/repos/{repository}{path}"
    body = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "sanida-fiscal-human-review-v1",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=30) as response:
            raw = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise GitHubIssueError(f"GitHub API {method} {path} failed: {exc.code} {detail}") from exc
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def _list_open_issues() -> list[dict[str, Any]]:
    query = urlencode({"state": "open", "per_page": 100})
    payload = _api("GET", f"/issues?{query}")
    if not isinstance(payload, list):
        raise GitHubIssueError("unexpected GitHub issues response")
    return [item for item in payload if isinstance(item, dict) and "pull_request" not in item]


def _issue_key(issue: dict[str, Any]) -> str | None:
    body = issue.get("body")
    if not isinstance(body, str):
        return None
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith(KEY_PREFIX) and stripped.endswith(" -->"):
            return stripped[len(KEY_PREFIX) : -4].strip()
    return None


def _regression(args: argparse.Namespace) -> tuple[str | None, str | None]:
    if args.regression_result is None or not args.regression_result.exists():
        return None, None
    data = _load(args.regression_result)
    return str(data.get("status") or "UNKNOWN"), str(data.get("summary") or "")


def _close_stale(issues: list[dict[str, Any]], current_key: str) -> None:
    for issue in issues:
        body = issue.get("body")
        if not isinstance(body, str) or CURRENT_MARKER not in body:
            continue
        key = _issue_key(issue)
        if key and key != current_key:
            number = issue.get("number")
            if isinstance(number, int):
                _api(
                    "PATCH",
                    f"/issues/{number}",
                    {
                        "state": "closed",
                        "state_reason": "not_planned",
                        "title": f"[STALE] {issue.get('title', 'Fiscal review')}",
                    },
                )


def _assignee_logins(issue: dict[str, Any]) -> set[str]:
    values = issue.get("assignees")
    if not isinstance(values, list):
        return set()
    return {
        str(item.get("login"))
        for item in values
        if isinstance(item, dict) and isinstance(item.get("login"), str)
    }


def _same_issue_payload(existing: dict[str, Any], *, title: str, body: str, owner: str) -> bool:
    if existing.get("title") != title or existing.get("body") != body:
        return False
    if owner and owner not in _assignee_logins(existing):
        return False
    return True


def upsert(packet_path: Path, regression_result: Path | None) -> int:
    packet = _load(packet_path)
    review_key = str(packet.get("review_key") or "")
    if len(review_key) != 64:
        raise GitHubIssueError("review packet has invalid review_key")

    class Args:
        pass

    args = Args()
    args.regression_result = regression_result
    status, summary = _regression(args)
    body = render_review_markdown(packet, regression_status=status, regression_summary=summary)
    title = f"[Fiscal review] {packet.get('review_kind')} {review_key[:12]}"
    issues = _list_open_issues()
    _close_stale(issues, review_key)

    existing = next((issue for issue in issues if _issue_key(issue) == review_key), None)
    owner = os.environ.get("GITHUB_REPOSITORY_OWNER", "").strip()
    payload: dict[str, Any] = {"title": title, "body": body}
    if owner:
        payload["assignees"] = [owner]

    if existing is not None:
        number = existing.get("number")
        if not isinstance(number, int):
            raise GitHubIssueError("existing review Issue has no number")
        if _same_issue_payload(existing, title=title, body=body, owner=owner):
            print(f"review_issue_number={number}")
            print(f"review_issue_url={existing.get('html_url', '')}")
            print("review_issue_action=REUSED_UNCHANGED")
            return number
        updated = _api("PATCH", f"/issues/{number}", payload)
        print(f"review_issue_number={number}")
        print(f"review_issue_url={updated.get('html_url') if isinstance(updated, dict) else ''}")
        print("review_issue_action=UPDATED")
        return number

    created = _api("POST", "/issues", payload)
    if not isinstance(created, dict) or not isinstance(created.get("number"), int):
        raise GitHubIssueError("GitHub did not return created review Issue number")
    number = int(created["number"])
    print(f"review_issue_number={number}")
    print(f"review_issue_url={created.get('html_url', '')}")
    print("review_issue_action=CREATED")
    return number


def close_review(review_key: str, resolution: str) -> None:
    issues = _list_open_issues()
    existing = next((issue for issue in issues if _issue_key(issue) == review_key), None)
    if existing is None:
        print("No open review Issue matched the published review_key.")
        return
    number = existing.get("number")
    if not isinstance(number, int):
        raise GitHubIssueError("review Issue has no number")
    _api("POST", f"/issues/{number}/comments", {"body": f"✅ {resolution}"})
    _api("PATCH", f"/issues/{number}", {"state": "closed", "state_reason": "completed"})
    print(f"closed_review_issue={number}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create/update/close GitHub Issue for Fiscal Contract human review")
    parser.add_argument("--packet", type=Path, default=ROOT / "state/fiscal-release-v12-review.json")
    parser.add_argument("--regression-result", type=Path, default=None)
    parser.add_argument("--close-review-key", default=None)
    parser.add_argument("--resolution", default="Candidato aprovado e publicação concluída.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.close_review_key:
        close_review(args.close_review_key.strip().lower(), args.resolution)
        return 0
    upsert(args.packet, args.regression_result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
