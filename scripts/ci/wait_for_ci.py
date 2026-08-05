#!/usr/bin/env python3
"""Wait for a successful CI workflow run for one exact release commit."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from collections.abc import Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


DEFAULT_API_URL = "https://api.github.com"
DEFAULT_API_VERSION = "2026-03-10"
DEFAULT_POLL_SECONDS = 30.0
DEFAULT_TIMEOUT_SECONDS = 9_000.0
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
ACTIVE_STATUSES = {"in_progress", "pending", "queued", "requested", "waiting"}
SHA_PATTERN = re.compile(r"[0-9a-fA-F]{40}")


class CiWaitError(RuntimeError):
    """The required CI run cannot safely authorize a release."""


class RetryableApiError(RuntimeError):
    """A transient GitHub API failure that may be retried until the deadline."""


def positive_number(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return value


def normalize_sha(raw: str) -> str:
    if SHA_PATTERN.fullmatch(raw) is None:
        raise CiWaitError("release SHA must be exactly 40 hexadecimal characters")
    return raw.lower()


def repository_path(repository: str) -> str:
    parts = repository.split("/")
    if len(parts) != 2 or not all(parts):
        raise CiWaitError("repository must use the exact owner/name form")
    return "/".join(quote(part, safe="") for part in parts)


def workflow_runs_url(
    *, api_url: str, repository: str, workflow: str, sha: str, branch: str, event: str
) -> str:
    if not workflow or "/" in workflow or "\\" in workflow:
        raise CiWaitError("workflow must be a workflow file name such as ci.yml")
    if not branch or not event:
        raise CiWaitError("branch and event must be non-empty")
    query = urlencode(
        {
            "head_sha": normalize_sha(sha),
            "branch": branch,
            "event": event,
            "exclude_pull_requests": "true",
            "per_page": 100,
        }
    )
    return (
        f"{api_url.rstrip('/')}/repos/{repository_path(repository)}"
        f"/actions/workflows/{quote(workflow, safe='')}/runs?{query}"
    )


def parse_workflow_runs(payload: bytes) -> list[dict[str, Any]]:
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CiWaitError("GitHub workflow-runs response is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise CiWaitError("GitHub workflow-runs response must be an object")
    runs = decoded.get("workflow_runs")
    if not isinstance(runs, list) or not all(isinstance(run, dict) for run in runs):
        raise CiWaitError("GitHub workflow-runs response has no valid workflow_runs list")
    return runs


def fetch_workflow_runs(
    url: str, *, token: str, api_version: str = DEFAULT_API_VERSION
) -> list[dict[str, Any]]:
    if not token.strip():
        raise CiWaitError("GITHUB_TOKEN is required to inspect the exact CI run")
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "pioneer-release-ci-gate",
            "X-GitHub-Api-Version": api_version,
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        if exc.code in {408, 429} or 500 <= exc.code <= 599:
            raise RetryableApiError(f"GitHub API temporarily returned HTTP {exc.code}") from exc
        raise CiWaitError(f"GitHub API rejected the CI lookup with HTTP {exc.code}") from exc
    except (TimeoutError, URLError) as exc:
        raise RetryableApiError(f"GitHub API request failed transiently: {exc}") from exc
    if len(payload) > MAX_RESPONSE_BYTES:
        raise CiWaitError("GitHub workflow-runs response exceeded the size limit")
    return parse_workflow_runs(payload)


def _integer_field(run: dict[str, Any], name: str) -> int:
    value = run.get(name)
    return value if isinstance(value, int) and not isinstance(value, bool) else -1


def latest_exact_run(
    runs: list[dict[str, Any]], *, sha: str, branch: str, event: str
) -> dict[str, Any] | None:
    expected_sha = normalize_sha(sha)
    candidates = [
        run
        for run in runs
        if isinstance(run.get("head_sha"), str)
        and run["head_sha"].lower() == expected_sha
        and run.get("head_branch") == branch
        and run.get("event") == event
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda run: (
            _integer_field(run, "run_number"),
            _integer_field(run, "run_attempt"),
            str(run.get("created_at", "")),
            _integer_field(run, "id"),
        ),
    )


def wait_for_successful_run(
    fetch_runs: Callable[[], list[dict[str, Any]]],
    *,
    sha: str,
    branch: str,
    event: str,
    timeout_seconds: float,
    poll_seconds: float,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    expected_sha = normalize_sha(sha)
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise CiWaitError("timeout and poll intervals must be greater than zero")

    deadline = monotonic() + timeout_seconds
    last_observation: tuple[Any, ...] | None = None
    while True:
        try:
            runs = fetch_runs()
        except RetryableApiError as exc:
            observation = ("transient-error", str(exc))
            if observation != last_observation:
                log(f"Waiting after transient GitHub API error: {exc}")
                last_observation = observation
        else:
            run = latest_exact_run(runs, sha=expected_sha, branch=branch, event=event)
            if run is None:
                observation = ("missing",)
                if observation != last_observation:
                    log(
                        "Waiting for CI workflow run "
                        f"event={event} branch={branch} sha={expected_sha}"
                    )
                    last_observation = observation
            else:
                run_id = run.get("id")
                status = run.get("status")
                conclusion = run.get("conclusion")
                run_url = run.get("html_url")
                observation = ("run", run_id, status, conclusion)
                if observation != last_observation:
                    log(
                        f"CI run id={run_id} status={status} conclusion={conclusion} "
                        f"url={run_url}"
                    )
                    last_observation = observation

                if status == "completed":
                    if conclusion == "success":
                        log(f"Exact-SHA CI succeeded for {expected_sha}")
                        return run
                    raise CiWaitError(
                        f"exact-SHA CI completed with conclusion={conclusion!r}; "
                        f"release is blocked ({run_url})"
                    )
                if status not in ACTIVE_STATUSES:
                    raise CiWaitError(
                        f"exact-SHA CI returned unsupported status={status!r}; release is blocked"
                    )

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise CiWaitError(
                f"timed out waiting for successful CI on exact SHA {expected_sha}"
            )
        sleep(min(poll_seconds, remaining))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True, help="GitHub owner/name")
    parser.add_argument("--workflow", default="ci.yml", help="CI workflow file name")
    parser.add_argument("--sha", required=True, help="exact 40-character release commit SHA")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--event", default="push")
    parser.add_argument("--api-url", default=os.environ.get("GITHUB_API_URL", DEFAULT_API_URL))
    parser.add_argument("--api-version", default=DEFAULT_API_VERSION)
    parser.add_argument(
        "--timeout-seconds", type=positive_number, default=DEFAULT_TIMEOUT_SECONDS
    )
    parser.add_argument("--poll-seconds", type=positive_number, default=DEFAULT_POLL_SECONDS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        sha = normalize_sha(args.sha)
        url = workflow_runs_url(
            api_url=args.api_url,
            repository=args.repository,
            workflow=args.workflow,
            sha=sha,
            branch=args.branch,
            event=args.event,
        )
        token = os.environ.get("GITHUB_TOKEN", "")
        wait_for_successful_run(
            lambda: fetch_workflow_runs(url, token=token, api_version=args.api_version),
            sha=sha,
            branch=args.branch,
            event=args.event,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
    except CiWaitError as exc:
        print(f"release CI prerequisite failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
