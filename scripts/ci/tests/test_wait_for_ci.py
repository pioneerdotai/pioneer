from __future__ import annotations

import argparse
from pathlib import Path
import sys
import unittest


CI_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CI_DIR))

import wait_for_ci as waiter  # noqa: E402


SHA = "a" * 40


def workflow_run(
    *,
    run_id: int = 1,
    run_number: int = 1,
    status: str = "completed",
    conclusion: str | None = "success",
    sha: str = SHA,
    branch: str = "main",
    event: str = "push",
) -> dict:
    return {
        "id": run_id,
        "run_number": run_number,
        "run_attempt": 1,
        "head_sha": sha,
        "head_branch": branch,
        "event": event,
        "status": status,
        "conclusion": conclusion,
        "html_url": f"https://github.example/actions/runs/{run_id}",
        "created_at": f"2026-08-05T00:00:{run_number:02d}Z",
    }


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class WaitForCiTests(unittest.TestCase):
    def test_workflow_url_is_exact_sha_branch_and_event_scoped(self) -> None:
        url = waiter.workflow_runs_url(
            api_url="https://api.github.example/",
            repository="pioneer/app",
            workflow="ci.yml",
            sha=SHA,
            branch="main",
            event="push",
        )
        self.assertEqual(
            url,
            "https://api.github.example/repos/pioneer/app/actions/workflows/ci.yml/runs"
            f"?head_sha={SHA}&branch=main&event=push&exclude_pull_requests=true&per_page=100",
        )

    def test_invalid_sha_is_rejected_before_lookup(self) -> None:
        with self.assertRaisesRegex(waiter.CiWaitError, "40 hexadecimal"):
            waiter.workflow_runs_url(
                api_url="https://api.github.com",
                repository="pioneer/app",
                workflow="ci.yml",
                sha="main",
                branch="main",
                event="push",
            )

    def test_non_finite_timeout_is_rejected(self) -> None:
        with self.assertRaisesRegex(argparse.ArgumentTypeError, "greater than zero"):
            waiter.positive_number("nan")

    def test_latest_exact_run_ignores_other_sha_branch_and_event(self) -> None:
        selected = waiter.latest_exact_run(
            [
                workflow_run(run_id=1, run_number=1, sha="b" * 40),
                workflow_run(run_id=2, run_number=2, branch="release"),
                workflow_run(run_id=3, run_number=3, event="workflow_dispatch"),
                workflow_run(run_id=4, run_number=4),
                workflow_run(run_id=5, run_number=5),
            ],
            sha=SHA,
            branch="main",
            event="push",
        )
        self.assertIsNotNone(selected)
        self.assertEqual(selected["id"], 5)

    def test_waits_for_missing_and_running_ci_then_accepts_success(self) -> None:
        responses = iter(
            [
                [],
                [workflow_run(status="queued", conclusion=None)],
                [workflow_run(status="in_progress", conclusion=None)],
                [workflow_run()],
            ]
        )
        clock = FakeClock()
        logs: list[str] = []
        selected = waiter.wait_for_successful_run(
            lambda: next(responses),
            sha=SHA,
            branch="main",
            event="push",
            timeout_seconds=10,
            poll_seconds=1,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            log=logs.append,
        )
        self.assertEqual(selected["conclusion"], "success")
        self.assertEqual(clock.now, 3)
        self.assertIn(f"Exact-SHA CI succeeded for {SHA}", logs)

    def test_transient_api_error_is_retried(self) -> None:
        responses: list[object] = [waiter.RetryableApiError("temporary"), [workflow_run()]]
        clock = FakeClock()

        def fetch() -> list[dict]:
            response = responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response

        selected = waiter.wait_for_successful_run(
            fetch,
            sha=SHA,
            branch="main",
            event="push",
            timeout_seconds=10,
            poll_seconds=1,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            log=lambda _: None,
        )
        self.assertEqual(selected["id"], 1)

    def test_failed_ci_blocks_release_immediately(self) -> None:
        with self.assertRaisesRegex(waiter.CiWaitError, "conclusion='failure'"):
            waiter.wait_for_successful_run(
                lambda: [workflow_run(conclusion="failure")],
                sha=SHA,
                branch="main",
                event="push",
                timeout_seconds=10,
                poll_seconds=1,
                log=lambda _: None,
            )

    def test_newer_failed_run_cannot_reuse_older_success(self) -> None:
        with self.assertRaisesRegex(waiter.CiWaitError, "conclusion='failure'"):
            waiter.wait_for_successful_run(
                lambda: [
                    workflow_run(run_id=1, run_number=1),
                    workflow_run(run_id=2, run_number=2, conclusion="failure"),
                ],
                sha=SHA,
                branch="main",
                event="push",
                timeout_seconds=10,
                poll_seconds=1,
                log=lambda _: None,
            )

    def test_unknown_status_fails_closed(self) -> None:
        with self.assertRaisesRegex(waiter.CiWaitError, "unsupported status"):
            waiter.wait_for_successful_run(
                lambda: [workflow_run(status="mystery", conclusion=None)],
                sha=SHA,
                branch="main",
                event="push",
                timeout_seconds=10,
                poll_seconds=1,
                log=lambda _: None,
            )

    def test_missing_ci_times_out(self) -> None:
        clock = FakeClock()
        with self.assertRaisesRegex(waiter.CiWaitError, "timed out"):
            waiter.wait_for_successful_run(
                lambda: [],
                sha=SHA,
                branch="main",
                event="push",
                timeout_seconds=2,
                poll_seconds=1,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
                log=lambda _: None,
            )

    def test_malformed_api_payload_is_rejected(self) -> None:
        with self.assertRaisesRegex(waiter.CiWaitError, "workflow_runs"):
            waiter.parse_workflow_runs(b'{"workflow_runs": {}}')


if __name__ == "__main__":
    unittest.main()
