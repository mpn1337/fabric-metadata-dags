"""Tests for fabric_metadata_dags.linter."""

from __future__ import annotations

import pytest

from fabric_metadata_dags.linter import LintSeverity, LintWarning, lint_pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def codes(warnings: list[LintWarning]) -> list[str]:
    return [w.code for w in warnings]


def make_pipeline(
    *,
    concurrency: int | None = 10,
    timeout: int | None = 21600,
    defaults: dict | None = None,
    activities: list[dict] | None = None,
) -> dict:
    settings: dict = {}
    if concurrency is not None:
        settings["concurrency"] = concurrency
    if timeout is not None:
        settings["timeoutInSeconds"] = timeout

    pipeline: dict = {"pipeline": "test", "settings": settings}
    if defaults:
        pipeline["defaults"] = defaults
    if activities is not None:
        pipeline["activities"] = activities
    return pipeline


def make_activity(
    name: str = "act",
    path: str = "/nb/path",
    **kwargs,
) -> dict:
    return {"name": name, "path": path, **kwargs}


# ---------------------------------------------------------------------------
# Settings checks
# ---------------------------------------------------------------------------


class TestSettingsLint:
    def test_no_warnings_for_clean_settings(self):
        pipeline = make_pipeline(concurrency=10, timeout=21600, activities=[])
        assert lint_pipeline(pipeline) == []

    def test_w003_concurrency_above_50(self):
        pipeline = make_pipeline(concurrency=51, activities=[])
        assert "W003" in codes(lint_pipeline(pipeline))

    def test_w003_exactly_50_is_fine(self):
        pipeline = make_pipeline(concurrency=50, activities=[])
        assert "W003" not in codes(lint_pipeline(pipeline))

    def test_w004_missing_timeout(self):
        pipeline = make_pipeline(concurrency=10, timeout=None, activities=[])
        assert "W004" in codes(lint_pipeline(pipeline))

    def test_w005_zero_timeout(self):
        pipeline = make_pipeline(concurrency=10, timeout=0, activities=[])
        ws = lint_pipeline(pipeline)
        assert "W005" in codes(ws)
        assert "W004" not in codes(ws)

    def test_w004_w005_are_mutually_exclusive(self):
        # W004 fires when key is absent; W005 fires when key is 0 — never both
        pipeline = make_pipeline(timeout=None, activities=[])
        assert "W005" not in codes(lint_pipeline(pipeline))

        pipeline2 = make_pipeline(timeout=0, activities=[])
        assert "W004" not in codes(lint_pipeline(pipeline2))

    def test_warning_severity(self):
        pipeline = make_pipeline(concurrency=51, activities=[])
        for w in lint_pipeline(pipeline):
            assert w.severity == LintSeverity.WARNING


# ---------------------------------------------------------------------------
# Activity checks
# ---------------------------------------------------------------------------


class TestActivityLint:
    def test_w001_retry_zero_from_framework_default(self):
        """Framework default retry=0 should trigger W001."""
        pipeline = make_pipeline(activities=[make_activity()])
        assert "W001" in codes(lint_pipeline(pipeline))

    def test_w001_retry_zero_explicit(self):
        pipeline = make_pipeline(activities=[make_activity(retry=0)])
        assert "W001" in codes(lint_pipeline(pipeline))

    def test_w001_no_warning_when_retry_positive(self):
        pipeline = make_pipeline(activities=[make_activity(retry=1)])
        assert "W001" not in codes(lint_pipeline(pipeline))

    def test_w001_inherits_from_pipeline_defaults(self):
        """retry=1 in defaults should suppress W001."""
        pipeline = make_pipeline(
            defaults={"retry": 1},
            activities=[make_activity()],
        )
        assert "W001" not in codes(lint_pipeline(pipeline))

    def test_w001_activity_overrides_defaults(self):
        """Activity retry=0 wins over pipeline default retry=2."""
        pipeline = make_pipeline(
            defaults={"retry": 2},
            activities=[make_activity(retry=0)],
        )
        assert "W001" in codes(lint_pipeline(pipeline))

    def test_w002_zero_interval_with_retries(self):
        """retryIntervalInSeconds=0 when retry>0 should trigger W002."""
        pipeline = make_pipeline(
            activities=[make_activity(retry=2, retryIntervalInSeconds=0)]
        )
        assert "W002" in codes(lint_pipeline(pipeline))

    def test_w002_no_warning_when_retry_is_zero(self):
        """W002 is irrelevant when retry=0 — retries never happen."""
        pipeline = make_pipeline(
            activities=[make_activity(retry=0, retryIntervalInSeconds=0)]
        )
        assert "W002" not in codes(lint_pipeline(pipeline))

    def test_w002_no_warning_when_interval_positive(self):
        pipeline = make_pipeline(
            activities=[make_activity(retry=2, retryIntervalInSeconds=10)]
        )
        assert "W002" not in codes(lint_pipeline(pipeline))

    def test_w006_duplicate_path(self):
        acts = [
            make_activity("a", "/nb/same"),
            make_activity("b", "/nb/same"),
        ]
        pipeline = make_pipeline(activities=acts)
        ws = lint_pipeline(pipeline)
        assert "W006" in codes(ws)
        w = next(w for w in ws if w.code == "W006")
        assert w.activity_name == "b"

    def test_w006_unique_paths_no_warning(self):
        acts = [
            make_activity("a", "/nb/one"),
            make_activity("b", "/nb/two"),
        ]
        pipeline = make_pipeline(activities=acts)
        assert "W006" not in codes(lint_pipeline(pipeline))

    def test_w007_short_timeout(self):
        pipeline = make_pipeline(activities=[make_activity(timeoutPerCellInSeconds=10)])
        assert "W007" in codes(lint_pipeline(pipeline))

    def test_w007_exactly_30_is_fine(self):
        pipeline = make_pipeline(activities=[make_activity(timeoutPerCellInSeconds=30)])
        assert "W007" not in codes(lint_pipeline(pipeline))

    def test_w007_uses_pipeline_default(self):
        pipeline = make_pipeline(
            defaults={"timeoutPerCellInSeconds": 10},
            activities=[make_activity()],
        )
        assert "W007" in codes(lint_pipeline(pipeline))

    def test_activity_name_attached_to_warning(self):
        pipeline = make_pipeline(activities=[make_activity(name="my_act", retry=0)])
        ws = [w for w in lint_pipeline(pipeline) if w.code == "W001"]
        assert ws[0].activity_name == "my_act"

    def test_multiple_activities_reported_separately(self):
        acts = [
            make_activity("a", "/nb/a", retry=0),
            make_activity("b", "/nb/b", retry=0),
        ]
        pipeline = make_pipeline(activities=acts)
        w001s = [w for w in lint_pipeline(pipeline) if w.code == "W001"]
        assert len(w001s) == 2
        assert {w.activity_name for w in w001s} == {"a", "b"}

    def test_no_activities_no_warnings(self):
        pipeline = make_pipeline(activities=[])
        assert lint_pipeline(pipeline) == []


# ---------------------------------------------------------------------------
# Empty / minimal pipeline
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_pipeline_only_emits_settings_warnings(self):
        pipeline = {"pipeline": "empty"}
        ws = lint_pipeline(pipeline)
        # No activities → no activity warnings; missing timeout → W004
        assert "W004" in codes(ws)
        assert all(w.activity_name is None for w in ws)

    def test_no_duplicate_w004_and_w005(self):
        for timeout in (None, 0):
            pipeline = make_pipeline(timeout=timeout, activities=[])
            ws = lint_pipeline(pipeline)
            assert codes(ws).count("W004") + codes(ws).count("W005") == 1

    def test_lint_pipeline_returns_list(self):
        pipeline = make_pipeline(activities=[])
        result = lint_pipeline(pipeline)
        assert isinstance(result, list)
