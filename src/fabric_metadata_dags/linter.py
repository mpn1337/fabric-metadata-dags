"""Linting rules for pipeline YAML metadata.

Linting is distinct from schema validation:
  - **Validation** catches structural errors that *block* notebook generation
    (unknown keys, missing dependencies, cycles).
  - **Linting** detects suspicious but technically valid configurations that
    *may* cause problems at runtime.  All results are advisory warnings that
    never block generation.

Usage::

    from fabric_metadata_dags.linter import lint_pipeline

    warnings = lint_pipeline(pipeline)
    for w in warnings:
        print(w.severity.value, w.code, w.message)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from fabric_metadata_dags.config import FRAMEWORK_DEFAULTS


class LintSeverity(Enum):
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass(frozen=True)
class LintWarning:
    severity: LintSeverity
    code: str
    message: str
    activity_name: str | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def lint_pipeline(pipeline: dict[str, Any]) -> list[LintWarning]:
    """Inspect *pipeline* for suspicious configurations.

    Args:
        pipeline: Raw or partially-resolved pipeline dict loaded from YAML.

    Returns:
        A (possibly empty) list of :class:`LintWarning` instances ordered by
        activity position, then by rule code.
    """
    warnings: list[LintWarning] = []

    settings: dict[str, Any] = pipeline.get("settings", {})
    defaults: dict[str, Any] = pipeline.get("defaults", {})
    activities: list[dict[str, Any]] = pipeline.get("activities", [])

    warnings.extend(_check_settings(settings))
    warnings.extend(_check_activities(activities, defaults))

    return warnings


# ---------------------------------------------------------------------------
# Rule implementations
# ---------------------------------------------------------------------------


def _check_settings(settings: dict[str, Any]) -> list[LintWarning]:
    results: list[LintWarning] = []

    # W003 — high concurrency may exhaust the Spark pool
    concurrency = settings.get("concurrency")
    if concurrency is not None and concurrency > 50:
        results.append(
            LintWarning(
                severity=LintSeverity.WARNING,
                code="W003",
                message=(
                    f"concurrency is {concurrency} — values above 50 may exhaust "
                    "the Spark pool capacity."
                ),
            )
        )

    # W004 — no timeout means a hung notebook runs forever
    if "timeoutInSeconds" not in settings:
        results.append(
            LintWarning(
                severity=LintSeverity.WARNING,
                code="W004",
                message=(
                    "No timeoutInSeconds set in settings — a hung notebook will "
                    "run indefinitely."
                ),
            )
        )

    # W005 — explicit zero has the same effect as no timeout
    elif settings.get("timeoutInSeconds") == 0:
        results.append(
            LintWarning(
                severity=LintSeverity.WARNING,
                code="W005",
                message=(
                    "timeoutInSeconds is 0 — treated as no timeout; a hung "
                    "notebook will run indefinitely."
                ),
            )
        )

    return results


def _check_activities(
    activities: list[dict[str, Any]],
    pipeline_defaults: dict[str, Any],
) -> list[LintWarning]:
    results: list[LintWarning] = []
    seen_paths: dict[str, str] = {}  # path → first activity name

    for activity in activities:
        name: str = activity.get("name", "<unnamed>")

        # Resolve effective values using the same 3-tier priority as config.py
        # (without importing resolve_activity to keep linting self-contained).
        def effective(key: str) -> Any:
            if key in activity:
                return activity[key]
            if key in pipeline_defaults:
                return pipeline_defaults[key]
            return FRAMEWORK_DEFAULTS.get(key)

        retry = effective("retry")
        retry_interval = effective("retryIntervalInSeconds")
        timeout_per_cell = effective("timeoutPerCellInSeconds")
        path: str | None = activity.get("path")

        # W001 — retry of 0 means one transient error fails the pipeline
        if retry == 0:
            results.append(
                LintWarning(
                    severity=LintSeverity.WARNING,
                    code="W001",
                    message=(
                        "retry resolves to 0 — a single transient failure will "
                        "fail the entire pipeline."
                    ),
                    activity_name=name,
                )
            )

        # W002 — retryIntervalInSeconds of 0 means instant retry with no backoff
        if retry_interval == 0 and retry != 0:
            results.append(
                LintWarning(
                    severity=LintSeverity.WARNING,
                    code="W002",
                    message=(
                        "retryIntervalInSeconds is 0 — retries will fire "
                        "immediately with no backoff."
                    ),
                    activity_name=name,
                )
            )

        # W006 — duplicate path across activities (likely copy-paste error)
        if path is not None:
            if path in seen_paths:
                results.append(
                    LintWarning(
                        severity=LintSeverity.WARNING,
                        code="W006",
                        message=(
                            f'Notebook path "{path}" is also used by activity '
                            f'"{seen_paths[path]}" — possible copy-paste error.'
                        ),
                        activity_name=name,
                    )
                )
            else:
                seen_paths[path] = name

        # W007 — cell timeout is very short and may fire on normal slow cells
        if timeout_per_cell is not None and timeout_per_cell < 30:
            results.append(
                LintWarning(
                    severity=LintSeverity.WARNING,
                    code="W007",
                    message=(
                        f"timeoutPerCellInSeconds is {timeout_per_cell} — values "
                        "below 30 s may time out on normal slow cells."
                    ),
                    activity_name=name,
                )
            )

    return results
