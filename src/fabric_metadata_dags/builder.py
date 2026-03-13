"""Convert resolved pipeline metadata into a Fabric-format DAG dictionary."""

from __future__ import annotations

from typing import Any


def build_dag(
    pipeline: dict[str, Any],
    resolved_activities: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the Fabric ``runMultiple`` DAG dict from resolved metadata.

    The returned structure matches the format expected by::

        mssparkutils.notebook.runMultiple(DAG, ...)

    Top-level keys ``timeoutInSeconds`` and ``concurrency`` are sourced from
    ``pipeline["settings"]``; activity-level fields come from the already-
    resolved activity dicts.

    Args:
        pipeline: The raw (or partially processed) pipeline dict as loaded
            from YAML, used to read ``settings``.
        resolved_activities: Activities with all inheritable keys resolved via
            :func:`fabric_metadata_dags.config.resolve_activity`.

    Returns:
        A ready-to-serialise DAG dictionary.
    """
    settings: dict[str, Any] = pipeline.get("settings", {})

    fabric_activities: list[dict[str, Any]] = []
    for act in resolved_activities:
        fabric_act: dict[str, Any] = {
            "name": act["name"],
            "path": act["path"],
        }

        # Inheritable scalar fields — always present after resolution.
        fabric_act["retry"] = act["retry"]
        fabric_act["retryIntervalInSeconds"] = act["retryIntervalInSeconds"]
        fabric_act["timeoutPerCellInSeconds"] = act["timeoutPerCellInSeconds"]

        # Optional structural fields — include only when present and non-empty.
        if act.get("args"):
            fabric_act["args"] = act["args"]
        if act.get("workspace"):
            fabric_act["workspace"] = act["workspace"]
        if act.get("dependencies"):
            fabric_act["dependencies"] = act["dependencies"]

        fabric_activities.append(fabric_act)

    dag: dict[str, Any] = {
        "activities": fabric_activities,
    }

    if "timeoutInSeconds" in settings:
        dag["timeoutInSeconds"] = settings["timeoutInSeconds"]
    if "concurrency" in settings:
        dag["concurrency"] = settings["concurrency"]
    if "refreshInterval" in settings:
        dag["refreshInterval"] = settings["refreshInterval"]

    return dag
