"""DAG validation: schema checks, duplicate names, missing/circular dependencies."""

from __future__ import annotations

from typing import Any

from fabric_metadata_dags.schema import (
    VALID_ACTIVITY_KEYS,
    VALID_DEFAULTS_KEYS,
    VALID_PIPELINE_KEYS,
    VALID_SETTINGS_KEYS,
)


def validate_pipeline_schema(pipeline: dict[str, Any]) -> None:
    """Validate the top-level pipeline dict for unknown keys.

    Checks the ``pipeline``, ``settings``, ``defaults``, and each entry in
    ``activities`` against the canonical key sets defined in
    :mod:`meta_driven_dags.schema`.  Raises :class:`ValueError` listing every
    unknown key found, so users get a single actionable error message rather
    than one error per key.

    Args:
        pipeline: The raw dict loaded directly from the YAML file.

    Raises:
        ValueError: If any section contains keys not present in the schema.
    """
    errors: list[str] = []

    # Top-level keys
    unknown_top = set(pipeline.keys()) - VALID_PIPELINE_KEYS
    if unknown_top:
        errors.append(f"Unknown top-level pipeline key(s): {_fmt(unknown_top)}")

    # settings block
    settings = pipeline.get("settings", {})
    unknown_settings = set(settings.keys()) - VALID_SETTINGS_KEYS
    if unknown_settings:
        errors.append(f"Unknown settings key(s): {_fmt(unknown_settings)}")

    # defaults block
    defaults = pipeline.get("defaults", {})
    unknown_defaults = set(defaults.keys()) - VALID_DEFAULTS_KEYS
    if unknown_defaults:
        errors.append(f"Unknown defaults key(s): {_fmt(unknown_defaults)}")

    # activity entries
    for i, activity in enumerate(pipeline.get("activities", [])):
        name = activity.get("name", f"<activity #{i}>")
        unknown_act = set(activity.keys()) - VALID_ACTIVITY_KEYS
        if unknown_act:
            errors.append(f'Unknown key(s) in activity "{name}": {_fmt(unknown_act)}')

    if errors:
        raise ValueError(
            "Pipeline schema validation failed:\n"
            + "\n".join(f"  • {e}" for e in errors)
        )


def validate_dag(activities: list[dict[str, Any]]) -> None:
    """Validate the activity list for structural correctness.

    Checks performed (in order):
      1. **Duplicate names** — every activity must have a unique name.
      2. **Missing dependencies** — every referenced dependency must resolve
         to an existing activity name.
      3. **Circular dependencies** — the dependency graph must be acyclic.

    Args:
        activities: List of (optionally resolved) activity dicts, each having
            at least a ``name`` key and an optional ``dependencies`` list.

    Raises:
        ValueError: On the first structural error found, with a human-readable
            description that includes the offending names/paths.
    """
    _check_duplicate_names(activities)
    _check_missing_dependencies(activities)
    _check_circular_dependencies(activities)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fmt(keys: set[str]) -> str:
    """Format a set of key names as a sorted, quoted, comma-separated string."""
    return ", ".join(f'"{k}"' for k in sorted(keys))


def _check_duplicate_names(activities: list[dict[str, Any]]) -> None:
    names = [a["name"] for a in activities]
    seen: set[str] = set()
    duplicates: list[str] = []
    for name in names:
        if name in seen:
            duplicates.append(name)
        seen.add(name)
    if duplicates:
        dup_list = ", ".join(f'"{d}"' for d in duplicates)
        raise ValueError(f"Duplicate activity name(s) detected: {dup_list}")


def _check_missing_dependencies(activities: list[dict[str, Any]]) -> None:
    known = {a["name"] for a in activities}
    for activity in activities:
        for dep in activity.get("dependencies", []):
            if dep not in known:
                raise ValueError(
                    f'Activity "{activity["name"]}" depends on "{dep}", '
                    f"but no such activity exists in this pipeline."
                )


def _check_circular_dependencies(activities: list[dict[str, Any]]) -> None:
    """DFS with 3-state colouring to detect back-edges (cycles).

    States:
      - UNVISITED (0): not yet reached
      - VISITING  (1): on the current DFS stack
      - VISITED   (2): fully explored, no cycle from here
    """
    UNVISITED, VISITING, VISITED = 0, 1, 2

    graph: dict[str, list[str]] = {
        a["name"]: list(a.get("dependencies", [])) for a in activities
    }
    state: dict[str, int] = {name: UNVISITED for name in graph}
    stack: list[str] = []  # tracks the current DFS path for error reporting

    def dfs(node: str) -> None:
        state[node] = VISITING
        stack.append(node)
        for dep in graph[node]:
            if state[dep] == VISITING:
                # Found a back-edge → report the cycle.
                cycle_start = stack.index(dep)
                cycle_path = " → ".join(stack[cycle_start:] + [dep])
                raise ValueError(f"Circular dependency detected: {cycle_path}")
            if state[dep] == UNVISITED:
                dfs(dep)
        stack.pop()
        state[node] = VISITED

    for name in graph:
        if state[name] == UNVISITED:
            dfs(name)
