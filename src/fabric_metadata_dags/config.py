"""Framework defaults and 3-tier configuration resolution.

Resolution priority (highest → lowest):
  1. Activity-level settings
  2. Pipeline-level defaults (``defaults:`` section in YAML)
  3. Framework defaults (defined below)
"""

from __future__ import annotations

from typing import Any

# Keys that participate in the 3-tier inheritance chain.
# Structural fields (name, path, args, dependencies) are NOT inherited.
INHERITABLE_KEYS: tuple[str, ...] = (
    "retry",
    "retryIntervalInSeconds",
    "timeoutPerCellInSeconds",
)

# Lowest-priority defaults baked into the framework.
# Values match the mssparkutils.notebook.runMultiple() spec:
#   retry                   → 0
#   retryIntervalInSeconds  → 0 s
#   timeoutPerCellInSeconds → 90 s
FRAMEWORK_DEFAULTS: dict[str, Any] = {
    "retry": 0,
    "retryIntervalInSeconds": 0,
    "timeoutPerCellInSeconds": 90,
}


def resolve_activity(
    activity: dict[str, Any],
    pipeline_defaults: dict[str, Any],
) -> dict[str, Any]:
    """Return a fully-resolved copy of *activity*.

    Inheritable fields are merged in priority order::

        activity value
          → pipeline default
          → framework default

    Structural fields (``name``, ``path``, ``args``, ``dependencies``) are
    passed through unchanged.

    Args:
        activity: Raw activity dict from the YAML metadata.
        pipeline_defaults: The ``defaults:`` block from the pipeline YAML
            (may be empty).

    Returns:
        A new dict with all inheritable keys populated.
    """
    resolved: dict[str, Any] = {}

    # Carry over structural fields as-is.
    # ``workspace`` is optional and not subject to inheritance — it is
    # either set on the activity or absent.
    for key in ("name", "path", "args", "dependencies", "workspace"):
        if key in activity:
            resolved[key] = activity[key]

    # Apply 3-tier inheritance for each inheritable key.
    for key in INHERITABLE_KEYS:
        if key in activity:
            resolved[key] = activity[key]
        elif key in pipeline_defaults:
            resolved[key] = pipeline_defaults[key]
        else:
            resolved[key] = FRAMEWORK_DEFAULTS[key]

    return resolved
