"""Canonical schema for pipeline YAML metadata and Fabric DAG dictionaries.

Why ``frozenset`` constants instead of ``Enum``
------------------------------------------------
* We need **membership tests** (``key in VALID_ACTIVITY_KEYS``) and **set
  arithmetic** (``unknown = actual_keys - VALID_ACTIVITY_KEYS``), both of
  which are native to ``frozenset``.
* An ``Enum`` would force callers to write ``ActivityKey.RETRY.value`` instead
  of the plain string ``"retry"`` — adding boilerplate with no benefit for a
  string-keyed data format.
* A ``TypedDict`` provides static type hints but cannot be iterated at runtime
  for validation purposes.

Adding a new supported field
----------------------------
Update the relevant ``frozenset`` below and then handle the new field in
``config.py`` (if inheritable) and ``builder.py`` (if it should appear in the
generated DAG).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Valid keys per YAML section
# ---------------------------------------------------------------------------

#: Top-level keys expected in a pipeline YAML file.
VALID_PIPELINE_KEYS: frozenset[str] = frozenset(
    {"pipeline", "settings", "defaults", "activities"}
)

#: Valid keys inside the ``settings:`` block.
VALID_SETTINGS_KEYS: frozenset[str] = frozenset(
    {"concurrency", "timeoutInSeconds", "refreshInterval"}
)

#: Valid keys inside the ``defaults:`` block (inheritable fields only).
VALID_DEFAULTS_KEYS: frozenset[str] = frozenset(
    {"retry", "retryIntervalInSeconds", "timeoutPerCellInSeconds"}
)

#: Valid keys for each activity in the ``activities:`` list.
#:
#: Sourced from ``mssparkutils.notebook.help("runMultiple")``:
#:   - name                    required, must be unique
#:   - path                    required, notebook path
#:   - timeoutPerCellInSeconds optional, default 90 s
#:   - args                    optional, dict of notebook parameters
#:   - workspace               optional, target workspace name
#:   - retry                   optional, default 0
#:   - retryIntervalInSeconds  optional, default 0 s
#:   - dependencies            optional, list of upstream activity names
VALID_ACTIVITY_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "path",
        "timeoutPerCellInSeconds",
        "args",
        "workspace",
        "retry",
        "retryIntervalInSeconds",
        "dependencies",
    }
)
