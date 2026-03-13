"""Tests for fabric_metadata_dags.config — 3-tier configuration resolution."""

import pytest

from fabric_metadata_dags.config import FRAMEWORK_DEFAULTS, resolve_activity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _activity(name="step_a", path="/notebooks/step_a", **overrides):
    base = {"name": name, "path": path}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Priority tests
# ---------------------------------------------------------------------------


class TestResolutionPriority:
    def test_activity_override_beats_pipeline_default(self):
        act = _activity(retry=10)
        result = resolve_activity(act, {"retry": 5})
        assert result["retry"] == 10

    def test_activity_override_beats_framework_default(self):
        act = _activity(timeoutPerCellInSeconds=999)
        result = resolve_activity(act, {})
        assert result["timeoutPerCellInSeconds"] == 999

    def test_pipeline_default_beats_framework_default(self):
        act = _activity()
        result = resolve_activity(act, {"retryIntervalInSeconds": 60})
        assert result["retryIntervalInSeconds"] == 60

    def test_framework_default_used_when_nothing_else_set(self):
        act = _activity()
        result = resolve_activity(act, {})
        assert result["retry"] == FRAMEWORK_DEFAULTS["retry"]
        assert (
            result["retryIntervalInSeconds"]
            == FRAMEWORK_DEFAULTS["retryIntervalInSeconds"]
        )
        assert (
            result["timeoutPerCellInSeconds"]
            == FRAMEWORK_DEFAULTS["timeoutPerCellInSeconds"]
        )

    def test_all_three_tiers_respected_simultaneously(self):
        """Each inheritable key resolved from a different tier."""
        act = _activity(retry=7)  # activity level
        pipeline_defaults = {"retryIntervalInSeconds": 45}  # pipeline level
        # timeoutPerCellInSeconds falls through to framework
        result = resolve_activity(act, pipeline_defaults)
        assert result["retry"] == 7
        assert result["retryIntervalInSeconds"] == 45
        assert (
            result["timeoutPerCellInSeconds"]
            == FRAMEWORK_DEFAULTS["timeoutPerCellInSeconds"]
        )


# ---------------------------------------------------------------------------
# Structural fields
# ---------------------------------------------------------------------------


class TestStructuralFields:
    def test_name_and_path_always_present(self):
        act = _activity(name="my_notebook", path="/notebooks/my_notebook")
        result = resolve_activity(act, {})
        assert result["name"] == "my_notebook"
        assert result["path"] == "/notebooks/my_notebook"

    def test_args_passed_through_when_present(self):
        act = _activity(args={"key": "value", "year": 2025})
        result = resolve_activity(act, {})
        assert result["args"] == {"key": "value", "year": 2025}

    def test_args_absent_when_not_in_activity(self):
        result = resolve_activity(_activity(), {})
        assert "args" not in result

    def test_dependencies_passed_through_when_present(self):
        act = _activity(dependencies=["step_a", "step_b"])
        result = resolve_activity(act, {})
        assert result["dependencies"] == ["step_a", "step_b"]

    def test_dependencies_absent_when_not_in_activity(self):
        result = resolve_activity(_activity(), {})
        assert "dependencies" not in result

    def test_workspace_passed_through_when_present(self):
        act = _activity(workspace="my_workspace")
        result = resolve_activity(act, {})
        assert result["workspace"] == "my_workspace"

    def test_workspace_absent_when_not_in_activity(self):
        result = resolve_activity(_activity(), {})
        assert "workspace" not in result

    def test_workspace_not_inheritable_from_pipeline_defaults(self):
        """workspace can only be set per-activity, never via pipeline defaults."""
        result = resolve_activity(_activity(), {"workspace": "should_be_ignored"})
        assert "workspace" not in result

    def test_result_is_a_new_dict(self):
        act = _activity()
        result = resolve_activity(act, {})
        assert result is not act
