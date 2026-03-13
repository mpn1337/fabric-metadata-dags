"""Tests for fabric_metadata_dags.builder — DAG dict construction."""

import pytest

from fabric_metadata_dags.builder import build_dag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolved_act(
    name, path, *, retry=0, retry_interval=10, timeout=90, args=None, deps=None
):
    act = {
        "name": name,
        "path": path,
        "retry": retry,
        "retryIntervalInSeconds": retry_interval,
        "timeoutPerCellInSeconds": timeout,
    }
    if args is not None:
        act["args"] = args
    if deps is not None:
        act["dependencies"] = deps
    return act


def _pipeline(concurrency=50, timeout_secs=43200):
    return {"settings": {"concurrency": concurrency, "timeoutInSeconds": timeout_secs}}


# ---------------------------------------------------------------------------
# Top-level settings
# ---------------------------------------------------------------------------


class TestTopLevelSettings:
    def test_concurrency_included(self):
        dag = build_dag(_pipeline(concurrency=10), [])
        assert dag["concurrency"] == 10

    def test_timeout_in_seconds_included(self):
        dag = build_dag(_pipeline(timeout_secs=3600), [])
        assert dag["timeoutInSeconds"] == 3600

    def test_missing_settings_keys_absent_from_dag(self):
        dag = build_dag({"settings": {}}, [])
        assert "concurrency" not in dag
        assert "timeoutInSeconds" not in dag

    def test_no_settings_block_at_all(self):
        dag = build_dag({}, [])
        assert "concurrency" not in dag
        assert "timeoutInSeconds" not in dag


# ---------------------------------------------------------------------------
# Activity fields
# ---------------------------------------------------------------------------


class TestActivityFields:
    def test_name_and_path_present(self):
        acts = [_resolved_act("step_a", "/notebooks/step_a")]
        dag = build_dag(_pipeline(), acts)
        fa = dag["activities"][0]
        assert fa["name"] == "step_a"
        assert fa["path"] == "/notebooks/step_a"

    def test_inheritable_scalar_fields_present(self):
        acts = [_resolved_act("step_a", "/nb", retry=3, retry_interval=15, timeout=120)]
        fa = build_dag(_pipeline(), acts)["activities"][0]
        assert fa["retry"] == 3
        assert fa["retryIntervalInSeconds"] == 15
        assert fa["timeoutPerCellInSeconds"] == 120

    def test_args_included_when_present(self):
        acts = [_resolved_act("step_a", "/nb", args={"key": "val"})]
        fa = build_dag(_pipeline(), acts)["activities"][0]
        assert fa["args"] == {"key": "val"}

    def test_args_omitted_when_absent(self):
        acts = [_resolved_act("step_a", "/nb")]
        fa = build_dag(_pipeline(), acts)["activities"][0]
        assert "args" not in fa

    def test_dependencies_included_when_present(self):
        acts = [_resolved_act("step_b", "/nb", deps=["step_a"])]
        fa = build_dag(_pipeline(), acts)["activities"][0]
        assert fa["dependencies"] == ["step_a"]

    def test_dependencies_omitted_when_absent(self):
        acts = [_resolved_act("step_a", "/nb")]
        fa = build_dag(_pipeline(), acts)["activities"][0]
        assert "dependencies" not in fa

    def test_empty_dependencies_list_omitted(self):
        act = _resolved_act("step_a", "/nb")
        act["dependencies"] = []
        fa = build_dag(_pipeline(), [act])["activities"][0]
        assert "dependencies" not in fa

    def test_workspace_included_when_present(self):
        act = _resolved_act("step_a", "/nb")
        act["workspace"] = "my_workspace"
        fa = build_dag(_pipeline(), [act])["activities"][0]
        assert fa["workspace"] == "my_workspace"

    def test_workspace_omitted_when_absent(self):
        acts = [_resolved_act("step_a", "/nb")]
        fa = build_dag(_pipeline(), acts)["activities"][0]
        assert "workspace" not in fa

    def test_multiple_activities_all_present(self):
        acts = [
            _resolved_act("a", "/nb/a"),
            _resolved_act("b", "/nb/b", deps=["a"]),
            _resolved_act("c", "/nb/c", deps=["b"]),
        ]
        dag = build_dag(_pipeline(), acts)
        names = [a["name"] for a in dag["activities"]]
        assert names == ["a", "b", "c"]

    def test_activity_order_preserved(self):
        acts = [_resolved_act(f"step_{i}", f"/nb/{i}") for i in range(5)]
        dag = build_dag(_pipeline(), acts)
        assert [a["name"] for a in dag["activities"]] == [f"step_{i}" for i in range(5)]


# ---------------------------------------------------------------------------
# refreshInterval in settings
# ---------------------------------------------------------------------------


class TestRefreshInterval:
    def test_refresh_interval_included_when_set(self):
        pipeline = {"settings": {"concurrency": 5, "refreshInterval": 3}}
        dag = build_dag(pipeline, [])
        assert dag["refreshInterval"] == 3

    def test_refresh_interval_omitted_when_absent(self):
        dag = build_dag(_pipeline(), [])
        assert "refreshInterval" not in dag
