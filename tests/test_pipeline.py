"""Tests for fabric_metadata_dags.pipeline — run_pipeline service."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fabric_metadata_dags.pipeline import run_pipeline


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_MINIMAL_YAML = """\
pipeline: my_pipeline
settings:
  concurrency: 5
  timeoutInSeconds: 3600
activities:
  - name: step_one
    path: /notebooks/step_one
"""

_TWO_ACTIVITY_YAML = """\
pipeline: two_step
activities:
  - name: first
    path: /notebooks/first
  - name: second
    path: /notebooks/second
    dependencies:
      - first
"""

_INVALID_SCHEMA_YAML = """\
pipeline: bad
unknown_top_key: oops
activities: []
"""

_CYCLE_YAML = """\
pipeline: cyclic
activities:
  - name: a
    path: /nb/a
    dependencies:
      - b
  - name: b
    path: /nb/b
    dependencies:
      - a
"""


def write_yaml(tmp_path: Path, name: str, content: str) -> Path:
    f = tmp_path / f"{name}.yaml"
    f.write_text(content, encoding="utf-8")
    return f


# ---------------------------------------------------------------------------
# Happy-path
# ---------------------------------------------------------------------------


class TestRunPipelineSuccess:
    def test_returns_path_to_ipynb(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "my_pipeline", _MINIMAL_YAML)
        out = run_pipeline(yaml_path, tmp_path)
        assert out == tmp_path / "my_pipeline.ipynb"

    def test_output_file_exists(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "my_pipeline", _MINIMAL_YAML)
        out = run_pipeline(yaml_path, tmp_path)
        assert out.exists()

    def test_output_is_valid_json(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "my_pipeline", _MINIMAL_YAML)
        out = run_pipeline(yaml_path, tmp_path)
        nb = json.loads(out.read_text(encoding="utf-8"))
        assert "cells" in nb
        assert "metadata" in nb

    def test_pipeline_name_from_yaml_key(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "ignored_filename", _MINIMAL_YAML)
        out = run_pipeline(yaml_path, tmp_path)
        # pipeline: key is "my_pipeline", not "ignored_filename"
        assert out.name == "my_pipeline.ipynb"

    def test_pipeline_name_falls_back_to_stem(self, tmp_path):
        content = "activities:\n  - name: a\n    path: /nb/a\n"
        yaml_path = write_yaml(tmp_path, "fallback_name", content)
        out = run_pipeline(yaml_path, tmp_path)
        assert out.name == "fallback_name.ipynb"

    def test_creates_output_dir_if_missing(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "my_pipeline", _MINIMAL_YAML)
        out_dir = tmp_path / "new_subdir"
        assert not out_dir.exists()
        run_pipeline(yaml_path, out_dir)
        assert out_dir.exists()

    def test_two_activity_pipeline_runs(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "two_step", _TWO_ACTIVITY_YAML)
        out = run_pipeline(yaml_path, tmp_path)
        assert out.exists()

    def test_display_dag_false_by_default(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "my_pipeline", _MINIMAL_YAML)
        out = run_pipeline(yaml_path, tmp_path)
        source = "".join(
            "".join(cell["source"])
            for cell in json.loads(out.read_text())["cells"]
            if cell["cell_type"] == "code"
        )
        assert (
            "displayDAGViaGraphviz: False" in source
            or '"displayDAGViaGraphviz": False' in source
        )

    def test_display_dag_true_when_requested(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "my_pipeline", _MINIMAL_YAML)
        out = run_pipeline(yaml_path, tmp_path, display_dag=True)
        source = "".join(
            "".join(cell["source"])
            for cell in json.loads(out.read_text())["cells"]
            if cell["cell_type"] == "code"
        )
        assert "True" in source

    def test_defaults_applied_to_activities(self, tmp_path):
        content = """\
pipeline: defaults_test
defaults:
  retry: 3
  retryIntervalInSeconds: 10
  timeoutPerCellInSeconds: 120
activities:
  - name: act
    path: /nb/act
"""
        yaml_path = write_yaml(tmp_path, "defaults_test", content)
        out = run_pipeline(yaml_path, tmp_path)
        nb = json.loads(out.read_text())
        # The DAG cell should contain the resolved retry value
        dag_cell = next(c for c in nb["cells"] if "DAG" in "".join(c["source"]))
        assert "3" in "".join(dag_cell["source"])


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestRunPipelineErrors:
    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            run_pipeline(tmp_path / "nonexistent.yaml", tmp_path)

    def test_invalid_schema_raises_value_error(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "bad", _INVALID_SCHEMA_YAML)
        with pytest.raises(ValueError, match="schema"):
            run_pipeline(yaml_path, tmp_path)

    def test_circular_dependency_raises_value_error(self, tmp_path):
        yaml_path = write_yaml(tmp_path, "cyclic", _CYCLE_YAML)
        with pytest.raises(ValueError, match="[Cc]ircular"):
            run_pipeline(yaml_path, tmp_path)

    def test_missing_dependency_raises_value_error(self, tmp_path):
        content = """\
pipeline: missing_dep
activities:
  - name: act
    path: /nb/act
    dependencies:
      - ghost
"""
        yaml_path = write_yaml(tmp_path, "missing_dep", content)
        with pytest.raises(ValueError, match="ghost"):
            run_pipeline(yaml_path, tmp_path)

    def test_duplicate_activity_names_raises_value_error(self, tmp_path):
        content = """\
pipeline: dupe
activities:
  - name: act
    path: /nb/a
  - name: act
    path: /nb/b
"""
        yaml_path = write_yaml(tmp_path, "dupe", content)
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            run_pipeline(yaml_path, tmp_path)
