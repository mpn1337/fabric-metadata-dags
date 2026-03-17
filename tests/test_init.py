"""Tests for the ``generate-pipeline init`` CLI command."""

from __future__ import annotations

import yaml
import pytest
from typer.testing import CliRunner

from fabric_metadata_dags.cli import app
from fabric_metadata_dags.validator import validate_pipeline_schema


runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def run_init(name: str, *extra_args: str):
    return runner.invoke(app, ["init", name, *extra_args])


# ---------------------------------------------------------------------------
# Basic creation
# ---------------------------------------------------------------------------


class TestInitCreatesFile:
    def test_creates_yaml_in_default_metadata_dir(self, tmp_path):
        result = run_init("my_pipeline", "--metadata-dir", str(tmp_path))
        assert result.exit_code == 0
        assert (tmp_path / "my_pipeline.yaml").exists()

    def test_output_confirms_path(self, tmp_path):
        result = run_init("my_pipeline", "--metadata-dir", str(tmp_path))
        assert "my_pipeline.yaml" in result.output

    def test_creates_metadata_dir_if_missing(self, tmp_path):
        subdir = tmp_path / "new_dir"
        assert not subdir.exists()
        run_init("pipe", "--metadata-dir", str(subdir))
        assert subdir.exists()

    def test_pipeline_name_in_file(self, tmp_path):
        run_init("sales", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "sales.yaml").read_text())
        assert content["pipeline"] == "sales"


# ---------------------------------------------------------------------------
# Activity stubs
# ---------------------------------------------------------------------------


class TestInitActivityStubs:
    def test_default_two_activities(self, tmp_path):
        run_init("pipe", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert len(content["activities"]) == 2

    def test_custom_activity_count(self, tmp_path):
        run_init("pipe", "--activities", "4", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert len(content["activities"]) == 4

    def test_first_activity_has_no_dependencies(self, tmp_path):
        run_init("pipe", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        first = content["activities"][0]
        assert "dependencies" not in first or first["dependencies"] is None

    def test_second_activity_depends_on_first(self, tmp_path):
        run_init("pipe", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        second = content["activities"][1]
        assert second["dependencies"] == ["activity_1"]

    def test_chain_dependencies_for_more_activities(self, tmp_path):
        run_init("pipe", "--activities", "3", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        acts = content["activities"]
        assert acts[1]["dependencies"] == ["activity_1"]
        assert acts[2]["dependencies"] == ["activity_2"]

    def test_single_activity_has_no_dependencies(self, tmp_path):
        run_init("pipe", "--activities", "1", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert len(content["activities"]) == 1
        assert "dependencies" not in content["activities"][0]


# ---------------------------------------------------------------------------
# Generated YAML passes schema validation
# ---------------------------------------------------------------------------


class TestInitYamlIsValid:
    def test_generated_yaml_passes_schema_validation(self, tmp_path):
        run_init("valid_pipe", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "valid_pipe.yaml").read_text())
        # Must not raise
        validate_pipeline_schema(content)

    def test_generated_yaml_has_settings_block(self, tmp_path):
        run_init("pipe", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert "settings" in content
        assert "concurrency" in content["settings"]
        assert "timeoutInSeconds" in content["settings"]

    def test_generated_yaml_has_defaults_block(self, tmp_path):
        run_init("pipe", "--metadata-dir", str(tmp_path))
        content = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert "defaults" in content


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestInitErrors:
    def test_exits_nonzero_if_file_exists(self, tmp_path):
        (tmp_path / "pipe.yaml").write_text("pipeline: pipe\n", encoding="utf-8")
        result = run_init("pipe", "--metadata-dir", str(tmp_path))
        assert result.exit_code != 0

    def test_does_not_overwrite_existing_file(self, tmp_path):
        original = "pipeline: original\n"
        (tmp_path / "pipe.yaml").write_text(original, encoding="utf-8")
        run_init("pipe", "--metadata-dir", str(tmp_path))
        assert (tmp_path / "pipe.yaml").read_text() == original

    def test_error_message_mentions_filename(self, tmp_path):
        (tmp_path / "pipe.yaml").write_text("pipeline: pipe\n", encoding="utf-8")
        result = run_init("pipe", "--metadata-dir", str(tmp_path))
        assert "pipe.yaml" in result.output
