"""Tests for fabric_metadata_dags.scaffold — scaffold_pipeline service."""

from __future__ import annotations

import yaml
import pytest

from fabric_metadata_dags.scaffold import scaffold_pipeline
from fabric_metadata_dags.validator import validate_pipeline_schema


# ---------------------------------------------------------------------------
# Return value and file creation
# ---------------------------------------------------------------------------


class TestScaffoldPipelineCreatesFile:
    def test_returns_path_to_yaml(self, tmp_path):
        result = scaffold_pipeline("my_pipe", metadata_dir=tmp_path)
        assert result == tmp_path / "my_pipe.yaml"

    def test_file_exists_after_call(self, tmp_path):
        result = scaffold_pipeline("my_pipe", metadata_dir=tmp_path)
        assert result.exists()

    def test_creates_metadata_dir_if_missing(self, tmp_path):
        subdir = tmp_path / "new_dir"
        assert not subdir.exists()
        scaffold_pipeline("pipe", metadata_dir=subdir)
        assert subdir.exists()

    def test_filename_matches_name(self, tmp_path):
        result = scaffold_pipeline("sales_pipeline", metadata_dir=tmp_path)
        assert result.name == "sales_pipeline.yaml"


# ---------------------------------------------------------------------------
# YAML content
# ---------------------------------------------------------------------------


class TestScaffoldPipelineContent:
    def test_pipeline_key_matches_name(self, tmp_path):
        scaffold_pipeline("my_pipe", metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "my_pipe.yaml").read_text())
        assert data["pipeline"] == "my_pipe"

    def test_settings_block_present(self, tmp_path):
        scaffold_pipeline("pipe", metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert "settings" in data
        assert "concurrency" in data["settings"]
        assert "timeoutInSeconds" in data["settings"]

    def test_defaults_block_present(self, tmp_path):
        scaffold_pipeline("pipe", metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert "defaults" in data
        assert "retry" in data["defaults"]
        assert "retryIntervalInSeconds" in data["defaults"]
        assert "timeoutPerCellInSeconds" in data["defaults"]

    def test_activities_block_present(self, tmp_path):
        scaffold_pipeline("pipe", metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert "activities" in data
        assert isinstance(data["activities"], list)

    def test_generated_yaml_passes_schema_validation(self, tmp_path):
        scaffold_pipeline("pipe", metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        validate_pipeline_schema(data)  # must not raise

    def test_file_is_utf8(self, tmp_path):
        result = scaffold_pipeline("pipe", metadata_dir=tmp_path)
        # Should decode without error
        result.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Activity stubs
# ---------------------------------------------------------------------------


class TestScaffoldPipelineActivities:
    def test_default_produces_two_activities(self, tmp_path):
        scaffold_pipeline("pipe", metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert len(data["activities"]) == 2

    def test_custom_count(self, tmp_path):
        scaffold_pipeline("pipe", num_activities=4, metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert len(data["activities"]) == 4

    def test_single_activity(self, tmp_path):
        scaffold_pipeline("pipe", num_activities=1, metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert len(data["activities"]) == 1

    def test_first_activity_has_no_dependencies(self, tmp_path):
        scaffold_pipeline("pipe", metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        first = data["activities"][0]
        assert "dependencies" not in first or first["dependencies"] is None

    def test_second_activity_depends_on_first(self, tmp_path):
        scaffold_pipeline("pipe", metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        assert data["activities"][1]["dependencies"] == ["activity_1"]

    def test_chain_for_three_activities(self, tmp_path):
        scaffold_pipeline("pipe", num_activities=3, metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        acts = data["activities"]
        assert acts[1]["dependencies"] == ["activity_1"]
        assert acts[2]["dependencies"] == ["activity_2"]

    def test_activity_names_are_sequential(self, tmp_path):
        scaffold_pipeline("pipe", num_activities=3, metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        names = [a["name"] for a in data["activities"]]
        assert names == ["activity_1", "activity_2", "activity_3"]

    def test_each_activity_has_path_key(self, tmp_path):
        scaffold_pipeline("pipe", num_activities=2, metadata_dir=tmp_path)
        data = yaml.safe_load((tmp_path / "pipe.yaml").read_text())
        for act in data["activities"]:
            assert "path" in act


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestScaffoldPipelineErrors:
    def test_raises_file_exists_error_if_file_exists(self, tmp_path):
        (tmp_path / "pipe.yaml").write_text("pipeline: pipe\n", encoding="utf-8")
        with pytest.raises(FileExistsError):
            scaffold_pipeline("pipe", metadata_dir=tmp_path)

    def test_does_not_overwrite_existing_file(self, tmp_path):
        original = "pipeline: original\n"
        (tmp_path / "pipe.yaml").write_text(original, encoding="utf-8")
        with pytest.raises(FileExistsError):
            scaffold_pipeline("pipe", metadata_dir=tmp_path)
        assert (tmp_path / "pipe.yaml").read_text() == original

    def test_error_contains_conflicting_path(self, tmp_path):
        (tmp_path / "pipe.yaml").write_text("pipeline: pipe\n", encoding="utf-8")
        with pytest.raises(FileExistsError, match="pipe.yaml"):
            scaffold_pipeline("pipe", metadata_dir=tmp_path)
