"""Tests for fabric_metadata_dags.loader — YAML pipeline loading."""

import pytest

from fabric_metadata_dags.loader import load_pipeline


class TestLoadPipeline:
    def test_loads_valid_yaml(self, tmp_path):
        f = tmp_path / "pipeline.yaml"
        f.write_text("pipeline: test\nactivities: []\n", encoding="utf-8")
        result = load_pipeline(f)
        assert result["pipeline"] == "test"
        assert result["activities"] == []

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="not found"):
            load_pipeline(tmp_path / "nonexistent.yaml")

    def test_non_mapping_yaml_raises(self, tmp_path):
        f = tmp_path / "bad.yaml"
        f.write_text("- item1\n- item2\n", encoding="utf-8")
        with pytest.raises(ValueError, match="mapping"):
            load_pipeline(f)

    def test_empty_file_raises(self, tmp_path):
        f = tmp_path / "empty.yaml"
        f.write_text("", encoding="utf-8")
        with pytest.raises(ValueError, match="mapping"):
            load_pipeline(f)

    def test_full_pipeline_structure_loaded(self, tmp_path):
        yaml_content = """\
pipeline: my_pipeline
settings:
  concurrency: 10
  timeoutInSeconds: 3600
defaults:
  retry: 2
activities:
  - name: step_a
    path: /notebooks/step_a
"""
        f = tmp_path / "my_pipeline.yaml"
        f.write_text(yaml_content, encoding="utf-8")
        result = load_pipeline(f)
        assert result["pipeline"] == "my_pipeline"
        assert result["settings"]["concurrency"] == 10
        assert result["defaults"]["retry"] == 2
        assert result["activities"][0]["name"] == "step_a"

    def test_accepts_string_path(self, tmp_path):
        f = tmp_path / "pipeline.yaml"
        f.write_text("pipeline: str_path_test\n", encoding="utf-8")
        result = load_pipeline(str(f))  # str, not Path
        assert result["pipeline"] == "str_path_test"
