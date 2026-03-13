"""Tests for fabric_metadata_dags.validator — schema validation (unknown keys)."""

import pytest

from fabric_metadata_dags.validator import validate_pipeline_schema


# ---------------------------------------------------------------------------
# Valid pipelines — must not raise
# ---------------------------------------------------------------------------


class TestValidSchema:
    def test_minimal_pipeline_valid(self):
        validate_pipeline_schema({"pipeline": "p", "activities": []})

    def test_all_known_top_level_keys(self):
        validate_pipeline_schema(
            {
                "pipeline": "p",
                "settings": {"concurrency": 10},
                "defaults": {"retry": 1},
                "activities": [{"name": "a", "path": "/nb/a"}],
            }
        )

    def test_all_known_settings_keys(self):
        validate_pipeline_schema(
            {
                "pipeline": "p",
                "settings": {
                    "concurrency": 10,
                    "timeoutInSeconds": 3600,
                    "refreshInterval": 5,
                },
                "activities": [],
            }
        )

    def test_all_known_defaults_keys(self):
        validate_pipeline_schema(
            {
                "pipeline": "p",
                "defaults": {
                    "retry": 2,
                    "retryIntervalInSeconds": 30,
                    "timeoutPerCellInSeconds": 120,
                },
                "activities": [],
            }
        )

    def test_all_known_activity_keys(self):
        validate_pipeline_schema(
            {
                "pipeline": "p",
                "activities": [
                    {
                        "name": "a",
                        "path": "/nb/a",
                        "timeoutPerCellInSeconds": 90,
                        "args": {"k": "v"},
                        "workspace": "my_workspace",
                        "retry": 1,
                        "retryIntervalInSeconds": 10,
                        "dependencies": [],
                    }
                ],
            }
        )


# ---------------------------------------------------------------------------
# Unknown top-level keys
# ---------------------------------------------------------------------------


class TestUnknownTopLevelKeys:
    def test_unknown_top_key_raises(self):
        with pytest.raises(ValueError, match="Unknown top-level pipeline key"):
            validate_pipeline_schema({"pipeline": "p", "foo": "bar", "activities": []})

    def test_error_lists_the_unknown_key(self):
        with pytest.raises(ValueError, match='"mystery"'):
            validate_pipeline_schema({"pipeline": "p", "mystery": 1, "activities": []})

    def test_multiple_unknown_top_keys_reported_together(self):
        with pytest.raises(ValueError) as exc_info:
            validate_pipeline_schema(
                {"pipeline": "p", "alpha": 1, "beta": 2, "activities": []}
            )
        msg = str(exc_info.value)
        assert '"alpha"' in msg
        assert '"beta"' in msg


# ---------------------------------------------------------------------------
# Unknown settings keys
# ---------------------------------------------------------------------------


class TestUnknownSettingsKeys:
    def test_unknown_settings_key_raises(self):
        with pytest.raises(ValueError, match="Unknown settings key"):
            validate_pipeline_schema(
                {"pipeline": "p", "settings": {"maxRetries": 3}, "activities": []}
            )

    def test_error_lists_the_unknown_key(self):
        with pytest.raises(ValueError, match='"maxRetries"'):
            validate_pipeline_schema(
                {"pipeline": "p", "settings": {"maxRetries": 3}, "activities": []}
            )


# ---------------------------------------------------------------------------
# Unknown defaults keys
# ---------------------------------------------------------------------------


class TestUnknownDefaultsKeys:
    def test_unknown_defaults_key_raises(self):
        with pytest.raises(ValueError, match="Unknown defaults key"):
            validate_pipeline_schema(
                {"pipeline": "p", "defaults": {"maxWait": 60}, "activities": []}
            )


# ---------------------------------------------------------------------------
# Unknown activity keys
# ---------------------------------------------------------------------------


class TestUnknownActivityKeys:
    def test_unknown_activity_key_raises(self):
        with pytest.raises(ValueError, match="Unknown key"):
            validate_pipeline_schema(
                {
                    "pipeline": "p",
                    "activities": [{"name": "a", "path": "/nb", "colour": "blue"}],
                }
            )

    def test_error_contains_activity_name(self):
        with pytest.raises(ValueError, match='"my_step"'):
            validate_pipeline_schema(
                {
                    "pipeline": "p",
                    "activities": [
                        {"name": "my_step", "path": "/nb", "bogus_field": True}
                    ],
                }
            )

    def test_error_contains_unknown_key_name(self):
        with pytest.raises(ValueError, match='"bogus_field"'):
            validate_pipeline_schema(
                {
                    "pipeline": "p",
                    "activities": [
                        {"name": "step", "path": "/nb", "bogus_field": True}
                    ],
                }
            )

    def test_multiple_bad_activities_all_reported(self):
        with pytest.raises(ValueError) as exc_info:
            validate_pipeline_schema(
                {
                    "pipeline": "p",
                    "activities": [
                        {"name": "a", "path": "/nb", "bad1": 1},
                        {"name": "b", "path": "/nb", "bad2": 2},
                    ],
                }
            )
        msg = str(exc_info.value)
        assert '"bad1"' in msg
        assert '"bad2"' in msg

    def test_all_errors_reported_in_single_exception(self):
        """One ValueError should surface all schema problems at once."""
        with pytest.raises(ValueError) as exc_info:
            validate_pipeline_schema(
                {
                    "pipeline": "p",
                    "unknown_top": 1,
                    "settings": {"bad_setting": 2},
                    "activities": [{"name": "x", "path": "/nb", "bad_act": 3}],
                }
            )
        msg = str(exc_info.value)
        assert '"unknown_top"' in msg
        assert '"bad_setting"' in msg
        assert '"bad_act"' in msg
