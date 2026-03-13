"""Tests for fabric_metadata_dags.validator — DAG structural validation."""

import pytest

from fabric_metadata_dags.validator import validate_dag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _act(name, deps=None):
    a = {"name": name}
    if deps is not None:
        a["dependencies"] = deps
    return a


# ---------------------------------------------------------------------------
# Valid graphs — must not raise
# ---------------------------------------------------------------------------


class TestValidGraphs:
    def test_empty_list_is_valid(self):
        validate_dag([])  # no exception

    def test_single_activity_no_deps(self):
        validate_dag([_act("a")])

    def test_linear_chain(self):
        validate_dag([_act("a"), _act("b", ["a"]), _act("c", ["b"])])

    def test_fan_out(self):
        validate_dag([_act("root"), _act("b", ["root"]), _act("c", ["root"])])

    def test_fan_in(self):
        validate_dag([_act("a"), _act("b"), _act("c", ["a", "b"])])

    def test_diamond(self):
        validate_dag(
            [
                _act("source"),
                _act("left", ["source"]),
                _act("right", ["source"]),
                _act("sink", ["left", "right"]),
            ]
        )


# ---------------------------------------------------------------------------
# Duplicate names
# ---------------------------------------------------------------------------


class TestDuplicateNames:
    def test_single_duplicate_raises(self):
        with pytest.raises(ValueError, match="Duplicate activity name"):
            validate_dag([_act("a"), _act("b"), _act("a")])

    def test_error_message_contains_duplicate_name(self):
        with pytest.raises(ValueError, match='"step_x"'):
            validate_dag([_act("step_x"), _act("step_x")])

    def test_multiple_duplicates_reported(self):
        with pytest.raises(ValueError, match="Duplicate"):
            validate_dag([_act("a"), _act("a"), _act("b"), _act("b")])


# ---------------------------------------------------------------------------
# Missing dependencies
# ---------------------------------------------------------------------------


class TestMissingDependencies:
    def test_missing_dep_raises(self):
        with pytest.raises(ValueError, match="no such activity"):
            validate_dag([_act("a", ["ghost"])])

    def test_error_message_contains_activity_and_dep(self):
        with pytest.raises(ValueError, match='"b"') as exc_info:
            validate_dag([_act("a", ["b"])])
        assert '"a"' in str(exc_info.value)

    def test_valid_dep_does_not_raise(self):
        validate_dag([_act("a"), _act("b", ["a"])])

    def test_dep_defined_after_activity_is_valid(self):
        # Order of definition in the list should not matter.
        validate_dag([_act("b", ["a"]), _act("a")])


# ---------------------------------------------------------------------------
# Circular dependencies
# ---------------------------------------------------------------------------


class TestCircularDependencies:
    def test_self_dependency_raises(self):
        with pytest.raises(ValueError, match="Circular dependency"):
            validate_dag([_act("a", ["a"])])

    def test_two_node_cycle_raises(self):
        with pytest.raises(ValueError, match="Circular dependency"):
            validate_dag([_act("a", ["b"]), _act("b", ["a"])])

    def test_three_node_cycle_raises(self):
        with pytest.raises(ValueError, match="Circular dependency"):
            validate_dag([_act("a", ["c"]), _act("b", ["a"]), _act("c", ["b"])])

    def test_cycle_in_one_branch_raises(self):
        """Cycle among some nodes while others are clean."""
        with pytest.raises(ValueError, match="Circular dependency"):
            validate_dag(
                [
                    _act("root"),
                    _act("good", ["root"]),
                    _act("bad_a", ["bad_b"]),
                    _act("bad_b", ["bad_a"]),
                ]
            )

    def test_long_linear_chain_no_cycle(self):
        chain = [_act("n0")]
        for i in range(1, 20):
            chain.append(_act(f"n{i}", [f"n{i - 1}"]))
        validate_dag(chain)  # must not raise
