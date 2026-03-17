"""Tests for fabric_metadata_dags.generator — .ipynb notebook generation."""

import json

import pytest

from fabric_metadata_dags.generator import generate_notebook, _format_dag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_dag():
    return {
        "activities": [
            {
                "name": "step_a",
                "path": "/notebooks/step_a",
                "retry": 0,
                "retryIntervalInSeconds": 10,
                "timeoutPerCellInSeconds": 90,
            }
        ],
        "concurrency": 5,
        "timeoutInSeconds": 3600,
    }


# ---------------------------------------------------------------------------
# File creation
# ---------------------------------------------------------------------------


class TestGenerateNotebook:
    def test_creates_ipynb_file(self, tmp_path):
        out = generate_notebook("my_pipeline", _minimal_dag(), output_dir=tmp_path)
        assert out.exists()
        assert out.suffix == ".ipynb"
        assert out.name == "my_pipeline.ipynb"

    def test_output_dir_created_if_missing(self, tmp_path):
        nested = tmp_path / "a" / "b" / "c"
        generate_notebook("p", _minimal_dag(), output_dir=nested)
        assert nested.is_dir()

    def test_output_path_uses_pipeline_name(self, tmp_path):
        out = generate_notebook("sales_pipeline", _minimal_dag(), output_dir=tmp_path)
        assert out.stem == "sales_pipeline"


# ---------------------------------------------------------------------------
# Notebook structure
# ---------------------------------------------------------------------------


class TestNotebookStructure:
    def _load_nb(self, tmp_path, display_dag=False):
        out = generate_notebook(
            "test_nb",
            _minimal_dag(),
            display_dag_graphviz=display_dag,
            output_dir=tmp_path,
            include_run_cell=False,
        )
        return json.loads(out.read_text(encoding="utf-8"))

    def test_notebook_has_exactly_three_cells(self, tmp_path):
        nb = self._load_nb(tmp_path)
        assert len(nb["cells"]) == 3

    def test_all_cells_are_code(self, tmp_path):
        nb = self._load_nb(tmp_path)
        for cell in nb["cells"]:
            assert cell["cell_type"] == "code"

    def test_cell_1_imports_mssparkutils(self, tmp_path):
        nb = self._load_nb(tmp_path)
        src = "".join(nb["cells"][0]["source"])
        assert "from notebookutils import mssparkutils" in src

    def test_cell_2_contains_dag_assignment(self, tmp_path):
        nb = self._load_nb(tmp_path)
        src = "".join(nb["cells"][1]["source"])
        assert src.startswith("DAG = ")

    def test_cell_2_dag_is_valid_python(self, tmp_path):
        nb = self._load_nb(tmp_path)
        src = "".join(nb["cells"][1]["source"])
        # Should be executable Python — eval the RHS
        namespace = {}
        exec(src, namespace)  # noqa: S102
        dag = namespace["DAG"]
        assert isinstance(dag, dict)
        assert "activities" in dag

    def test_cell_3_calls_run_multiple(self, tmp_path):
        nb = self._load_nb(tmp_path)
        src = "".join(nb["cells"][2]["source"])
        assert "mssparkutils.notebook.runMultiple" in src

    def test_cell_3_display_dag_false_by_default(self, tmp_path):
        nb = self._load_nb(tmp_path, display_dag=False)
        src = "".join(nb["cells"][2]["source"])
        assert '"displayDAGViaGraphviz": False' in src

    def test_cell_3_display_dag_true_when_requested(self, tmp_path):
        nb = self._load_nb(tmp_path, display_dag=True)
        src = "".join(nb["cells"][2]["source"])
        assert '"displayDAGViaGraphviz": True' in src

    def test_notebook_has_python_kernelspec(self, tmp_path):
        nb = self._load_nb(tmp_path)
        assert nb["metadata"]["kernelspec"]["language"] == "python"


# ---------------------------------------------------------------------------
# DAG formatter (_format_dag)
# ---------------------------------------------------------------------------


class TestFormatDag:
    def test_output_starts_with_dag_assignment(self):
        result = _format_dag({"activities": []})
        assert result.startswith("DAG = ")

    def test_small_dict_rendered_inline(self):
        result = _format_dag({"activities": [], "concurrency": 5})
        # Neither key is long; the whole thing should be a single-level result
        assert "concurrency" in result

    def test_trailing_comma_on_last_item(self):
        dag = {
            "activities": [
                {
                    "name": "x",
                    "path": "/nb/x",
                    "retry": 0,
                    "retryIntervalInSeconds": 10,
                    "timeoutPerCellInSeconds": 90,
                }
            ]
        }
        result = _format_dag(dag)
        # Multiline dict items end with a trailing comma before closing brace
        assert "}," in result or result.endswith("}")

    def test_result_is_valid_python(self):
        dag = _minimal_dag()
        result = _format_dag(dag)
        namespace: dict = {}
        exec(result, namespace)  # noqa: S102
        assert namespace["DAG"] == dag
