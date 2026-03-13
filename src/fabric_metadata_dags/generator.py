"""Generate a Jupyter notebook (.ipynb) that executes a Fabric pipeline DAG."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import nbformat
from nbformat.v4 import new_code_cell, new_notebook


# ---------------------------------------------------------------------------
# Prettier-style Python literal formatter
# ---------------------------------------------------------------------------

# Collections whose inline rendering fits within this character budget are
# kept on a single line (e.g. small args dicts, single-item dependency lists).
_INLINE_WIDTH = 72


def _format_value(value: Any, level: int = 0) -> str:
    """Recursively format *value* as a Python literal.

    Rules:
    - Dicts and lists are rendered inline when the single-line form fits
      within ``_INLINE_WIDTH`` characters.
    - Otherwise they are expanded with 4-space indentation and trailing
      commas on every item (including the last).
    - All other types are passed through ``repr()``.
    """
    indent = "    " * level
    inner_indent = "    " * (level + 1)

    if isinstance(value, bool):
        return repr(value)

    if isinstance(value, (int, float, str, type(None))):
        return repr(value)

    if isinstance(value, dict):
        if not value:
            return "{}"
        # Try inline first.
        inline_items = ", ".join(
            f"{repr(k)}: {_format_value(v, 0)}" for k, v in value.items()
        )
        inline = "{" + inline_items + "}"
        if len(inline) <= _INLINE_WIDTH and "\n" not in inline:
            return inline
        # Multiline.
        lines = ["{"]
        for k, v in value.items():
            lines.append(f"{inner_indent}{repr(k)}: {_format_value(v, level + 1)},")
        lines.append(f"{indent}}}")
        return "\n".join(lines)

    if isinstance(value, (list, tuple)):
        open_b, close_b = ("[", "]") if isinstance(value, list) else ("(", ")")
        if not value:
            return open_b + close_b
        # Try inline first.
        inline_items = ", ".join(_format_value(item, 0) for item in value)
        inline = open_b + inline_items + close_b
        if len(inline) <= _INLINE_WIDTH and "\n" not in inline:
            return inline
        # Multiline.
        lines = [open_b]
        for item in value:
            lines.append(f"{inner_indent}{_format_value(item, level + 1)},")
        lines.append(f"{indent}{close_b}")
        return "\n".join(lines)

    return repr(value)


def _format_dag(dag: dict[str, Any]) -> str:
    """Return the DAG dict as a nicely-formatted Python literal string."""
    return "DAG = " + _format_value(dag, level=0)


# ---------------------------------------------------------------------------
# Cell templates
# ---------------------------------------------------------------------------

_CELL_IMPORTS = "from notebookutils import mssparkutils"

_CELL_RUN_TEMPLATE = (
    "mssparkutils.notebook.runMultiple(\n"
    "    DAG,\n"
    '    {{"displayDAGViaGraphviz": {display_dag}}},\n'
    ")"
)


def generate_notebook(
    pipeline_name: str,
    dag: dict[str, Any],
    display_dag_graphviz: bool = False,
    output_dir: Path | str = "output",
) -> Path:
    """Generate a ``.ipynb`` orchestration notebook for the given DAG.

    The notebook contains three code cells:

    1. ``from notebookutils import mssparkutils``
    2. ``DAG = { ... }``  — the full DAG dict as a Python literal
    3. ``mssparkutils.notebook.runMultiple(DAG, {...})``

    Args:
        pipeline_name: Used as the notebook filename
            (``<output_dir>/<pipeline_name>.ipynb``).
        dag: The Fabric-format DAG dictionary produced by
            :func:`meta_driven_dags.builder.build_dag`.
        display_dag_graphviz: Value injected into the
            ``displayDAGViaGraphviz`` option of ``runMultiple``.
        output_dir: Directory where the notebook will be written.
            Created automatically if it does not exist.

    Returns:
        :class:`pathlib.Path` of the generated ``.ipynb`` file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Cell 1: imports ---------------------------------------------------
    cell_imports = new_code_cell(source=_CELL_IMPORTS)

    # --- Cell 2: DAG definition --------------------------------------------
    cell_dag = new_code_cell(source=_format_dag(dag))

    # --- Cell 3: execution -------------------------------------------------
    display_dag_str = "True" if display_dag_graphviz else "False"
    cell_run = new_code_cell(
        source=_CELL_RUN_TEMPLATE.format(display_dag=display_dag_str)
    )

    # --- Assemble notebook -------------------------------------------------
    nb = new_notebook(cells=[cell_imports, cell_dag, cell_run])
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    nb.metadata["language_info"] = {
        "name": "python",
        "version": "3.11.0",
    }

    out_path = output_dir / f"{pipeline_name}.ipynb"
    nbformat.write(nb, str(out_path))

    return out_path
