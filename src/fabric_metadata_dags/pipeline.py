"""Pipeline execution service.

Orchestrates the full load → validate → resolve → build → generate pipeline.
This module contains no CLI or presentation logic; it can be called directly
from tests, scripts, or other tools without importing Typer or Rich.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fabric_metadata_dags.builder import build_dag
from fabric_metadata_dags.config import resolve_activity
from fabric_metadata_dags.generator import generate_notebook
from fabric_metadata_dags.loader import load_pipeline
from fabric_metadata_dags.validator import validate_dag, validate_pipeline_schema

logger = logging.getLogger(__name__)


def run_pipeline(
    yaml_path: Path,
    output_dir: Path,
    display_dag: bool = False,
    include_run_cell: bool = True,
) -> Path:
    """Load, validate, and generate a notebook for a single pipeline YAML file.

    Args:
        yaml_path: Path to the pipeline YAML metadata file.
        output_dir: Directory where the generated ``.ipynb`` file will be written.
        display_dag: Whether to enable ``displayDAGViaGraphviz`` in the notebook.
        include_run_cell: Whether to prepend a ``%run`` cell to the notebook.

    Returns:
        The :class:`~pathlib.Path` of the generated notebook.

    Raises:
        FileNotFoundError: If *yaml_path* does not exist.
        ValueError: If the YAML fails schema or DAG validation.
        OSError: If the notebook cannot be written to *output_dir*.
    """
    logger.info("Processing: %s", yaml_path)

    pipeline: dict[str, Any] = load_pipeline(yaml_path)
    pipeline_name: str = pipeline.get("pipeline", yaml_path.stem)
    logger.debug("Pipeline name: %s", pipeline_name)

    logger.debug("Validating pipeline schema...")
    validate_pipeline_schema(pipeline)

    pipeline_defaults: dict = pipeline.get("defaults", {})
    raw_activities: list[dict] = pipeline.get("activities", [])
    logger.debug("Resolving config across %d activities.", len(raw_activities))
    resolved_activities = [
        resolve_activity(a, pipeline_defaults) for a in raw_activities
    ]

    logger.info("Validating pipeline DAG...")
    validate_dag(resolved_activities)
    logger.info("DAG validation passed.")

    dag = build_dag(pipeline, resolved_activities)
    logger.debug("Built DAG: %s", dag)

    logger.info("Generating notebook → %s/%s.ipynb", output_dir, pipeline_name)
    return generate_notebook(
        pipeline_name=pipeline_name,
        dag=dag,
        display_dag_graphviz=display_dag,
        output_dir=output_dir,
        include_run_cell=include_run_cell,
    )
