"""Command-line interface for the metadata-driven DAG generator.

Usage::

    # Single file
    generate-pipeline metadata/sales_pipeline.yaml [options]

    # All YAML files in the default metadata/ directory
    generate-pipeline [options]

    # All YAML files in a custom directory
    generate-pipeline --metadata-dir pipelines/ --output-dir generated/
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler

from fabric_metadata_dags.builder import build_dag
from fabric_metadata_dags.config import resolve_activity
from fabric_metadata_dags.generator import generate_notebook
from fabric_metadata_dags.loader import load_pipeline
from fabric_metadata_dags.validator import validate_dag, validate_pipeline_schema

app = typer.Typer(
    name="generate-pipeline",
    help="Generate Microsoft Fabric orchestration notebooks from YAML pipeline metadata.",
    add_completion=False,  # disable shell-completion install prompt
)

console = Console(stderr=True)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)],
    )


def _resolve_yaml_paths(yaml_path: Optional[Path], metadata_dir: Path) -> list[Path]:
    """Return ordered list of YAML files to process."""
    if yaml_path is not None:
        return [yaml_path]

    if not metadata_dir.is_dir():
        console.print(
            f"[red]✗[/red] Metadata directory not found: [bold]{metadata_dir}[/bold]\n"
            "  Pass a specific YAML file or --metadata-dir to override.",
        )
        raise typer.Exit(code=1)

    paths = sorted(metadata_dir.glob("*.yaml")) + sorted(metadata_dir.glob("*.yml"))
    if not paths:
        console.print(
            f"[red]✗[/red] No *.yaml / *.yml files found in: [bold]{metadata_dir}[/bold]"
        )
        raise typer.Exit(code=1)

    logger.info("Discovered %d pipeline file(s) in %s", len(paths), metadata_dir)
    return paths


def _process_one(
    yaml_path: Path,
    output_dir: Path,
    display_dag: bool,
) -> Path | None:
    """Load, validate, and generate a notebook for a single YAML file.

    Returns the output path on success, ``None`` on failure.
    """
    logger.info("Processing: %s", yaml_path)

    # Load ------------------------------------------------------------------
    try:
        pipeline = load_pipeline(yaml_path)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Failed to load metadata: %s", exc)
        return None

    pipeline_name: str = pipeline.get("pipeline", yaml_path.stem)
    logger.debug("Pipeline name: %s", pipeline_name)

    # Schema validation -----------------------------------------------------
    logger.debug("Validating pipeline schema...")
    try:
        validate_pipeline_schema(pipeline)
    except ValueError as exc:
        logger.error("Schema validation failed: %s", exc)
        return None

    # Config resolution -----------------------------------------------------
    pipeline_defaults: dict = pipeline.get("defaults", {})
    raw_activities: list[dict] = pipeline.get("activities", [])
    logger.debug("Resolving config across %d activities.", len(raw_activities))
    resolved_activities = [
        resolve_activity(a, pipeline_defaults) for a in raw_activities
    ]

    # DAG validation --------------------------------------------------------
    logger.info("Validating pipeline DAG...")
    try:
        validate_dag(resolved_activities)
    except ValueError as exc:
        logger.error("DAG validation failed: %s", exc)
        return None
    logger.info("DAG validation passed.")

    # Build + generate ------------------------------------------------------
    dag = build_dag(pipeline, resolved_activities)
    logger.debug("Built DAG: %s", dag)

    logger.info("Generating notebook → %s/%s.ipynb", output_dir, pipeline_name)
    try:
        return generate_notebook(
            pipeline_name=pipeline_name,
            dag=dag,
            display_dag_graphviz=display_dag,
            output_dir=output_dir,
        )
    except OSError as exc:
        logger.error("Failed to write notebook: %s", exc)
        return None


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------


@app.command()
def main(
    yaml_path: Optional[Path] = typer.Argument(
        default=None,
        help=(
            "Path to a specific pipeline YAML file. "
            "When omitted, all *.yaml / *.yml files inside --metadata-dir are processed."
        ),
        show_default=False,
    ),
    metadata_dir: Path = typer.Option(
        Path("metadata"),
        "--metadata-dir",
        metavar="DIR",
        help="Directory scanned for pipelines when no YAML_PATH is given.",
    ),
    output_dir: Path = typer.Option(
        Path("output"),
        "--output-dir",
        metavar="DIR",
        help="Directory where generated .ipynb files will be saved.",
    ),
    display_dag: bool = typer.Option(
        False,
        "--display-dag",
        help="Set displayDAGViaGraphviz=True in the generated notebook.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Enable verbose (DEBUG) logging.",
    ),
) -> None:
    _setup_logging(verbose)
    logger.debug(
        "yaml_path=%s  metadata_dir=%s  output_dir=%s  display_dag=%s",
        yaml_path,
        metadata_dir,
        output_dir,
        display_dag,
    )

    yaml_paths = _resolve_yaml_paths(yaml_path, metadata_dir)

    failed: list[Path] = []
    for path in yaml_paths:
        out = _process_one(path, output_dir, display_dag)
        if out is None:
            failed.append(path)
        else:
            console.print(f"[green]✓[/green] Generated: [bold]{out}[/bold]")

    if failed:
        console.print(
            f"\n[red]✗ {len(failed)} pipeline(s) failed:[/red] "
            + ", ".join(str(p) for p in failed)
        )
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
