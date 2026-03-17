"""Command-line interface for the metadata-driven DAG generator.

Each command is a thin I/O wrapper:
  - parse CLI arguments
  - call the relevant service function
  - format output / set exit code

Business logic lives in the service modules:
  - :mod:`fabric_metadata_dags.pipeline`  — load / validate / generate
  - :mod:`fabric_metadata_dags.scaffold`  — scaffold new pipeline YAML
  - :mod:`fabric_metadata_dags.linter`    — lint pipeline YAML

Usage::

    # Generate notebooks
    generate-pipeline generate metadata/sales_pipeline.yaml [options]
    generate-pipeline generate [options]   # all *.yaml in metadata/
    generate-pipeline generate --metadata-dir pipelines/ --output-dir generated/

    # Lint pipelines for warnings
    generate-pipeline lint metadata/sales_pipeline.yaml
    generate-pipeline lint   # all *.yaml in metadata/

    # Scaffold a new pipeline YAML
    generate-pipeline init my_pipeline
    generate-pipeline init my_pipeline --activities 3 --metadata-dir pipelines/
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler

from fabric_metadata_dags.linter import LintSeverity, lint_pipeline
from fabric_metadata_dags.loader import load_pipeline
from fabric_metadata_dags.pipeline import run_pipeline
from fabric_metadata_dags.scaffold import scaffold_pipeline

app = typer.Typer(
    name="generate-pipeline",
    help="Generate Microsoft Fabric orchestration notebooks from YAML pipeline metadata.",
    add_completion=False,  # disable shell-completion install prompt
    no_args_is_help=True,
)

console = Console(stderr=True)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared CLI utilities
# ---------------------------------------------------------------------------


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)],
    )


def _resolve_yaml_paths(yaml_path: Optional[Path], metadata_dir: Path) -> list[Path]:
    """Return the ordered list of pipeline YAML files to process."""
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


@app.command("generate")
def generate(
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
    include_run_cell: bool = typer.Option(
        True,
        "--run-cell/--no-run-cell",
        help="Prepend a %%run AquaVilla_Functions cell at the top of the notebook.",
    ),
    run_lint: bool = typer.Option(
        False,
        "--lint",
        help="Run linting after generation and print warnings (non-blocking).",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Enable verbose (DEBUG) logging.",
    ),
) -> None:
    """Generate Fabric orchestration notebooks from pipeline YAML metadata."""
    _setup_logging(verbose)

    yaml_paths = _resolve_yaml_paths(yaml_path, metadata_dir)

    failed: list[Path] = []
    for path in yaml_paths:
        try:
            out = run_pipeline(path, output_dir, display_dag, include_run_cell)
            console.print(f"[green]✓[/green] Generated: [bold]{out}[/bold]")
            if run_lint:
                _print_lint_warnings(path)
        except (FileNotFoundError, ValueError, OSError) as exc:
            logger.error("%s: %s", path.name, exc)
            failed.append(path)

    if failed:
        console.print(
            f"\n[red]✗ {len(failed)} pipeline(s) failed:[/red] "
            + ", ".join(str(p) for p in failed)
        )
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# lint command
# ---------------------------------------------------------------------------


@app.command("lint")
def lint(
    yaml_path: Optional[Path] = typer.Argument(
        default=None,
        help=(
            "Path to a specific pipeline YAML file. "
            "When omitted, all *.yaml / *.yml files inside --metadata-dir are linted."
        ),
        show_default=False,
    ),
    metadata_dir: Path = typer.Option(
        Path("metadata"),
        "--metadata-dir",
        metavar="DIR",
        help="Directory scanned for pipelines when no YAML_PATH is given.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Enable verbose (DEBUG) logging.",
    ),
) -> None:
    """Lint pipeline YAML files and report warnings."""
    _setup_logging(verbose)

    yaml_paths = _resolve_yaml_paths(yaml_path, metadata_dir)

    any_warnings = any(_print_lint_warnings(path) for path in yaml_paths)

    if not any_warnings:
        console.print("[green]✓[/green] No lint warnings found.")


def _print_lint_warnings(yaml_path: Path) -> bool:
    """Load *yaml_path*, run linting, and print any warnings to the console.

    Returns ``True`` if any warnings were printed.
    """
    try:
        pipeline = load_pipeline(yaml_path)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Failed to load %s: %s", yaml_path.name, exc)
        return False

    warnings = lint_pipeline(pipeline)
    if not warnings:
        return False

    console.print(f"\n[bold]{yaml_path}[/bold]")
    for w in warnings:
        severity_style = "yellow" if w.severity == LintSeverity.WARNING else "cyan"
        activity_label = f"  [dim]{w.activity_name}[/dim]" if w.activity_name else ""
        console.print(
            f"  [{severity_style}]{w.severity.value}[/{severity_style}] "
            f"[dim]{w.code}[/dim]{activity_label}  {w.message}"
        )
    return True


# ---------------------------------------------------------------------------
# init command
# ---------------------------------------------------------------------------


@app.command("init")
def init(
    name: str = typer.Argument(help="Pipeline name — becomes the output filename."),
    activities: int = typer.Option(
        2,
        "--activities",
        metavar="N",
        min=1,
        help="Number of activity stubs to scaffold (second depends on first).",
    ),
    metadata_dir: Path = typer.Option(
        Path("metadata"),
        "--metadata-dir",
        metavar="DIR",
        help="Directory where the new YAML file will be written.",
    ),
) -> None:
    """Scaffold a new pipeline YAML file with commented boilerplate."""
    try:
        output_path = scaffold_pipeline(name, activities, metadata_dir)
        console.print(f"[green]✓[/green] Created: [bold]{output_path}[/bold]")
    except FileExistsError as exc:
        console.print(
            f"[red]✗[/red] File already exists: [bold]{exc}[/bold]\n"
            "  Remove it first or choose a different name."
        )
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
