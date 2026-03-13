"""Command-line interface for the metadata-driven DAG generator.

Usage::

    # Single file
    generate-pipeline metadata/sales_pipeline.yaml [options]

    # All YAML files in the default metadata directory
    generate-pipeline [options]

    # All YAML files in a custom directory
    generate-pipeline --metadata-dir pipelines/ [options]

Options
-------
--metadata-dir DIR      Directory scanned for *.yaml / *.yml when no file is given (default: metadata/).
--display-dag           Enable ``displayDAGViaGraphviz`` in the notebook.
--output-dir DIR        Directory for the generated notebook (default: output/).
--verbose               Enable DEBUG-level logging.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from fabric_metadata_dags.builder import build_dag
from fabric_metadata_dags.config import resolve_activity
from fabric_metadata_dags.generator import generate_notebook
from fabric_metadata_dags.loader import load_pipeline
from fabric_metadata_dags.validator import validate_dag, validate_pipeline_schema

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="generate-pipeline",
        description="Generate Microsoft Fabric orchestration notebook(s) from YAML pipeline metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Process a single pipeline\n"
            "  generate-pipeline metadata/sales_pipeline.yaml\n\n"
            "  # Process ALL *.yaml files in metadata/\n"
            "  generate-pipeline\n\n"
            "  # Process ALL *.yaml files in a custom directory\n"
            "  generate-pipeline --metadata-dir pipelines/ --output-dir generated/\n\n"
            "  # Single file with options\n"
            "  generate-pipeline metadata/sales_pipeline.yaml --display-dag --verbose\n"
        ),
    )

    parser.add_argument(
        "yaml_path",
        metavar="YAML_PATH",
        nargs="?",
        type=Path,
        default=None,
        help=(
            "Path to a specific pipeline YAML file. "
            "When omitted, all *.yaml / *.yml files inside --metadata-dir are processed."
        ),
    )
    parser.add_argument(
        "--metadata-dir",
        metavar="DIR",
        type=Path,
        default=Path("metadata"),
        help="Directory scanned for pipelines when no YAML_PATH is given (default: metadata/).",
    )
    parser.add_argument(
        "--display-dag",
        action="store_true",
        default=False,
        help="Set displayDAGViaGraphviz=True in the generated notebook (default: False).",
    )
    parser.add_argument(
        "--output-dir",
        metavar="DIR",
        type=Path,
        default=Path("output"),
        help="Directory where generated .ipynb files will be saved (default: output/).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable verbose (DEBUG) logging.",
    )

    return parser


def _resolve_yaml_paths(args: argparse.Namespace) -> list[Path]:
    """Return the ordered list of YAML files to process.

    - If a specific file was supplied on the command line, return only that.
    - Otherwise, scan ``--metadata-dir`` for ``*.yaml`` and ``*.yml`` files
      (sorted alphabetically for deterministic output).

    Exits with a helpful message if no files can be found.
    """
    if args.yaml_path is not None:
        return [args.yaml_path]

    metadata_dir: Path = args.metadata_dir
    if not metadata_dir.is_dir():
        logger.error(
            "Metadata directory not found: %s  "
            "(pass a specific YAML file or --metadata-dir to override)",
            metadata_dir,
        )
        sys.exit(1)

    paths = sorted(list(metadata_dir.glob("*.yaml")) + list(metadata_dir.glob("*.yml")))
    if not paths:
        logger.error("No *.yaml / *.yml files found in: %s", metadata_dir)
        sys.exit(1)

    logger.info("Discovered %d pipeline file(s) in %s", len(paths), metadata_dir)
    return paths


def _process_one(yaml_path: Path, args: argparse.Namespace) -> Path | None:
    """Load, validate, and generate a notebook for a single YAML file.

    Returns the output path on success, or ``None`` on failure (error already
    logged — caller decides whether to abort or continue).
    """
    logger.info("Processing: %s", yaml_path)

    # --- Load ---------------------------------------------------------------
    try:
        pipeline = load_pipeline(yaml_path)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Failed to load metadata: %s", exc)
        return None

    pipeline_name: str = pipeline.get("pipeline", yaml_path.stem)
    logger.debug("Pipeline name: %s", pipeline_name)

    # --- Schema validation (unknown keys) ----------------------------------
    logger.debug("Validating pipeline schema...")
    try:
        validate_pipeline_schema(pipeline)
    except ValueError as exc:
        logger.error("Schema validation failed: %s", exc)
        return None

    # --- Resolve configuration ----------------------------------------------
    pipeline_defaults: dict = pipeline.get("defaults", {})
    raw_activities: list[dict] = pipeline.get("activities", [])

    logger.debug(
        "Applying 3-tier configuration resolution across %d activities.",
        len(raw_activities),
    )
    resolved_activities = [
        resolve_activity(act, pipeline_defaults) for act in raw_activities
    ]

    # --- Validate -----------------------------------------------------------
    logger.info("Validating pipeline DAG...")
    try:
        validate_dag(resolved_activities)
    except ValueError as exc:
        logger.error("DAG validation failed: %s", exc)
        return None
    logger.info("DAG validation passed.")

    # --- Build + generate ---------------------------------------------------
    dag = build_dag(pipeline, resolved_activities)
    logger.debug("Built DAG:\n%s", dag)

    logger.info("Generating notebook → %s/%s.ipynb", args.output_dir, pipeline_name)
    try:
        out_path = generate_notebook(
            pipeline_name=pipeline_name,
            dag=dag,
            display_dag_graphviz=args.display_dag,
            output_dir=args.output_dir,
        )
    except OSError as exc:
        logger.error("Failed to write notebook: %s", exc)
        return None

    return out_path


def main(argv: list[str] | None = None) -> None:
    """Entry point for the ``generate-pipeline`` CLI command."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    # --- Logging setup ------------------------------------------------------
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(levelname)-8s %(message)s",
        stream=sys.stderr,
    )

    logger.debug("Arguments: %s", args)

    # --- Resolve files to process -------------------------------------------
    yaml_paths = _resolve_yaml_paths(args)

    # --- Process each pipeline ----------------------------------------------
    failed: list[Path] = []
    for yaml_path in yaml_paths:
        out_path = _process_one(yaml_path, args)
        if out_path is None:
            failed.append(yaml_path)
        else:
            print(str(out_path))

    if failed:
        logger.error(
            "%d pipeline(s) failed: %s",
            len(failed),
            ", ".join(str(p) for p in failed),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
