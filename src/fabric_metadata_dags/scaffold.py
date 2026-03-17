"""Pipeline scaffold service.

Generates annotated boilerplate YAML files for new pipelines.
This module contains no CLI or presentation logic.
"""

from __future__ import annotations

from pathlib import Path

_PIPELINE_TEMPLATE = """\
# Pipeline name — used as the output notebook filename.
pipeline: {name}

settings:
  concurrency: 10          # max notebooks running in parallel
  timeoutInSeconds: 21600  # total pipeline timeout in seconds (21600 = 6 h)

# Default values inherited by all activities unless overridden at activity level.
defaults:
  retry: 1
  retryIntervalInSeconds: 30
  timeoutPerCellInSeconds: 90

activities:
{activities}"""

_ACTIVITY_TEMPLATE = """\
  - name: {name}
    path: /notebooks/your/notebook/path
{extras}"""


def scaffold_pipeline(
    name: str,
    num_activities: int = 2,
    metadata_dir: Path = Path("metadata"),
) -> Path:
    """Write a new annotated pipeline YAML skeleton to *metadata_dir*.

    The first activity has no dependencies. Each subsequent activity depends on
    the one before it, forming a simple linear chain that users can reshape.

    Args:
        name: Pipeline name — used as both the ``pipeline:`` key and the filename.
        num_activities: Number of activity stubs to generate. Must be >= 1.
        metadata_dir: Directory where the ``.yaml`` file will be written.
            Created automatically if it does not exist.

    Returns:
        The :class:`~pathlib.Path` of the newly created YAML file.

    Raises:
        FileExistsError: If ``<metadata_dir>/<name>.yaml`` already exists.
    """
    output_path = metadata_dir / f"{name}.yaml"

    if output_path.exists():
        raise FileExistsError(output_path)

    metadata_dir.mkdir(parents=True, exist_ok=True)

    activity_blocks: list[str] = []
    for i in range(num_activities):
        activity_name = f"activity_{i + 1}"
        extras = "" if i == 0 else f"    dependencies:\n      - activity_{i}\n"
        activity_blocks.append(
            _ACTIVITY_TEMPLATE.format(name=activity_name, extras=extras)
        )

    content = _PIPELINE_TEMPLATE.format(
        name=name,
        activities="\n".join(activity_blocks),
    )
    output_path.write_text(content, encoding="utf-8")
    return output_path
