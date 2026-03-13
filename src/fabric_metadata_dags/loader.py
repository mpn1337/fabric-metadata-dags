"""Load pipeline metadata from YAML files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_pipeline(path: Path | str) -> dict[str, Any]:
    """Parse a YAML pipeline metadata file and return the raw dict.

    Args:
        path: Path to the ``.yaml`` / ``.yml`` file.

    Returns:
        The parsed pipeline dictionary.

    Raises:
        FileNotFoundError: If *path* does not exist.
        yaml.YAMLError: If the file cannot be parsed.
        ValueError: If the file is empty or not a mapping.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Pipeline metadata file not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if not isinstance(data, dict):
        raise ValueError(
            f"Expected a YAML mapping at the top level, got {type(data).__name__}: {path}"
        )

    return data
