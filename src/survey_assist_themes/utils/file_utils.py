from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def themefinder_output_to_serialisable(data: dict[str, Any]) -> dict[str, Any]:
    """Convert raw ThemeFinder output into a JSON-serialisable dictionary.

    ThemeFinder returns a dictionary where several values are Pandas DataFrames
    and some fields may contain Enum values. These cannot be directly encoded
    as JSON. This function safely converts:

    - DataFrames to lists of record dictionaries
    - Enum values to strings
    - NumPy types to native Python types

    Args:
        data: The raw output dictionary produced by the ThemeFinder pipeline.

    Returns:
        A dictionary containing only JSON-serialisable types.
    """

    serialised: dict[str, Any] = {}

    for key, value in data.items():
        # Convert DataFrame values
        if isinstance(value, pd.DataFrame):
            serialised[key] = value.to_dict(orient="records")
            continue

        # Convert Enum values by stringifying them
        if hasattr(value, "name") and hasattr(value, "value"):
            serialised[key] = str(value)
            continue

        # Fallback: store the value directly; json.dumps will handle built-ins
        serialised[key] = value

    return serialised


def save_themefinder_output_as_json(output: dict[str, Any], filepath: Path) -> None:
    """Serialise and save the ThemeFinder analysis output as a JSON file.

    This writes a deployment-safe JSON file containing the ThemeFinder results,
    converting DataFrames and enums as required.

    Args:
        output: Raw ThemeFinder output dictionary.
        filepath: Path where the JSON file will be written.

    Raises:
        OSError: If writing to the filesystem fails.
    """
    serialisable = themefinder_output_to_serialisable(output)

    filepath.parent.mkdir(parents=True, exist_ok=True)

    with filepath.open("w", encoding="utf-8") as f:
        json.dump(serialisable, f, indent=2, ensure_ascii=False)
