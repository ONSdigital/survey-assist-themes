from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone, UTC
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import storage


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


_ID_PATTERN = re.compile(r"^STP(\d+)(?:-\d+)?$")


def _normalise_response_id(raw_id: str) -> int:
    """Normalise an ID of the form 'STP00861-01' into an integer.

    Args:
        raw_id: The raw response ID string from the CSV.

    Returns:
        An integer ID suitable for ThemeFinder.

    Raises:
        ValueError: If the ID does not match the expected pattern.
    """
    raw_id = raw_id.strip()

    match = _ID_PATTERN.match(raw_id)
    if not match:
        msg = (
            f"Unable to normalise response ID '{raw_id}'. "
            "Expected format 'STP<digits>' or 'STP<digits>-<digits>'."
        )
        raise ValueError(msg)

    return int(match.group(1))


def _filter_empty_feedback(df: pd.DataFrame, text_col: str) -> pd.DataFrame:
    """Return a copy of ``df`` with rows lacking feedback removed.

    A row is considered to have no feedback if the feedback column is:
    * missing (NaN/None)
    * an empty string or only whitespace
    * the literal string ``'nan'`` (case-insensitive), which can occur when
      missing values are cast to strings.

    Args:
        df: The original DataFrame loaded from CSV.
        text_col: Name of the feedback text column.

    Returns:
        A new DataFrame containing only rows with non-empty feedback.
    """
    # Work on a copy to avoid mutating caller state accidentally.
    cleaned = df.copy()

    # Start from the raw column.
    feedback = cleaned[text_col]

    # Identify actual missing values first.
    is_missing = feedback.isna()

    # For non-missing, normalise to string and strip whitespace.
    as_str = feedback.astype(str).str.strip()
    is_empty_string = as_str.eq("")
    is_literal_nan = as_str.str.lower().eq("nan")

    mask = ~(is_missing | is_empty_string | is_literal_nan)
    return cleaned.loc[mask].copy()


def load_feedback_csv_from_gcs(
    bucket_name: str,
    file_name: str,
    *,
    column_headers: Sequence[str] = ("user", "feedback_comments"),
) -> pd.DataFrame:
    """Load a pipe-delimited CSV from GCS and convert it into a ThemeFinder-
    compatible DataFrame with integer response IDs.

    Rows with no feedback are removed before returning the DataFrame. A row is
    treated as having no feedback if the feedback cell is missing, blank, or
    effectively the string ``'nan'``.

    Args:
        bucket_name: The name of the GCS bucket containing the CSV.
        file_name: The name/path of the CSV file within the bucket.
        column_headers: Two expected CSV column names: the user ID column and
            the feedback text column. Defaults to
            ``("user", "feedback_comments")``.

    Returns:
        A DataFrame with:
            * ``response_id`` – integer IDs suitable for ThemeFinder.
            * ``response`` – free-text feedback as strings.

    Raises:
        FileNotFoundError: If the file does not exist in the bucket.
        ValueError: If expected columns are missing from the CSV, or if any
            response ID cannot be normalised to an integer.
    """
    if len(column_headers) != 2:
        msg = (
            "Exactly two column headers must be provided: "
            "an ID column and a feedback column."
        )
        raise ValueError(msg)

    id_col, text_col = column_headers

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if not blob.exists():
        raise FileNotFoundError(
            f"The file '{file_name}' does not exist in bucket '{bucket_name}'."
        )

    csv_bytes = blob.download_as_bytes()

    # Read the pipe-delimited CSV. Let pandas infer dtypes so that missing
    # values remain proper NaNs rather than strings.
    df = pd.read_csv(
        pd.io.common.BytesIO(csv_bytes),
        sep="|",
    )

    missing = [col for col in (id_col, text_col) if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in CSV: {', '.join(missing)}")

    # Drop rows without any meaningful feedback.
    df = _filter_empty_feedback(df, text_col=text_col)

    # If everything was empty, fail fast with a clear error rather than sending
    # an empty DataFrame into ThemeFinder.
    if df.empty:
        msg = (
            "No rows with non-empty feedback were found in the CSV. "
            "The comments question may have been optional and left blank "
            "by all respondents."
        )
        raise ValueError(msg)

    # Normalise the ID column and build the ThemeFinder schema.
    df["normalised_id"] = df[id_col].astype(str).apply(_normalise_response_id)

    tf_df = pd.DataFrame(
        {
            "response_id": df["normalised_id"].astype(int),
            "response": df[text_col].astype(str),
        }
    )

    return tf_df


def save_themefinder_output_to_gcs(
    output: Mapping[str, Any],
    *,
    bucket_name: str,
    destination_blob_name: str,
) -> None:
    """Save ThemeFinder output as JSON to a Google Cloud Storage bucket.

    The destination path can include "folders" by using ``/`` in the blob name,
    for example::

        destination_blob_name="themefinder/runs/2025-12-05/output.json"

    Args:
        output: The ThemeFinder result dictionary (or any JSON-serialisable
            mapping) to be written.
        bucket_name: Name of the GCS bucket where the JSON file will be stored.
        destination_blob_name: Name of the blob within the bucket, including
            any folder-like prefixes.

    Raises:
        FileNotFoundError: If the bucket does not exist.
        TypeError: If ``output`` is not JSON-serialisable.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    if not bucket.exists():
        msg = f"The bucket '{bucket_name}' does not exist."
        raise FileNotFoundError(msg)

    blob = bucket.blob(destination_blob_name)

    serialised = themefinder_output_to_serialisable(output)

    json_text = json.dumps(serialised, ensure_ascii=False, indent=2)
    blob.upload_from_string(
        json_text,
        content_type="application/json",
    )


def make_timestamped_blob_name(prefix: str = "themefinder_output") -> str:
    """Return a timestamped blob name for storing JSON outputs in GCS.

    The returned string follows the pattern:
        ``<prefix>_YYYYMMDD_HHMMSS.json``

    Example:
        themefinder_output_20251205_142355.json

    Args:
        prefix: The filename prefix to use before the timestamp.

    Returns:
        A string representing the timestamped blob name.
    """
    now = datetime.now(UTC)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}.json"