"""Utilities for file operations with Google Cloud Storage and data processing.

This module provides functions for loading CSV files from GCS, transforming
data for ThemeFinder, and saving ThemeFinder output back to GCS.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from survey_assist_utils.logging import get_logger

from survey_assist_themes.exceptions import (
    DataProcessingError,
    GCSOperationError,
    ThemeFinderError,
)
from survey_assist_themes.utils.retry import retry_with_backoff

logger = get_logger(__name__)


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


def _build_id_mapping(df: pd.DataFrame, *, original_id_col: str) -> pd.DataFrame:
    """Maps Original Source ID(s) to integer response IDs and participant keys in a DataFrame.
    Each input row receives a unique response_id, while identical original IDs
    share the same participant_key.

    Args:
        df: The original DataFrame loaded from CSV.
        original_id_col: The name of the column containing original respondent IDs.
    Returns:
    A DataFrame with columns:
        - response_id: Integer IDs suitable for ThemeFinder (1-indexed).
        - participant_key: Integer keys representing unique participants (1-indexed).
        - original_id: The original respondent IDs from the CSV, as strings.
    Raises:
        ValueError: If the ID does not match the expected pattern.
    """
    original_id = df[original_id_col].astype(str).str.strip()

    _validate_ids(original_id, column_name=original_id_col)
    logger.info("ID(s) have been successfully validated")

    #  Log if there are any duplicate original IDs
    duplicate_mask = original_id.duplicated(keep=False)
    if duplicate_mask.any():
        duplicates = original_id[duplicate_mask].value_counts()
        num_duplicate_ids = len(duplicates)
        logger.info(f"Found {num_duplicate_ids} duplicate original ID(s)")

    response_id = pd.RangeIndex(start=1, stop=len(df) + 1)  # 1 index for ThemeFinder compatibility
    codes, _ = pd.factorize(original_id, sort=False)
    participant_key = codes + 1  # 1 index for ThemeFinder compatibility

    mapping_df = pd.DataFrame(
        {
            "response_id": response_id,
            "participant_key": participant_key,
            "original_id": original_id,
        }
    )
    return mapping_df


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


def _validate_ids(original_id: pd.Series, *, column_name: str) -> None:
    """
    Validates that all ID(s) contain only letters, numbers, or hyphens.
    """
    logger.debug("Validating ID(s)")
    valid_mask = original_id.str.fullmatch(r"[A-Za-z0-9-]+")
    if (~valid_mask).any():
        invalid_ids = original_id[~valid_mask].unique()
        msg = (
            f"Invalid response ID(s) found: {invalid_ids.tolist()}. "
            "IDs must contain only letters, numbers, or hyphens."
        )
        raise ValueError(msg)


@retry_with_backoff(
    max_attempts=3,
    initial_delay=1.0,
    backoff_factor=2.0,
    exceptions=(GoogleCloudError, IOError),
)
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
        msg = "Exactly two column headers must be provided: " "an ID column and a feedback column."
        logger.error(msg)
        raise ValueError(msg)

    id_col, text_col = column_headers

    logger.debug(f"Loading CSV from GCS: bucket={bucket_name}, file={file_name}")
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        if not blob.exists():
            msg = f"The file '{file_name}' does not exist in bucket '{bucket_name}'."
            logger.error(msg)
            raise FileNotFoundError(msg)

        csv_bytes = blob.download_as_bytes()
        logger.debug(f"Downloaded {len(csv_bytes)} bytes from GCS")
    except GoogleCloudError as e:
        logger.error(f"GCS operation failed: {e}", exc_info=True)
        raise GCSOperationError(f"Failed to load file from GCS: {e}") from e

    # Read the pipe-delimited CSV. Let pandas infer dtypes so that missing
    # values remain proper NaNs rather than strings.
    df = pd.read_csv(
        pd.io.common.BytesIO(csv_bytes),
        sep="|",
    )

    missing = [col for col in (id_col, text_col) if col not in df.columns]
    if missing:
        msg = f"Missing required columns in CSV: {', '.join(missing)}"
        logger.error(msg)
        raise DataProcessingError(msg)

    logger.debug(f"CSV loaded with {len(df)} rows, columns: {list(df.columns)}")

    # Drop rows without any meaningful feedback.
    df = _filter_empty_feedback(df, text_col=text_col)
    logger.debug(f"After filtering empty feedback: {len(df)} rows remaining")

    # If everything was empty, fail fast with a clear error rather than sending
    # an empty DataFrame into ThemeFinder.
    if df.empty:
        msg = (
            "No rows with non-empty feedback were found in the CSV. "
            "The comments question may have been optional and left blank "
            "by all respondents."
        )
        logger.error(msg)
        raise DataProcessingError(msg)

    id_mapping_df = _build_id_mapping(df, original_id_col=id_col)

    tf_df = pd.DataFrame(
        {
            "response_id": id_mapping_df["response_id"].astype(int),
            "response": df[text_col].astype(str),
        }
    )

    return {"tf_df": tf_df, "id_mapping": id_mapping_df}


@retry_with_backoff(
    max_attempts=3,
    initial_delay=1.0,
    backoff_factor=2.0,
    exceptions=(GoogleCloudError, IOError),
)
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
    logger.debug(f"Saving output to GCS: bucket={bucket_name}, " f"blob={destination_blob_name}")
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        if not bucket.exists():
            msg = f"The bucket '{bucket_name}' does not exist."
            logger.error(msg)
            raise FileNotFoundError(msg)

        blob = bucket.blob(destination_blob_name)

        serialised = themefinder_output_to_serialisable(dict(output))

        json_text = json.dumps(serialised, ensure_ascii=False, indent=2)
        blob.upload_from_string(
            json_text,
            content_type="application/json",
        )
        logger.info(
            f"Successfully saved {len(json_text)} bytes to GCS: "
            f"{bucket_name}/{destination_blob_name}"
        )
    except GoogleCloudError as e:
        logger.error(f"GCS operation failed: {e}", exc_info=True)
        raise GCSOperationError(f"Failed to save output to GCS: {e}") from e


@retry_with_backoff(
    max_attempts=3,
    initial_delay=1.0,
    backoff_factor=2.0,
    exceptions=(GoogleCloudError, IOError),
)
def load_json_from_gcs(bucket_name: str, blob_name: str) -> dict[str, Any]:
    """Load JSON from a Google Cloud Storage blob.

    Reads a JSON file stored in GCS and returns the parsed dictionary. The
    blob name may include folder-like prefixes (for example
    "themefinder/runs/2025-12-05/output.json").

    Args:
        bucket_name: Name of the GCS bucket containing the JSON blob.
        blob_name: Path/name of the blob within the bucket.

    Returns:
        The parsed JSON content as a dictionary.

    Raises:
        GCSOperationError: If any GCS operation fails (for example if the bucket
            or blob cannot be accessed or downloaded).
        ThemeFinderError: If the downloaded content cannot be parsed as JSON.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        raw = blob.download_as_text()
        logger.info(f"Loaded JSON from gs://{bucket_name}/{blob_name}")
    except Exception as e:
        raise GCSOperationError(
            f"Failed to load JSON from gs://{bucket_name}/{blob_name}: {e}"
        ) from e

    try:
        return json.loads(raw)  # type: ignore[no-any-return]
    except json.JSONDecodeError as e:
        raise ThemeFinderError(f"Could not parse JSON: {e}") from e


@retry_with_backoff(
    max_attempts=3,
    initial_delay=1.0,
    backoff_factor=2.0,
    exceptions=(GoogleCloudError, IOError),
)
def save_markdown_report_to_gcs(
    report: str,
    *,
    bucket_name: str,
    destination_blob_name: str,
    ensure_md_extension: bool = True,
) -> None:
    """Save a markdown report to a Google Cloud Storage bucket.

    The `report` argument must be a markdown string. If `ensure_md_extension`
    is True the destination blob name will be suffixed with `.md` if it does
    not already end with that extension.

    Args:
        report: Markdown content as a string.
        bucket_name: Name of the GCS bucket where the markdown file will be stored.
        destination_blob_name: Name of the blob within the bucket, including
            any folder-like prefixes.
        ensure_md_extension: If True, append ".md" to destination name when missing.

    Raises:
        TypeError: If `report` is not a string.
        FileNotFoundError: If the bucket does not exist.
    """
    if not isinstance(report, str):
        msg = "The 'report' argument must be a string containing markdown content."
        logger.error(msg)
        raise TypeError(msg)

    report_text = report

    if ensure_md_extension and not destination_blob_name.lower().endswith(".md"):
        destination_blob_name = destination_blob_name + ".md"

    logger.debug(f"Saving markdown report to GCS: bucket={bucket_name}, \
                 blob={destination_blob_name}")
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        if not bucket.exists():
            msg = f"The bucket '{bucket_name}' does not exist."
            logger.error(msg)
            raise FileNotFoundError(msg)

        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(
            report_text,
            content_type="text/markdown; charset=utf-8",
        )
        logger.info(
            f"Successfully saved markdown report ({len(report_text)} bytes) to GCS: "
            f"{bucket_name}/{destination_blob_name}"
        )
    except GoogleCloudError as e:
        logger.error(f"GCS operation failed: {e}", exc_info=True)
        raise GCSOperationError(f"Failed to save markdown report to GCS: {e}") from e


def make_timestamped_blob_names(
    output_prefix: str = "themefinder_output",
) -> tuple[str, str]:
    """Return a timestamped blob name for storing JSON outputs in GCS.

    The returned string follows the pattern:
        ``<prefix>_YYYYMMDD_HHMMSS.json``

    Example:
        themefinder_output_20251205_142355.json
        themefinder_output_20251205_142355_mapping.json

    Args:
        output_prefix: The filename prefix to use before the timestamp for the output JSON file.

    Returns:
        A tuple of strings representing the timestamped blob names
        for the output and mapping JSON files.
    """
    now = datetime.now(UTC)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    output_name = f"{output_prefix}_{timestamp}.json"
    mapping_name = f"{output_prefix}_{timestamp}_mapping.json"
    return output_name, mapping_name


def build_theme_table_df(result: dict[str, Any], id_mapping_df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a theme table from the ThemeFinder result and ID mapping DataFrame.

    Args:
        result: The ThemeFinder result dictionary.
        id_mapping_df: DataFrame containing the ID mappings.

    Returns:
        A DataFrame representing the theme table.
    """
    mapping_df = pd.DataFrame(result["mapping"]).explode("labels")
    themes_df = pd.DataFrame(result["themes"]).rename(columns={"topic": "theme_description"})
    id_lookup_df = id_mapping_df[["response_id", "original_id"]].copy()

    theme_table = (
        mapping_df.rename(columns={"labels": "topic_id"})
        .merge(
            themes_df[["topic_id", "theme_description"]],
            on="topic_id",
            how="left",
        )
        .merge(id_lookup_df, on="response_id", how="left")
    )

    return theme_table[["response_id", "original_id", "response", "theme_description", "topic_id"]]


def save_theme_csvs_to_gcs(
    result: dict[str, Any], id_mapping_df: pd.DataFrame, bucket_name: str, output_name: str
) -> None:
    """
    Save theme tables as CSV files to a Google Cloud Storage bucket.

    Args:
        result: The ThemeFinder result dictionary.
        id_mapping_df: DataFrame containing the ID mappings.
        bucket_name: Name of the GCS bucket where the CSV files will be stored.
        output_name: Base name for the output CSV files; theme-specific suffixes will be added.

    Raises:
        DataProcessingError: If the theme table is empty after processing.

    """
    theme_table = build_theme_table_df(result, id_mapping_df)

    if theme_table.empty:
        raise DataProcessingError("Theme table is empty; no CSVs to save to GCS")

    base_name, _ = os.path.splitext(output_name)
    logger.debug(f"Saving output to GCS: bucket={bucket_name}, " f"blob={base_name}")
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        for topic_id, df in theme_table.groupby("topic_id"):
            csv_name = f"{base_name}_theme_{topic_id}.csv"
            blob = bucket.blob(f"output/{csv_name}")

            csv_df = df[["response_id", "original_id", "response", "theme_description"]].copy()

            csv_content = csv_df.to_csv(index=False)

            blob.upload_from_string(
                csv_content.encode("utf-8-sig"),
                content_type="text/csv; charset=utf-8",
            )

    except GoogleCloudError as e:
        logger.error(f"GCS operation failed: {e}", exc_info=True)
        raise GCSOperationError(f"Failed to save output to GCS: {e}") from e
