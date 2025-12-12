"""Themes Cloud Run Job - Runs ThemeFinder analysis on survey responses."""

from __future__ import annotations

import asyncio
import os

from survey_assist_utils.logging import get_logger

from survey_assist_themes.demo_themefinder_vertexai import run_demo
from survey_assist_themes.exceptions import ConfigurationError

logger = get_logger(__name__)


async def run_analysis() -> None:
    """Run ThemeFinder analysis pipeline.

    Maps Cloud Run Job environment variables to the format expected by
    run_demo() and delegates to the main ThemeFinder analysis function.

    Environment variables:
        STAGING_BUCKET or INPUT_BUCKET: GCS bucket containing input CSV
        OUTPUT_BUCKET: GCS bucket for saving results
        STAGING_FILE or INPUT_FILE: Path to CSV file (default: "input/example_feedback_v2.csv")
        QUESTION: Survey question being evaluated
            (default: "Do you have any other feedback about this survey?")

    Raises:
        ConfigurationError: If required environment variables are missing.
        GCSOperationError: If GCS operations fail.
        ThemeFinderError: If ThemeFinder processing fails.
    """
    # Map Cloud Run Job environment variables to run_demo() expected format
    input_bucket = os.environ.get("STAGING_BUCKET") or os.environ.get("INPUT_BUCKET")
    output_bucket = os.environ.get("OUTPUT_BUCKET")
    input_file = os.environ.get("STAGING_FILE") or os.environ.get(
        "INPUT_FILE", "input/example_feedback_v2.csv"
    )
    eval_question = os.environ.get("QUESTION", "Do you have any other feedback about this survey?")

    if not input_bucket or not output_bucket:
        msg = (
            "Environment variables STAGING_BUCKET (or INPUT_BUCKET) "
            "and OUTPUT_BUCKET must be set."
        )
        logger.error(msg)
        raise ConfigurationError(msg)

    # Set environment variables in the format expected by run_demo()
    os.environ["INPUT_BUCKET"] = input_bucket
    os.environ["OUTPUT_BUCKET"] = output_bucket
    os.environ["INPUT_FILE"] = input_file
    os.environ["QUESTION"] = eval_question

    # Delegate to the main ThemeFinder analysis function
    await run_demo()


def main() -> None:
    """Entry point to run the ThemeFinder job from the command line."""
    asyncio.run(run_analysis())


if __name__ == "__main__":
    main()
