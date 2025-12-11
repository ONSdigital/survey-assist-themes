"""Themes Cloud Run Job - Runs ThemeFinder analysis on survey responses."""

from __future__ import annotations

import asyncio
import os
from typing import Any, cast

from langchain_google_vertexai import ChatVertexAI
from survey_assist_utils.logging import get_logger
from themefinder import find_themes

from survey_assist_themes.exceptions import (
    ConfigurationError,
    GCSOperationError,
    ThemeFinderError,
)
from survey_assist_themes.utils.file_utils import (
    load_feedback_csv_from_gcs,
    make_timestamped_blob_name,
    save_themefinder_output_to_gcs,
)
from survey_assist_themes.utils.retry import async_retry_with_backoff

logger = get_logger(__name__)


@async_retry_with_backoff(
    max_attempts=3,
    initial_delay=2.0,
    backoff_factor=2.0,
    exceptions=(Exception,),
)
async def _run_themefinder_with_retry(
    responses_df: Any,
    llm: ChatVertexAI,
    question: str,
    system_prompt: str,
) -> dict[str, Any]:
    """Run ThemeFinder with retry logic for transient failures.

    Args:
        responses_df: DataFrame containing survey responses.
        llm: The language model to use for analysis.
        question: The survey question being evaluated.
        system_prompt: System prompt for the LLM.

    Returns:
        ThemeFinder result dictionary.

    Raises:
        Exception: If ThemeFinder processing fails after all retries.
    """
    result = await find_themes(
        responses_df,
        llm,
        question,
        system_prompt=system_prompt,
    )
    return cast(dict[str, Any], result)


async def run_analysis() -> None:
    """Run ThemeFinder analysis pipeline.

    This function:
    * loads survey responses from GCS (using STAGING_BUCKET/STAGING_FILE)
    * initialises a Gemini chat model via Vertex AI and LangChain
    * calls ThemeFinder's find_themes pipeline
    * saves the structured result to GCS (OUTPUT_BUCKET)

    Raises:
        ConfigurationError: If required environment variables are missing.
        GCSOperationError: If GCS operations fail.
        ThemeFinderError: If ThemeFinder processing fails.
    """
    logger.info("Starting ThemeFinder pipeline")

    # Map Cloud Run Job environment variables to ThemeFinder inputs
    input_bucket = os.environ.get("STAGING_BUCKET") or os.environ.get("INPUT_BUCKET")
    output_bucket = os.environ.get("OUTPUT_BUCKET")
    input_file = os.environ.get("STAGING_FILE") or os.environ.get("INPUT_FILE", "input/example_feedback_v2.csv")
    eval_question = os.environ.get("QUESTION", "Do you have any other feedback about this survey?")

    if not input_bucket or not output_bucket:
        msg = "Environment variables STAGING_BUCKET (or INPUT_BUCKET) and OUTPUT_BUCKET must be set."
        logger.error(msg)
        raise ConfigurationError(msg)

    logger.info(f"Loading feedback from GCS bucket: {input_bucket}, file: {input_file}")

    try:
        # Choose your Gemini model.
        llm = ChatVertexAI(
            model="gemini-2.5-flash",
            temperature=0.0,
        )
        logger.debug("Initialised ChatVertexAI with model: gemini-2.5-flash")

        responses_df = load_feedback_csv_from_gcs(
            bucket_name=input_bucket,
            file_name=input_file,
        )
        logger.info(f"Loaded {len(responses_df)} survey responses")

    except Exception as e:
        logger.error(f"Failed to load feedback from GCS: {e}", exc_info=True)
        raise GCSOperationError(f"Failed to load feedback from GCS: {e}") from e

    question = eval_question
    system_prompt = (
        "You are an AI assistant working for a UK government policy team. "
        "You carefully analyse free-text survey responses to identify key "
        "themes, sentiments and concerns raised by respondents."
    )

    logger.info("Running ThemeFinder analysis")
    try:
        # ThemeFinder runs an asynchronous pipeline over the DataFrame using the
        # provided LLM. The result is a nested, structured dictionary containing
        # intermediate stages (sentiment, themes, mappings, etc.).
        # Retry logic handles transient Vertex AI API failures.
        result: dict[str, Any] = await _run_themefinder_with_retry(
            responses_df,
            llm,
            question,
            system_prompt,
        )
        logger.info("ThemeFinder analysis completed successfully")
        logger.debug(f"Result contains {len(result)} top-level keys")

    except Exception as e:
        logger.error(f"ThemeFinder processing failed: {e}", exc_info=True)
        raise ThemeFinderError(f"ThemeFinder processing failed: {e}") from e

    try:
        output_path = f"output/{make_timestamped_blob_name()}"
        logger.info(f"Saving results to GCS bucket: {output_bucket}, path: {output_path}")
        save_themefinder_output_to_gcs(
            output=result,
            bucket_name=output_bucket,
            destination_blob_name=output_path,
        )
        logger.info("Results saved successfully to GCS")

    except Exception as e:
        logger.error(f"Failed to save results to GCS: {e}", exc_info=True)
        raise GCSOperationError(f"Failed to save results to GCS: {e}") from e


def main() -> None:
    """Entry point to run the ThemeFinder job from the command line."""
    asyncio.run(run_analysis())


if __name__ == "__main__":
    main()
