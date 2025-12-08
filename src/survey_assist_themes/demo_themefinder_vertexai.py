"""Minimal ThemeFinder proof-of-concept using Gemini on Vertex AI.

This module wires ThemeFinder to Google's Gemini models through Vertex AI
via LangChain's ChatVertexAI integration.

It runs the ThemeFinder pipeline on a small, hard-coded set of survey
responses so you can confirm that:
  * Vertex AI authentication is working
  * Gemini 2.5 (Flash or Pro) is reachable
  * ThemeFinder integrates cleanly with your LLM selection

The code is written to be mypy- and ruff-friendly and uses British
spelling in documentation.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, cast

import pandas as pd
from dotenv import load_dotenv
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
    responses_df: pd.DataFrame,
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


async def run_demo() -> None:
    """Run a minimal ThemeFinder pipeline using Gemini on Vertex AI.

    This function:

    * loads environment variables (for project and location settings)
    * initialises a Gemini chat model via Vertex AI and LangChain
    * loads survey responses from GCS
    * calls ThemeFinder's ``find_themes`` pipeline
    * saves the structured result to GCS

    It is intended as a proof of concept, not a production analysis.

    Raises:
        ConfigurationError: If required environment variables are missing.
        GCSOperationError: If GCS operations fail.
        ThemeFinderError: If ThemeFinder processing fails.
    """
    logger.info("Starting ThemeFinder pipeline")
    load_dotenv()

    input_bucket = os.getenv("INPUT_BUCKET")
    output_bucket = os.getenv("OUTPUT_BUCKET")
    input_file = os.getenv("INPUT_FILE", "input/example_feedback_v2.csv")
    eval_question = os.getenv("QUESTION", "Do you have any other feedback about this survey?")

    if not input_bucket or not output_bucket:
        msg = (
            "Environment variables INPUT_BUCKET and OUTPUT_BUCKET " "must be set in your .env file."
        )
        logger.error(msg)
        raise ConfigurationError(msg)

    logger.info(f"Loading feedback from GCS bucket: {input_bucket}, file: {input_file}")

    try:
        # Choose your Gemini model. For quick, cheaper runs you might start with
        # gemini-2.5-flash; for heavier reasoning you can switch to gemini-2.5-pro.
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
    """Entry point to run the ThemeFinder demo from the command line.

    This thin wrapper allows the demo to be executed via::

        python -m survey_assist_themes.demo_themefinder_vertex

    while remaining friendly to static type-checking and linting tools.
    """
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
