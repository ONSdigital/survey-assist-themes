from __future__ import annotations

"""
Minimal ThemeFinder proof-of-concept using Gemini on Vertex AI.

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
import asyncio
import os
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from langchain_google_vertexai import ChatVertexAI
from src.survey_assist_themes.utils.file_utils import (
    load_feedback_csv_from_gcs,
    make_timestamped_blob_name,
    save_themefinder_output_as_json,
    save_themefinder_output_to_gcs,
)
from themefinder import find_themes


async def run_demo() -> None:
    """Run a minimal ThemeFinder pipeline using Gemini on Vertex AI.

    This function:

    * loads environment variables (for project and location settings)
    * initialises a Gemini chat model via Vertex AI and LangChain
    * constructs a simple pandas DataFrame of survey responses
    * calls ThemeFinder's ``find_themes`` pipeline
    * prints the raw structured result to standard output

    It is intended as a proof of concept, not a production analysis.
    """
    # Load env vars from .env, if present.
    # Authentication is handled via Application Default Credentials (ADC),
    # which you configure with `gcloud auth application-default login`.
    load_dotenv()

    input_bucket = os.getenv("INPUT_BUCKET")
    output_bucket = os.getenv("OUTPUT_BUCKET")
    input_file = os.getenv("INPUT_FILE", "input/example_feedback_v2.csv")
    eval_question = os.getenv("QUESTION", "Do you have any other feedback about this survey?")

    if not input_bucket or not output_bucket:
        msg = (
            "Environment variables INPUT_BUCKET and OUTPUT_BUCKET "
            "must be set in your .env file."
        )
        raise RuntimeError(msg)

    # Choose your Gemini model. For quick, cheaper runs you might start with
    # gemini-2.5-flash; for heavier reasoning you can switch to gemini-2.5-pro.
    llm = ChatVertexAI(
        model="gemini-2.5-flash",
        temperature=0.0,
    )

    responses_df = load_feedback_csv_from_gcs(bucket_name=input_bucket,
        file_name=input_file,
    )

    question = eval_question
    system_prompt = (
        "You are an AI assistant working for a UK government policy team. "
        "You carefully analyse free-text survey responses to identify key "
        "themes, sentiments and concerns raised by respondents."
    )

    # ThemeFinder runs an asynchronous pipeline over the DataFrame using the
    # provided LLM. The result is a nested, structured dictionary containing
    # intermediate stages (sentiment, themes, mappings, etc.).
    result: dict[str, Any] = await find_themes(
        responses_df,
        llm,
        question,
        system_prompt=system_prompt,
    )

    # Print the raw output
    print(result)

    # Convert to JSON and store locally
    # save_themefinder_output_as_json(
    #     output=result,
    #     filepath=Path("data/themefinder_output.json"),
    # )

    save_themefinder_output_to_gcs(
        output=result,
        bucket_name=output_bucket,
        destination_blob_name=f"output/{make_timestamped_blob_name()}",
    )


def main() -> None:
    """Entry point to run the ThemeFinder demo from the command line.

    This thin wrapper allows the demo to be executed via::

        python -m survey_assist_themes.demo_themefinder_vertex

    while remaining friendly to static type-checking and linting tools.
    """
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
