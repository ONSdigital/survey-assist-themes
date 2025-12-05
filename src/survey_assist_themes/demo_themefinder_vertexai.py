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
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from langchain_google_vertexai import ChatVertexAI
from src.survey_assist_themes.utils.file_utils import save_themefinder_output_as_json
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
    # Load GOOGLE_CLOUD_PROJECT / GOOGLE_CLOUD_LOCATION from .env, if present.
    # Authentication is handled via Application Default Credentials (ADC),
    # which you configure with `gcloud auth application-default login`.
    load_dotenv()

    # Choose your Gemini model. For quick, cheaper runs you might start with
    # gemini-2.5-flash; for heavier reasoning you can switch to gemini-2.5-pro.
    llm = ChatVertexAI(
        model="gemini-2.5-flash",
        temperature=0.0,
    )

    # Example survey responses for the PoC.
    responses_df = pd.DataFrame(
        {
            "response_id": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            "response": [
                "No.",
                "It was easy and quick",
                "Seemed very short.",
                "Very in depth and great interface to use. Simple and not complicated.",
                "Nope - straightforward",
                "It would have been useful if there was an expandable box when I clicked 'other'",
                "Poor questions that were unclear and did not suit the job role in question ",
                "It was clear and easy to understand",
                "I described my job, industry, and the products my company supplies directly. The questions were relevant and easy to answer. The organisation type question worked well, and selecting 'Some other kind of organisation' accurately represented my private company.",
                "Provide a reason for this line of questions, this would promote fuller answers."
            ],
        }
    )

    question = "Do you have any other feedback about this survey?"
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

    # For this PoC, simply print the full result. In a real project you may
    # wish to persist these data or turn them into visualisations.
    print(result)

    save_themefinder_output_as_json(
        output=result,
        filepath=Path("data/themefinder_output.json"),
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
