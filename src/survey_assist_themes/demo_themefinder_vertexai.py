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
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from langchain_google_vertexai import ChatVertexAI
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
            "response_id": ["1", "2", "3", "4", "5"],
            "response": [
                "The new digital service is straightforward and saves me time.",
                "It works, but I often cannot find the guidance I need.",
                "I like the idea, although performance is slow at busy times.",
                "I worry it is hard to use for people with low digital skills.",
                "It feels unreliable, so I still rely on the older process.",
            ],
        }
    )

    question = "What do you think about the new digital service?"
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


def main() -> None:
    """Entry point to run the ThemeFinder demo from the command line.

    This thin wrapper allows the demo to be executed via::

        python -m survey_assist_themes.demo_themefinder_vertex

    while remaining friendly to static type-checking and linting tools.
    """
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
