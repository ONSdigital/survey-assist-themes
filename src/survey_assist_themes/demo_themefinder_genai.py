from __future__ import annotations

"""
Minimal ThemeFinder proof-of-concept using Gemini via the GenAI API.

This module wires ThemeFinder to Google's Gemini models through the
`langchain-google-genai` integration. It avoids Vertex AI's heavier
dependency stack (including newer versions of pyarrow), so it plays
nicely with ThemeFinder's current requirements.

It:

* loads environment variables (GEMINI_API_KEY)
* initialises a Gemini chat model using LangChain
* constructs a simple pandas DataFrame of survey responses
* calls ThemeFinder's `find_themes` pipeline
* prints the structured result

The code is written to be mypy- and ruff-friendly and uses British
spelling in documentation.
"""

import asyncio
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from themefinder import find_themes


async def run_demo() -> None:
    """Run a minimal ThemeFinder pipeline using Gemini.

    This function:

    * loads environment variables from a .env file, if present
    * initialises a Gemini chat model via LangChain
    * builds a small in-memory DataFrame of survey responses
    * invokes ThemeFinder's ``find_themes`` pipeline
    * prints the raw structured result to standard output

    It is intended as a proof of concept rather than a full analysis
    workflow.
    """
    # Load GEMINI_API_KEY and any other configuration from .env.
    load_dotenv()

    # You can switch between "gemini-2.5-flash" and "gemini-2.5-pro".
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0.0,
    )

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

    result: dict[str, Any] = await find_themes(
        responses_df,
        llm,
        question,
        system_prompt=system_prompt,
    )

    print(result)


def main() -> None:
    """Entry point to run the ThemeFinder demo from the command line.

    This wrapper allows the demo to be executed via::

        python -m survey_assist_themes.demo_themefinder_gemini
    """
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
