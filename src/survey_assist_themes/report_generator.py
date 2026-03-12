"""Spike to generate a report using the ThemeFinder output with Gemini and Vertex AI."""
from __future__ import annotations

import asyncio
import base64
import json
import os
from typing import Any

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_vertexai import ChatVertexAI
from survey_assist_utils.logging import get_logger

from survey_assist_themes.exceptions import ConfigurationError, ThemeFinderError
from survey_assist_themes.utils.file_utils import (
    make_timestamped_blob_name,
    save_themefinder_output_to_gcs,
)

from google.cloud import storage  # type: ignore[import]
from survey_assist_themes.exceptions import GCSOperationError

logger = get_logger(__name__)
 
SYSTEM_PROMPT = (
    "I have been testing an application with the public which provides AI generated questions, "
    "only when they are appropriate, into a survey that aims to categorise the type of "
    "organisation the respondent works for. The application consists of a survey section "
    "(asks job title, description and organisation description + dynamic questions when required) "
    "and a feedback section which allowed the respondents to provide \"other feedback\" by "
    "answering the question: \"Do you have any other feedback about this survey?\""
    "I have run all of the responses from the public through a library called ThemeFinder - "
    "https://github.com/i-dot-ai/themefinder/ which maps responses to themes and categorises "
    "sentiment as well as indicating how detailed the feedback was and therefore how suitable "
    "for analysis."
    "Use the ThemeFinder readme and the uploaded json output file to provide a high level summary "
    "of the themes identified, user sentiment to the application and the level of detail provided "
    "in the feedback."
    "This output must be presented in a way that refers back to the input data to give examples "
    "and is accessible to non-data scientists."
)
 
 
def _load_themefinder_output_from_gcs(bucket_name: str, blob_name: str) -> dict[str, Any]:

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        raw = blob.download_as_text()
        logger.info(f"Loaded ThemeFinder output from gs://{bucket_name}/{blob_name}")
    except Exception as e:
        raise GCSOperationError(
            f"Failed to load ThemeFinder output from gs://{bucket_name}/{blob_name}: {e}"
        ) from e

    try:
        return json.loads(raw)  # type: ignore[no-any-return]
    except json.JSONDecodeError as e:
        raise ThemeFinderError(
            f"Could not parse ThemeFinder output as JSON: {e}"
        ) from e


async def generate_report(
    themefinder_output_path: str,
    question: str,
    output_bucket: str,
    preprocess: bool = False,
    use_document_upload: bool = False,
) -> None:
    if preprocess and use_document_upload:
        raise ConfigurationError("Cannot use both preprocess and document upload strategies at the same time.")
    # Parse the GCS path
    path = themefinder_output_path.removeprefix("gs://")
    if "/" not in path:
        raise ConfigurationError(
            f"THEMEFINDER_OUTPUT_PATH must be in the form "
            f"'gs://bucket/blob' or 'bucket/blob', got: {themefinder_output_path!r}"
        )
    input_bucket, blob_name = path.split("/", 1)

    result = _load_themefinder_output_from_gcs(input_bucket, blob_name)
 
    llm = ChatVertexAI(model="gemini-2.5-flash", temperature=0.2)
 
    if use_document_upload:
        # uses Gemini's document handling rather than inline JSON in the prompt
        json_bytes = json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8")
        b64_json = base64.b64encode(json_bytes).decode("utf-8")
 
        response = await llm.ainvoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=[
                {
                    "type": "text",
                    "text": (
                        f"Survey question: **{question}**\n\n"
                        "The ThemeFinder output JSON is attached as a document below. "
                        "Write a comprehensive report based on this analysis."
                    ),
                },
                {
                    "type": "media",
                    "mime_type": "text/plain",
                    "data": b64_json,
                },
            ]),
        ])
        prefix = "report_document_upload"
 
    elif preprocess:
        themes = result.get("themes", [])
        mapping = result.get("mapping", [])

        if not themes:
            raise ThemeFinderError(
                f"No themes found in result. Top-level keys: {list(result.keys())}"
            )

        logger.debug(f"First theme type: {type(themes[0])}, value: {themes[0]}")

        # Build themes block
        themes_block = "\n".join(
            f"- [{t.get('topic_id', i)}] {t.get('topic', t)} (condensed from {t.get('source_topic_count', 0)} initial themes)"
            if isinstance(t, dict) else f"- {t}"
            for i, t in enumerate(themes)
        )

        # Group a few example responses by theme
        responses_by_theme: dict[str, list[str]] = {}
        for m in mapping:
            for label in m.get("labels", []):
                responses_by_theme.setdefault(str(label), []).append(m.get("response", ""))

        samples_block = ""
        for t in themes:
            tid = str(t.get("topic_id", "")) if isinstance(t, dict) else ""
            samples = responses_by_theme.get(tid, [])[:5]
            if samples:
                samples_block += (
                    f"\n[{tid}] Examples:\n" + "\n".join(f'  - "{r}"' for r in samples) + "\n"
                )
 
        response = await llm.ainvoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=(
                f"Survey question: **{question}**\n\n"
                f"## Themes\n{themes_block}\n\n"
                f"## Example responses\n{samples_block}\n\n"
                "Write a comprehensive report based on this analysis."
            )),
        ])
        prefix = "report_preprocessed"
 
    else:
        # Pass the full ThemeFinder output inline to Gemini and let it figure
        # out how to use it.
        response = await llm.ainvoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=(
                f"Survey question: **{question}**\n\n"
                f"ThemeFinder output (JSON): {json.dumps(result)}\n\n"
                "Write a comprehensive report based on this analysis."
            )),
        ])
        prefix = "report_fulljson"
 
    report_text: str = response.content  # type: ignore[assignment]
    logger.info(f"Report generated ({len(report_text)} characters)")
 
    blob_name = f"reports/{make_timestamped_blob_name(prefix=prefix)}"
    save_themefinder_output_to_gcs(
        output={"report": report_text},
        bucket_name=output_bucket,
        destination_blob_name=blob_name,
    )
    logger.info(f"Report saved to gs://{output_bucket}/{blob_name}")


async def run(preprocess: bool = False, use_document_upload: bool = False) -> None:
    logger.info("Starting report generator pipeline")
    load_dotenv()

    question = os.getenv("QUESTION", "Do you have any other feedback about this survey?")
    output_bucket = os.getenv("OUTPUT_BUCKET")
    themefinder_output_path = os.getenv("THEMEFINDER_OUTPUT_PATH")

    if not output_bucket or not themefinder_output_path:
        msg = (
            "Environment variables OUTPUT_BUCKET and THEMEFINDER_OUTPUT_PATH "
            "must be set in your .env file."
        )
        logger.error(msg)
        raise ConfigurationError(msg)

    logger.info(f"Using ThemeFinder output: {themefinder_output_path}")
    logger.info(f"Output bucket: {output_bucket}")

    await generate_report(
        themefinder_output_path=themefinder_output_path,
        question=question,
        output_bucket=output_bucket,
        preprocess=preprocess,
        use_document_upload=use_document_upload,
    )


def main() -> None:
    # Generates two reports, with/without themefinder output preprocessing. 
    asyncio.run(run(preprocess=True)) # run preprocessing on themefinder output to avoid passing full json inline to LLM
    asyncio.run(run(preprocess=False)) # generate report by passing full json file inline 
    asyncio.run(run(use_document_upload=True)) # generate report by passing full json file as a base64-encoded document upload to Gemini

 
if __name__ == "__main__":
    main()