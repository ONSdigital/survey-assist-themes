"""
This module generates markdown reports based on the output from the ThemeFinder pipeline. 
It reads the ThemeFinder output JSON from a specified GCS location, processes it,
and uses a generative model to create reports summarising the themes identified in survey feedback.
The report generation is configurable via a JSON config file, allowing for different prompts, model settings, and report titles.
Additional statistics about the themes and sentiments can also be included in the report based on the configuration.
The generated reports are then saved back to GCS.
"""
from __future__ import annotations

import asyncio
import base64
from datetime import UTC, datetime
import json
import os
from typing import Any

from dotenv import load_dotenv
from google.cloud import storage  # type: ignore[import]
import vertexai
from vertexai.generative_models import GenerativeModel, Part, Content

from survey_assist_utils.logging import get_logger

from survey_assist_themes.exceptions import ConfigurationError, GCSOperationError, ThemeFinderError
from survey_assist_themes.utils.file_utils import (
    save_markdown_report_to_gcs,
    load_themefinder_output_from_gcs,
)

logger = get_logger(__name__)

# Load report config, example config provided at src/survey_assist_themes/report_config.json.txt
def get_report_config() -> dict[str, Any]:
    try:
        with open("src/survey_assist_themes/report_config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
            logger.info("Report configuration loaded successfully.")
            if "reports_config" not in config:
                raise KeyError("Missing required key 'reports_config' in configuration")
            
            if not isinstance(config["reports_config"], list):
                raise ValueError("'reports_config' must be a list")
            
            if len(config["reports_config"]) == 0:
                raise ValueError("'reports_config' cannot be empty")
            return config
    except Exception as e:
        raise ConfigurationError(f"Failed to load report configuration: {e}") from e

def generate_report_stats(result: dict[str, Any]) -> str:
    themes = result.get("themes", [])
    mapping = result.get("mapping", [])
    sentiment_data = result.get("sentiment", [])
    detail_data = result.get("detailed_responses", [])
    unprocessables = result.get("unprocessables", [])
    
    total_responses = len(mapping)
    total_unprocessables = len(unprocessables)

    theme_counts = {}
    for m in mapping:
        for label in m.get("labels", []):
            theme_counts[label] = theme_counts.get(label, 0) + 1

    themes_block = ""
    for t in themes:
        tid = t.get("topic_id")
        count = theme_counts.get(tid, 0)
        percentage = (count / total_responses * 100) if total_responses > 0 else 0
        themes_block += f"- [{tid}] {t.get('topic')} | Count: {count} ({percentage:.1f}%)\n"

    pos_count = sum(1 for s in sentiment_data if s.get("position") == "AGREEMENT")
    neg_count = sum(1 for s in sentiment_data if s.get("position") == "DISAGREEMENT")
    unclear_count = sum(1 for s in sentiment_data if s.get("position") == "UNCLEAR")

    rich_count = sum(1 for d in detail_data if d.get("evidence_rich") == "YES")
    non_rich_count = sum(1 for d in detail_data if d.get("evidence_rich") == "NO")

    no_theme_count = sum(1 for m in mapping if not m.get("labels"))
    no_theme_count_pct = (no_theme_count / total_responses * 100) if total_responses > 0 else 0
    multi_theme_count = sum(1 for m in mapping if len(m.get("labels", [])) > 1)

    stats_text = (
        "Response statistics:\n\n"
        f"**Thematic Summary:**\n"
        f"Total responses processed: {total_responses}\n"
        f"Total unprocessables: {total_unprocessables}\n"
        f"Themes identified and their frequency:\n{themes_block}\n"
        f"Responses not mapped to any theme: {no_theme_count} ({no_theme_count_pct:.1f}%)\n"
        f"Responses mapped to multiple themes: {multi_theme_count}\n\n"
        f"**Sentiment & Detail:**\n"
        f"Sentiment breakdown: {pos_count} Agreement, {neg_count} Disagreement, {unclear_count} Unclear.\n"
        f"Feedback depth: {rich_count} evidence-rich responses vs {non_rich_count} surface-level responses.\n\n"
        "Please provide a high-level summary that is accessible to non-data scientists, "
        "referring to specific examples from the JSON data to support the themes."
    )
    return stats_text

async def _generate_single_report(
        config: dict[str, Any],
) -> str:
        """Helper function to generate a single report asynchronously."""
        contents = [Content(role="user", parts=[config["prompt_part"], config["json_part"]])]
        title = config.get("title", "report")
        prefix = f"{config["blob_name"].rsplit('.', 1)[0]}_{title.replace(' ', '_')}"
        logger.info(f"Generating report '{title}'")
        try:
            response = await asyncio.to_thread(
                config["model"].generate_content, 
                contents, 
                generation_config=config["model_config"]
            )
        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}")
            raise ThemeFinderError(f"Model failed to generate report: {e}")

        if not response.text:
            msg = "LLM response missing or empty"
            logger.error(msg)
            raise ValueError(msg)
        
        try:
            report_text: str = response.text
        except Exception as e:
            logger.error(f"Failed to extract report text: {str(e)}")
            raise ThemeFinderError(f"Failed to extract report text: {e}")
        logger.info(f"Report generated ({len(report_text)} characters)")

        try:
            save_markdown_report_to_gcs(
                report=report_text,
                bucket_name=config["output_bucket"],
                destination_blob_name=prefix,
            )
            logger.info(f"Report saved to gs://{config["output_bucket"]}/{prefix}.md")
        except Exception as e:
            logger.error(f"Failed to save report to GCS: {str(e)}")
            raise GCSOperationError(f"Failed to save report to GCS: {e}")

async def generate_report(
    themefinder_output_path: str,
    question: str,
    output_bucket: str,
    project: str,
    location: str,
) -> None:
    # Parse the GCS path
    if not themefinder_output_path.startswith("gs://"):
        raise ConfigurationError(
            f"THEMEFINDER_OUTPUT_PATH must start with 'gs://', got: {themefinder_output_path!r}"
        )
    
    path = themefinder_output_path.removeprefix("gs://")
    if "/" not in path:
        raise ConfigurationError(
            f"THEMEFINDER_OUTPUT_PATH must be in the form 'gs://bucket/blob', got: {themefinder_output_path!r}"
        )
    input_bucket, blob_name = path.split("/", 1)

    # load themefinder output json
    result = load_themefinder_output_from_gcs(input_bucket, blob_name)
    json_bytes = json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8")
    json_part = Part.from_data(data=json_bytes, mime_type="text/plain")

    config = get_report_config()

    report_tasks = []
    # Iterate over report configs and produce task list
    for report_cfg in config.get("reports_config", []):

        model_cfg = report_cfg["model"]
        prompt_file_text = report_cfg["prompt_text"]
        system_instruction = report_cfg["system_instructions"]

        vertexai.init(project=project, location=location)
        model = GenerativeModel(
            model_name=model_cfg["model_name"],
            system_instruction=system_instruction
        )
        
        stats_text = None
        if report_cfg.get("add_stats", False):
            stats_text = generate_report_stats(result)

        prompt_part = Part.from_text(
            f"{prompt_file_text}\n\n"
            f"Survey question: **{question}**\n\n"
            "The ThemeFinder output JSON is attached as a document below. "
            f"{'Here are some statistics about the responses:\n\n' + stats_text if stats_text else ''}"
        )
        logger.debug(f"{prompt_part}")

        generation_config = {
            "model_config": {
                "temperature": model_cfg.get("temperature", 0.2)
            },
            "model": model,
            "title": report_cfg.get("title", "report"),
            "blob_name": blob_name, # name of the themefinder output file
            "prompt_part": prompt_part,
            "json_part": json_part,
            "output_bucket": output_bucket,
        }

        report_tasks.append(
            _generate_single_report(
                config=generation_config,
            )
        )

    await asyncio.gather(*report_tasks)
    logger.info("All report generation tasks completed")


async def run() -> None:
    logger.info("Starting report generator pipeline")
    load_dotenv()

    question = os.getenv("QUESTION", "Do you have any other feedback about this survey?")
    output_bucket = os.getenv("OUTPUT_BUCKET")
    themefinder_output_path = os.getenv("THEMEFINDER_OUTPUT_PATH")
    project = os.getenv("GCP_PROJECT")
    location = os.getenv("GCP_LOCATION", "europe-west2")

    if not output_bucket or not themefinder_output_path or not project:
        msg = (
            "Environment variables OUTPUT_BUCKET, THEMEFINDER_OUTPUT_PATH, and GCP_PROJECT "
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
        project=project,
        location=location,
    )
 
if __name__ == "__main__":
    asyncio.run(run())