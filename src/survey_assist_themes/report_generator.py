"""
This module generates markdown reports based on the output from the ThemeFinder pipeline.
It reads the ThemeFinder output JSON from a specified GCS location, processes it,
and uses a generative model to create reports summarising the themes identified in survey feedback.
The report generation is configurable via a JSON config file, allowing for different prompts,
model settings, and report titles.
Additional statistics about the themes and sentiments can also be included in the report
based on the configuration. The generated reports are then saved back to GCS.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import vertexai
from dotenv import load_dotenv
from survey_assist_utils.logging import get_logger
from vertexai.generative_models import Content, GenerativeModel, Part

from survey_assist_themes.exceptions import (
    ConfigurationError,
    GCSOperationError,
    ThemeFinderError,
)
from survey_assist_themes.utils.file_utils import (
    load_themefinder_output_from_gcs,
    save_markdown_report_to_gcs,
)

logger = get_logger(__name__)


# Load report config, example config provided at src/survey_assist_themes/report_config.json.txt
def get_report_config() -> dict[str, Any]:
    """ #TODO; rework this to load config from GCS. & update docstring
    Load and validate the report configuration from the project's JSON file.

    This function opens and parses the JSON file at
    "src/survey_assist_themes/report_config.json". It validates that the top-level
    mapping contains a key "reports_config" whose value is a non-empty list, where each item 
    in the list represents a separate report configuration. Each configuration
    specifies the report title, prompt text, system instructions, model settings, 
    and whether to include statistics in the prompt.

    Args:
        None

    Returns:
        dict[str, Any]: Parsed configuration mapping from the JSON file.

    Raises:
        ConfigurationError: If the file cannot be opened, the JSON is invalid, the required
            "reports_config" key is missing, or if "reports_config" is not a non-empty list.
            The underlying exception is chained to this error for debugging.
    """
    try:
        with open("src/survey_assist_themes/report_config.json", encoding="utf-8") as f:
            config: dict[str, Any] = json.load(f)
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
    """Generate a statistics summary from the ThemeFinder output.

    This function processes the structured output from ThemeFinder to extract key statistics
    about the themes identified in the survey responses, the sentiment distribution, and
    the depth of responses. It calculates counts and percentages for each theme, the number
    of responses mapped to no themes or multiple themes, and the sentiment breakdown. It then
    formats this information into a human-readable summary that can be included in the report.

    Args:
        result (dict[str, Any]): The structured output from ThemeFinder, containing
            data such as "themes", "mapping", "sentiment", "detailed_responses", and "unprocessables".

    Returns:
        str: A formatted string summarising the statistics of the ThemeFinder output, ready to be included in the report.
    """
    themes = result.get("themes", [])
    mapping = result.get("mapping", [])
    sentiment_data = result.get("sentiment", [])
    detail_data = result.get("detailed_responses", [])
    unprocessables = result.get("unprocessables", [])

    total_responses = len(mapping)
    total_unprocessables = len(unprocessables)

    theme_counts: dict[str, int] = {}
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
        f"Sentiment: {pos_count} Agreement, {neg_count} Disagreement, {unclear_count} Unclear.\n"
        f"Depth: {rich_count} evidence-rich, {non_rich_count} surface-level responses.\n\n"
        "Please provide a high-level summary that is accessible to non-data scientists, "
        "referring to specific examples from the JSON data to support the themes."
    )
    return stats_text


async def _generate_single_report(
    config: dict[str, Any],
) -> None:
    """Helper function to generate a single report asynchronously.

    This function takes a configuration dictionary that includes the generative model, prompt parts,
    the name of the ThemeFinder output blob, and the output bucket. It constructs the content for the model,
    invokes the model to generate the report text, and then saves the generated report to GCS. 
    It includes error handling to catch and log issues during model generation and GCS operations.

    Args:
        config (dict[str, Any]): A dictionary containing the following keys:
            - "model": An instance of a GenerativeModel to use for report generation.
            - "prompt_part": A Part object containing the prompt text for the model.
            - "json_part": A Part object containing the ThemeFinder output JSON as text.
            - "blob_name": The name of the ThemeFinder output blob, used for naming the report.
            - "output_bucket": The name of the GCS bucket where the report should be saved.
            - "title": The title of the report, used in naming the output file.

    Returns:
        None

    Raises:
        ThemeFinderError: If the model fails to generate the report.
        GCSOperationError: If there is an error saving the report to GCS.
    """
    contents = [Content(role="user", parts=[config["prompt_part"], config["json_part"]])]
    title = config.get("title", "report")
    prefix = f"{config["blob_name"].rsplit('.', 1)[0]}_{title.replace(' ', '_')}"
    logger.info(f"Generating report '{title}'")
    try:
        response = await asyncio.to_thread(
            config["model"].generate_content, contents, generation_config=config["model_config"]
        )
    except Exception as e:
        logger.error(f"Report generation failed: {str(e)}")
        raise ThemeFinderError(f"Model failed to generate report: {e}") from e

    if not response.text:
        msg = "LLM response missing or empty"
        logger.error(msg)
        raise ValueError(msg)

    try:
        report_text: str = response.text
    except Exception as e:
        logger.error(f"Failed to extract report text: {str(e)}")
        raise ThemeFinderError(f"Failed to extract report text: {e}") from e
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
        raise GCSOperationError(f"Failed to save report to GCS: {e}") from e


async def generate_reports(
    themefinder_output_path: str,
    question: str,
    output_bucket: str,
    project: str,
    location: str,
) -> None:
    """Main function to generate multiple reports based on ThemeFinder output.

    This function handles the overall report generation workflow.
    It reads the ThemeFinder output JSON from the specified GCS path, generates statistics about the themes,
    loads the report configuration, and then iterates over each report configuration to generate reports in parallel
    using asyncio. Each report is generated by invoking the _generate_single_report helper function, which
    interacts with the generative model and saves the output to GCS. The function includes error handling to manage
    issues with configuration, model generation, and GCS operations.


    Args:
        themefinder_output_path (str): The GCS path to the ThemeFinder output JSON file 
            (e.g., "gs://bucket/path/to/output.json").
        question (str): The survey question that was analysed, to be included in the report prompt.
        output_bucket (str): The name of the GCS bucket where the generated reports should be
            saved.
        project (str): The GCP project ID to use for Vertex AI operations.
        location (str): The GCP location to use for Vertex AI operations.
    Returns:
        None
    Raises:
        ConfigurationError: If the themefinder_output_path is not properly formatted 
            or if required environment variables are missing.
        ThemeFinderError: If there is an error during report generation by the model.
        GCSOperationError: If there is an error saving the generated report to GCS.    
    """
    # Parse the GCS path
    if not themefinder_output_path.startswith("gs://"):
        raise ConfigurationError(
            f"THEMEFINDER_OUTPUT_PATH must start with 'gs://', got: {themefinder_output_path!r}"
        )

    path = themefinder_output_path.removeprefix("gs://")
    if "/" not in path:
        raise ConfigurationError(
            f"THEMEFINDER_OUTPUT_PATH must be in the form 'gs://bucket/blob', \
              got: {themefinder_output_path!r}"
        )
    input_bucket, blob_name = path.split("/", 1)

    # load themefinder output json
    result = load_themefinder_output_from_gcs(input_bucket, blob_name)
    json_bytes = json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8")
    json_part = Part.from_data(data=json_bytes, mime_type="text/plain")
    stats_text = generate_report_stats(result)

    config = get_report_config()

    report_tasks = []
    # Iterate over report configs and produce task list
    for report_cfg in config.get("reports_config", []):
        model_cfg = report_cfg["model"]
        prompt_file_text = report_cfg["prompt_text"]
        system_instruction = report_cfg["system_instructions"]

        vertexai.init(project=project, location=location)
        model = GenerativeModel(
            model_name=model_cfg["model_name"], system_instruction=system_instruction
        )

        if not stats_text and report_cfg.get("add_stats", False):
            logger.warning(
                "Report config requests stats to be added to the prompt, but no stats were generated."
            )

        prompt_part = Part.from_text(
            f"{prompt_file_text}\n\n"
            f"Survey question: **{question}**\n\n"
            "The ThemeFinder output JSON is attached as a document below. "
            f"{'Here are some statistics about the responses:\n\n' \
             + stats_text if report_cfg.get("add_stats", False) else ''}"
        )
        logger.debug(f"{prompt_part}")

        generation_config = {
            "model_config": {"temperature": model_cfg.get("temperature", 0.2)},
            "model": model,
            "title": report_cfg.get("title", "report"),
            "blob_name": blob_name,  # name of the themefinder output file
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
    """Entry point for running the report generator pipeline.
    
    This function reads necessary configuration from environment variables 
    and then calls the main report generation function. 
    It includes some basic validation of the required environment variables.

    Args:
        None
    Returns:
        None
    Raises:
        ConfigurationError: If required environment variables are missing or invalid."""

    logger.info("Starting report generator pipeline")
    load_dotenv()

    generate_report_flag = os.getenv("GENERATE_REPORTS", "true").lower() == "true"
    if not generate_report_flag:
        logger.info("Report generation is disabled via GENERATE_REPORTS flag. Exiting.")
        return

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

    await generate_reports(
        themefinder_output_path=themefinder_output_path,
        question=question,
        output_bucket=output_bucket,
        project=project,
        location=location,
    )


if __name__ == "__main__":
    asyncio.run(run())
