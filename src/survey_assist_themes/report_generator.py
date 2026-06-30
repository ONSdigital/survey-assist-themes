"""
Generate markdown reports from ThemeFinder output.

This module reads a ThemeFinder output JSON document from Google Cloud Storage,
builds one or more report-generation prompts from a JSON configuration file,
and uses Vertex AI generative models to create markdown reports.

Each report configuration can specify:
- model name
- temperature
- max output tokens
- system instructions
- prompt text
- title
- whether summary statistics should be included in the prompt

Generated reports are written back to Google Cloud Storage.
"""

from __future__ import annotations

import asyncio
import json
import os
from enum import IntEnum
from typing import Any, Final

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
    load_json_from_gcs,
    save_markdown_report_to_gcs,
)

logger = get_logger(__name__)

_REPORTS_CONFIG_KEY: Final[str] = "reports_config"
_DEFAULT_TEMPERATURE: Final[float] = 0.2
_DEFAULT_MAX_OUTPUT_TOKENS: Final[int] = 65535


class FinishReason(IntEnum):
    """Known Vertex AI finish reasons used for defensive validation."""

    FINISH_REASON_UNSPECIFIED = 0
    STOP = 1
    MAX_TOKENS = 2
    SAFETY = 3
    RECITATION = 4
    OTHER = 5
    BLOCKLIST = 6
    PROHIBITED_CONTENT = 7
    SPII = 8
    MALFORMED_FUNCTION_CALL = 9


def get_report_config(config_path: str) -> dict[str, Any]:
    """Load and validate report configuration from GCS.

    Args:
        config_path: GCS path to the report configuration JSON file.

    Returns:
        Parsed configuration mapping.

    Raises:
        ConfigurationError: If the path is invalid, the file cannot be read,
            the JSON is invalid, or the configuration structure is invalid.
    """
    if not config_path.startswith("gs://"):
        raise ConfigurationError(
            f"REPORT_CONFIG_PATH must start with 'gs://', got: {config_path!r}"
        )

    path = config_path.removeprefix("gs://")
    if "/" not in path:
        raise ConfigurationError(
            "REPORT_CONFIG_PATH must be in the form 'gs://bucket/blob', " f"got: {config_path!r}"
        )

    input_bucket, blob_name = path.split("/", 1)

    try:
        config = load_json_from_gcs(input_bucket, blob_name)
    except Exception as exc:
        raise ConfigurationError(f"Failed to load report configuration: {exc}") from exc

    if not isinstance(config, dict):
        raise ConfigurationError("Report configuration must be a JSON object")

    reports_config = config.get(_REPORTS_CONFIG_KEY)
    if reports_config is None:
        raise ConfigurationError(f"Missing required key {_REPORTS_CONFIG_KEY!r} in configuration")

    if not isinstance(reports_config, list):
        raise ConfigurationError(f"{_REPORTS_CONFIG_KEY!r} must be a list")

    if not reports_config:
        raise ConfigurationError(f"{_REPORTS_CONFIG_KEY!r} cannot be empty")

    logger.info("Report configuration loaded successfully.")
    return config


def generate_report_stats(result: dict[str, Any]) -> str:
    """Generate a textual statistics summary from ThemeFinder output.

    Args:
        result: Structured ThemeFinder output.

    Returns:
        Human-readable statistics text suitable for inclusion in a prompt.
    """
    themes = result.get("themes", [])
    responses = result.get("responses", {})

    processable_responses = [
        response for response in responses.values() if response.get("processable", True)
    ]

    unprocessable_responses = [
        response for response in responses.values() if not response.get("processable", True)
    ]

    total_responses = len(responses)
    total_processable = len(processable_responses)
    total_unprocessables = len(unprocessable_responses)

    theme_counts: dict[str, int] = {}

    for response in processable_responses:
        for label in response.get("labels", []):
            theme_counts[label] = theme_counts.get(label, 0) + 1

    themes_block = ""
    for topic_id, theme in themes.items():
        count = theme_counts.get(topic_id, 0)
        percentage = (count / total_responses * 100) if total_responses > 0 else 0
        themes_block += (
            f"- [{topic_id}] {theme.get('topic')} | " f"Count: {count} ({percentage:.1f}%)\n"
        )

    pos_count = sum(
        1 for response in processable_responses if response.get("sentiment") == "AGREEMENT"
    )

    neg_count = sum(
        1 for response in processable_responses if response.get("sentiment") == "DISAGREEMENT"
    )

    unclear_count = sum(
        1 for response in processable_responses if response.get("sentiment") == "UNCLEAR"
    )

    rich_count = sum(
        1 for response in processable_responses if response.get("evidence_rich") is True
    )

    non_rich_count = sum(
        1 for response in processable_responses if response.get("evidence_rich") is False
    )

    no_theme_count = sum(1 for response in processable_responses if not response.get("labels"))

    no_theme_count_pct = no_theme_count / total_responses * 100 if total_responses > 0 else 0

    multi_theme_count = sum(
        1 for response in processable_responses if len(response.get("labels", [])) > 1
    )

    return (
        "Response statistics:\n\n"
        "**Thematic Summary:**\n"
        f"Total responses: {total_responses}\n"
        f"Total responses processed: {total_processable}\n"
        f"Total unprocessables: {total_unprocessables}\n"
        f"Themes identified and their frequency:\n{themes_block}\n"
        f"Responses not mapped to any theme: "
        f"{no_theme_count} ({no_theme_count_pct:.1f}%)\n"
        f"Responses mapped to multiple themes: {multi_theme_count}\n\n"
        "**Sentiment & Detail:**\n"
        f"Sentiment: {pos_count} Agreement, {neg_count} Disagreement, "
        f"{unclear_count} Unclear.\n"
        f"Depth: {rich_count} evidence-rich, {non_rich_count} surface-level responses.\n\n"
        "Please provide a high-level summary that is accessible to non-data "
        "scientists, referring to specific examples from the JSON data to "
        "support the themes."
    )


def _extract_response_debug(response: Any) -> dict[str, Any]:
    """Extract debug metadata from a Vertex AI response.

    Args:
        response: Response returned by ``GenerativeModel.generate_content``.

    Returns:
        Mapping containing finish reason, finish message, usage data, and text
        length for logging.
    """
    candidate = response.candidates[0] if getattr(response, "candidates", None) else None
    usage = getattr(response, "usage_metadata", None)
    finish_reason = getattr(candidate, "finish_reason", None)

    return {
        "finish_reason": int(finish_reason) if finish_reason is not None else None,
        "finish_reason_name": _normalise_finish_reason(finish_reason),
        "finish_message": getattr(candidate, "finish_message", None),
        "usage_metadata": str(usage) if usage is not None else None,
        "text_length": len(getattr(response, "text", "") or ""),
    }


def _normalise_finish_reason(finish_reason: Any) -> str:
    """Convert a raw finish reason into a stable string value.

    Args:
        finish_reason: Raw finish reason from the Vertex response.

    Returns:
        Readable finish reason name.
    """
    if finish_reason is None:
        return "UNKNOWN"

    try:
        return FinishReason(int(finish_reason)).name
    except (TypeError, ValueError):
        return str(finish_reason)


def _validate_report_response(response: Any, report_title: str) -> str:
    """Validate a Vertex AI response and extract report text.

    Args:
        response: Response returned by ``GenerativeModel.generate_content``.
        report_title: Human-readable report title for logging and errors.

    Returns:
        Extracted markdown report text.

    Raises:
        ThemeFinderError: If the model response is empty, missing, or finished
            for any reason other than a normal stop.
    """
    report_text = getattr(response, "text", None)
    if not isinstance(report_text, str) or not report_text.strip():
        raise ThemeFinderError(
            f"Report generation returned an empty response for {report_title!r}."
        )

    candidate = response.candidates[0] if getattr(response, "candidates", None) else None
    finish_reason_raw = getattr(candidate, "finish_reason", None)
    finish_reason_name = _normalise_finish_reason(finish_reason_raw)
    finish_message = getattr(candidate, "finish_message", "")

    if finish_reason_raw is None:
        raise ThemeFinderError(
            f"Report generation did not return a finish reason for {report_title!r}."
        )

    try:
        finish_reason = FinishReason(int(finish_reason_raw))
    except (TypeError, ValueError) as exc:
        raise ThemeFinderError(
            f"Report generation returned an unknown finish reason "
            f"{finish_reason_raw!r} for {report_title!r}."
        ) from exc

    if finish_reason is not FinishReason.STOP:
        detail = (
            f"Report generation for {report_title!r} did not complete cleanly. "
            f"Finish reason: {finish_reason_name}."
        )
        if finish_message:
            detail = f"{detail} Finish message: {finish_message}"
        raise ThemeFinderError(detail)

    return report_text


def _build_prompt_text(
    prompt_file_text: str,
    question: str,
    add_stats: bool,
    stats_text: str,
) -> str:
    """Build the final prompt text for a report.

    Args:
        prompt_file_text: Base prompt text from configuration.
        question: Survey question being analysed.
        add_stats: Whether to append generated statistics.
        stats_text: Generated statistics block.

    Returns:
        Final prompt text.
    """
    prompt = (
        f"{prompt_file_text}\n\n"
        f"Survey question: **{question}**\n\n"
        "The ThemeFinder output JSON is attached as a document below."
    )

    if add_stats:
        prompt += f"\n\nHere are some statistics about the responses:\n\n{stats_text}"

    return prompt


def _build_model_config(model_cfg: dict[str, Any]) -> dict[str, Any]:
    """Build Vertex generation configuration for one report.

    Args:
        model_cfg: Model configuration block from the report config.

    Returns:
        Dictionary suitable for ``generate_content(generation_config=...)``.

    Raises:
        ConfigurationError: If ``max_output_tokens`` is present but invalid.
    """
    max_output_tokens = model_cfg.get("max_output_tokens", _DEFAULT_MAX_OUTPUT_TOKENS)
    if not isinstance(max_output_tokens, int) or max_output_tokens <= 0:
        raise ConfigurationError("'max_output_tokens' must be a positive integer when specified.")

    temperature = model_cfg.get("temperature", _DEFAULT_TEMPERATURE)
    if not isinstance(temperature, int | float) or temperature < 0:
        raise ConfigurationError("'temperature' must be numeric when specified.")

    return {
        "temperature": float(temperature),
        "max_output_tokens": max_output_tokens,
    }


async def _generate_single_report(config: dict[str, Any]) -> None:
    """Generate and store a single report.

    Args:
        config: Report-generation configuration containing model, prompt parts,
            output details, and model generation settings.

    Raises:
        ThemeFinderError: If the model call fails or the response is incomplete.
        GCSOperationError: If saving the report to GCS fails.
    """
    contents = [Content(role="user", parts=[config["prompt_part"], config["json_part"]])]
    title = str(config.get("title", "report"))
    blob_name = str(config["blob_name"])
    output_bucket = str(config["output_bucket"])
    prefix = f"{blob_name.rsplit('.', 1)[0]}_{title.replace(' ', '_')}"

    logger.info(f"Generating report {title}")

    try:
        response = await asyncio.to_thread(
            config["model"].generate_content,
            contents,
            generation_config=config["model_config"],
        )
    except Exception as exc:
        logger.error(f"Report generation failed for {title!r}")
        raise ThemeFinderError(f"Model failed to generate report {title!r}: {exc}") from exc

    debug_meta = _extract_response_debug(response)
    logger.info(
        "Report generation completed",
        extra={
            "report_title": title,
            "finish_reason": debug_meta["finish_reason"],
            "finish_reason_name": debug_meta["finish_reason_name"],
            "finish_message": debug_meta["finish_message"],
            "text_length": debug_meta["text_length"],
            "usage_metadata": debug_meta["usage_metadata"],
        },
    )

    report_text = _validate_report_response(response=response, report_title=title)
    logger.info(f"Report generated ({len(report_text)} characters)")

    try:
        save_markdown_report_to_gcs(
            report=report_text,
            bucket_name=output_bucket,
            destination_blob_name=prefix,
        )
    except Exception as exc:
        logger.error(f"Failed to save report {title!r} to GCS")
        raise GCSOperationError(f"Failed to save report {title!r} to GCS: {exc}") from exc

    logger.info(f"Report saved to gs://{output_bucket}/{prefix}.md")


async def generate_reports(
    themefinder_output_path: str,
    question: str,
    output_bucket: str,
    project: str,
    location: str,
    config_path: str,
) -> None:
    """Generate all configured reports from ThemeFinder output.

    Args:
        themefinder_output_path: GCS path to the ThemeFinder output JSON file.
        question: Survey question that was analysed.
        output_bucket: GCS bucket to which generated reports should be written.
        project: GCP project ID for Vertex AI.
        location: GCP location for Vertex AI.
        config_path: GCS path to the report configuration JSON file.

    Raises:
        ConfigurationError: If configuration or paths are invalid.
        ThemeFinderError: If report generation fails.
        GCSOperationError: If report storage fails.
    """
    if not themefinder_output_path.startswith("gs://"):
        raise ConfigurationError(
            "THEMEFINDER_OUTPUT_PATH must start with 'gs://', " f"got: {themefinder_output_path!r}"
        )

    path = themefinder_output_path.removeprefix("gs://")
    if "/" not in path:
        raise ConfigurationError(
            "THEMEFINDER_OUTPUT_PATH must be in the form 'gs://bucket/blob', "
            f"got: {themefinder_output_path!r}"
        )

    input_bucket, blob_name = path.split("/", 1)

    result = load_json_from_gcs(input_bucket, blob_name)
    json_bytes = json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8")
    json_part = Part.from_data(data=json_bytes, mime_type="text/plain")
    stats_text = generate_report_stats(result)

    config = get_report_config(config_path=config_path)

    vertexai.init(project=project, location=location)

    report_tasks: list[asyncio.Task[None]] = []

    for report_cfg in config.get(_REPORTS_CONFIG_KEY, []):
        if not isinstance(report_cfg, dict):
            raise ConfigurationError("Each report configuration must be a JSON object.")

        model_cfg_raw = report_cfg.get("model")
        if not isinstance(model_cfg_raw, dict):
            raise ConfigurationError("Each report configuration must contain a 'model' object.")

        model_name = model_cfg_raw.get("model_name")
        if not isinstance(model_name, str) or not model_name.strip():
            raise ConfigurationError(
                "Each report configuration must contain a non-empty model name."
            )

        prompt_file_text = report_cfg.get("prompt_text")
        if not isinstance(prompt_file_text, str) or not prompt_file_text.strip():
            raise ConfigurationError(
                "Each report configuration must contain non-empty 'prompt_text'."
            )

        system_instruction = report_cfg.get("system_instructions")
        if not isinstance(system_instruction, str) or not system_instruction.strip():
            raise ConfigurationError(
                "Each report configuration must contain non-empty " "'system_instructions'."
            )

        add_stats = bool(report_cfg.get("add_stats", False))
        if add_stats and not stats_text:
            logger.warning(
                "Report config requests statistics to be added, but no "
                "statistics were generated."
            )

        prompt_part = Part.from_text(
            _build_prompt_text(
                prompt_file_text=prompt_file_text,
                question=question,
                add_stats=add_stats,
                stats_text=stats_text,
            )
        )

        model = GenerativeModel(
            model_name=model_name,
            system_instruction=system_instruction,
        )

        generation_request_config = {
            "model_config": _build_model_config(model_cfg_raw),
            "model": model,
            "title": report_cfg.get("title", "report"),
            "blob_name": blob_name,
            "prompt_part": prompt_part,
            "json_part": json_part,
            "output_bucket": output_bucket,
        }

        report_tasks.append(
            asyncio.create_task(_generate_single_report(config=generation_request_config))
        )

    await asyncio.gather(*report_tasks)
    logger.info("All report generation tasks completed")


async def run() -> None:
    """Run the report-generation pipeline from environment variables.

    Raises:
        ConfigurationError: If required environment variables are missing or invalid.
    """
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
    config_path = os.getenv("REPORT_CONFIG_PATH", "")

    if not output_bucket or not themefinder_output_path or not project:
        msg = (
            "Environment variables OUTPUT_BUCKET, THEMEFINDER_OUTPUT_PATH, and "
            "GCP_PROJECT must be set in your .env file."
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
        config_path=config_path,
    )


if __name__ == "__main__":
    asyncio.run(run())
