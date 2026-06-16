"""Tests for report generator module"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from vertexai.generative_models import GenerativeModel, Part

from survey_assist_themes.exceptions import (
    ConfigurationError,
    GCSOperationError,
    ThemeFinderError,
)
from survey_assist_themes.report_generator import (
    _generate_single_report,
    generate_report_stats,
    generate_reports,
    get_report_config,
)
from survey_assist_themes.utils.file_utils import rationalise_themefinder_output


def mock_vertex_response(
    text: str = "Generated report content",
    finish_reason: int = 1,
) -> MagicMock:
    response = MagicMock()
    response.text = text

    candidate = MagicMock()
    candidate.finish_reason = finish_reason
    candidate.finish_message = ""

    response.candidates = [candidate]
    response.usage_metadata = None
    return response


@pytest.fixture
def base_config() -> dict[str, Any]:
    """Fixture for common config used in multiple tests."""
    return {
        "model": MagicMock(spec=GenerativeModel),
        "model_config": {"temperature": 0.2},
        "title": "Executive Summary",
        "blob_name": "themefinder_output.json",
        "prompt_part": Part.from_text("Test prompt"),
        "json_part": Part.from_data(data=b"test", mime_type="text/plain"),
        "output_bucket": "test-bucket",
    }


@pytest.fixture
def themefinder_result() -> dict[str, Any]:
    """Fixture for typical ThemeFinder output."""
    return {
        "question": "Why did you rate your nursing service as good or very good?",
        "themes": [
            {
                "topic_id": "A",
                "topic": "Theme A",
                "source_topic_count": 1,
            }
        ],
        "mapping": [
            {
                "response_id": 1,
                "response": "Friendly informative and helpful",
                "labels": ["A"],
            }
        ],
        "sentiment": [
            {
                "response_id": 1,
                "response": "Friendly informative and helpful",
                "position": "AGREEMENT",
            }
        ],
        "detailed_responses": [
            {
                "response_id": 1,
                "response": "Friendly informative and helpful",
                "evidence_rich": "YES",
            }
        ],
        "unprocessables": [],
    }


@pytest.fixture
def integration_themefinder_result() -> dict[str, Any]:
    """Fixture for realistic ThemeFinder output for integration tests."""
    return {
        "themes": [
            {
                "topic_id": "A",
                "topic": "Inadequate Appointment System",
                "source_topic_count": 1,
            },
            {
                "topic_id": "B",
                "topic": "Consultation Experience",
                "source_topic_count": 1,
            },
        ],
    }


@pytest.fixture
def single_report_config() -> dict[str, Any]:
    """Fixture for single report configuration."""
    return {
        "reports_config": [
            {
                "model": {"model_name": "gemini-2.5-flash", "temperature": 0.2},
                "prompt_text": "Generate report",
                "system_instructions": "You are helpful",
                "title": "Executive Summary",
                "add_stats": False,
            }
        ]
    }


@pytest.fixture
def multiple_reports_config() -> dict[str, Any]:
    """Fixture for multiple report configurations."""
    return {
        "reports_config": [
            {
                "model": {"model_name": "gemini-2.5-flash", "temperature": 0.2},
                "prompt_text": "Summary prompt",
                "system_instructions": "You are helpful",
                "title": "Executive Summary",
            },
            {
                "model": {"model_name": "gemini-2.5-flash", "temperature": 0.3},
                "prompt_text": "Detail prompt",
                "system_instructions": "Be detailed",
                "title": "Detailed Analysis",
            },
        ]
    }


class TestGetReportConfig:
    """Tests for get_report_config function."""

    def test_get_report_config_success(self) -> None:
        """Test successfully loading report configuration."""
        config_data = {
            "reports_config": [
                {
                    "model": {"model_name": "gemini-2.5-flash", "temperature": 0.2},
                    "prompt_text": "Generate a report",
                    "system_instructions": "You are a report generator",
                }
            ]
        }

        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs", return_value=config_data
        ):
            result = get_report_config(config_path="gs://bucket/config.json")

        assert result == config_data
        assert result["reports_config"][0]["model"]["model_name"] == "gemini-2.5-flash"
        assert result["reports_config"][0]["model"]["temperature"] == 0.2

    def test_get_report_config_file_not_found(self) -> None:
        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            side_effect=GCSOperationError("Config not found"),
        ):
            with pytest.raises(ConfigurationError) as exc_info:
                get_report_config(config_path="gs://bucket/missing_config.json")

        assert "Failed to load report configuration" in str(exc_info.value)

    def test_get_report_config_invalid_json(self) -> None:
        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value="{ invalid json",
        ):
            with pytest.raises(ConfigurationError) as exc_info:
                get_report_config(config_path="gs://bucket/invalid_config.json")

        assert "Report configuration must be a JSON object" in str(exc_info.value)

    def test_get_multiple_report_configs(self) -> None:
        """Test loading config file with multilpe report configs."""
        config_data = {
            "reports_config": [
                {
                    "model": {"model_name": "gemini-2.5-flash", "temperature": 0.2},
                    "prompt_text": "Executive summary",
                    "system_instructions": "Be concise",
                    "title": "Summary",
                },
                {
                    "model": {"model_name": "gemini-2.5-flash", "temperature": 0.3},
                    "prompt_text": "Detailed analysis",
                    "system_instructions": "Be thorough",
                    "title": "Detailed",
                },
            ]
        }

        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value=config_data,
        ):
            result = get_report_config(config_path="gs://bucket/multiple_reports_config.json")

        assert len(result["reports_config"]) == 2
        assert result["reports_config"][0]["title"] == "Summary"
        assert result["reports_config"][1]["title"] == "Detailed"

    def test_get_report_config_missing_keys(self) -> None:
        config_data = {"unexpected_key": "value"}

        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value=config_data,
        ):
            with pytest.raises(ConfigurationError) as exc_info:
                get_report_config(config_path="gs://bucket/missing_keys_config.json")

        assert "Missing required key 'reports_config'" in str(exc_info.value)


class TestGenerateReportStats:
    """Tests for generate_report_stats function."""

    def test_generate_report_stats_basic(self) -> None:
        """Test basic stats generation."""
        result = {
            "themes": {
                "A": {"topic": "Theme A"},
                "B": {"topic": "Theme B"},
            },
            "responses": {
                "1": {
                    "labels": ["A"],
                    "sentiment": "AGREEMENT",
                    "evidence_rich": True,
                    "processable": True,
                },
                "2": {
                    "labels": ["A", "B"],
                    "sentiment": "DISAGREEMENT",
                    "evidence_rich": False,
                    "processable": True,
                },
                "3": {
                    "labels": [],
                    "sentiment": "UNCLEAR",
                    "evidence_rich": True,
                    "processable": True,
                },
                "4": {
                    "processable": False,
                },
            },
        }

        stats = generate_report_stats(result)

        assert "Total responses: 4" in stats
        assert "Total responses processed: 3" in stats
        assert "Total unprocessables: 1" in stats
        assert "[A] Theme A | Count: 2 (50.0%)" in stats
        assert "[B] Theme B | Count: 1 (25.0%)" in stats
        assert "Responses not mapped to any theme: 1 (33.3%)" in stats
        assert "Responses mapped to multiple themes: 1" in stats
        assert "Sentiment: 1 Agreement, 1 Disagreement, 1 Unclear" in stats
        assert "Depth: 2 evidence-rich, 1 surface-level" in stats

    def test_generate_report_stats_empty(self) -> None:
        result: dict[str, dict[str, dict[str, Any]]] = {
            "themes": {},
            "responses": {},
        }

        stats = generate_report_stats(result)

        assert "Total responses processed: 0" in stats
        assert "Total unprocessables: 0" in stats
        assert "Sentiment: 0 Agreement, 0 Disagreement, 0 Unclear" in stats

    def test_generate_report_stats_no_divisions_by_zero(self) -> None:
        """Test that stats handles zero responses without division errors."""
        result = {
            "themes": {
                "A": {"topic": "Theme"},
            },
            "responses": {},
        }

        stats = generate_report_stats(result)

        assert "Total responses processed: 0" in stats

    def test_generate_report_stats_all_agreement(self) -> None:
        """Test stats when all sentiments are agreement."""
        result = {
            "themes": {
                "A": {"topic": "Theme A"},
            },
            "responses": {
                "1": {
                    "labels": ["A"],
                    "sentiment": "AGREEMENT",
                    "evidence_rich": True,
                    "processable": True,
                },
                "2": {
                    "labels": ["A"],
                    "sentiment": "AGREEMENT",
                    "evidence_rich": True,
                    "processable": True,
                },
            },
        }

        stats = generate_report_stats(result)

        assert "Sentiment: 2 Agreement, 0 Disagreement, 0 Unclear" in stats

    def test_generate_reports_rationalises_themefinder_output(
        self, themefinder_result: dict[str, Any], single_report_config: dict[str, Any]
    ) -> None:
        compact_result = rationalise_themefinder_output(themefinder_result)

        with patch("survey_assist_themes.report_generator.load_json_from_gcs") as mock_load:
            mock_load.return_value = themefinder_result

            with patch(
                "survey_assist_themes.report_generator.rationalise_themefinder_output",
                return_value=compact_result,
            ) as mock_rationalise:
                with patch(
                    "survey_assist_themes.report_generator.generate_report_stats"
                ) as mock_stats:
                    mock_stats.return_value = "Statistics summary"

                    with patch(
                        "survey_assist_themes.report_generator.get_report_config"
                    ) as mock_config:
                        mock_config.return_value = single_report_config

                        with patch("vertexai.init"):
                            with patch("survey_assist_themes.report_generator.GenerativeModel"):
                                with patch(
                                    "survey_assist_themes.report_generator._generate_single_report"
                                ):
                                    asyncio.run(
                                        generate_reports(
                                            themefinder_output_path="gs://bucket/output.json",
                                            question="Test question?",
                                            output_bucket="output-bucket",
                                            project="test-project",
                                            location="europe-west2",
                                            config_path="gs://bucket/report_config.json",
                                        )
                                    )

        mock_rationalise.assert_called_once_with(themefinder_result)
        mock_stats.assert_called_once_with(compact_result)


class TestGenerateSingleReport:
    """Tests for _generate_single_report async function."""

    def test_generate_single_report_success(self, base_config: dict[str, Any]) -> None:
        base_config["model"].generate_content.return_value = mock_vertex_response(
            "Generated report content"
        )

        with patch(
            "survey_assist_themes.report_generator.save_markdown_report_to_gcs"
        ) as mock_save:
            asyncio.run(_generate_single_report(base_config))

        mock_save.assert_called_once()
        call_args = mock_save.call_args
        assert call_args[1]["bucket_name"] == "test-bucket"
        assert "Executive_Summary" in call_args[1]["destination_blob_name"]
        assert call_args[1]["report"] == "Generated report content"

    def test_generate_single_report_empty_response(self, base_config: dict[str, Any]) -> None:
        base_config["model"].generate_content.return_value = mock_vertex_response("")

        with pytest.raises(ThemeFinderError, match="empty response"):
            asyncio.run(_generate_single_report(base_config))

    def test_generate_single_report_model_error(self, base_config: dict[str, Any]) -> None:
        """Test error handling when model generation fails."""

        base_config["model"].generate_content.side_effect = Exception("API Error")

        with pytest.raises(ThemeFinderError, match="Model failed to generate report"):
            asyncio.run(_generate_single_report(base_config))

    @pytest.mark.asyncio
    async def test_generate_single_report_gcs_save_error(self, base_config: dict[str, Any]) -> None:
        mock_response = mock_vertex_response("Valid content")

        with patch("asyncio.to_thread", return_value=mock_response):
            with patch(
                "survey_assist_themes.report_generator.save_markdown_report_to_gcs",
                side_effect=Exception("GCS Error"),
            ):
                with pytest.raises(GCSOperationError, match="Failed to save report"):
                    await _generate_single_report(base_config)


class TestGenerateReport:
    """Tests for generate_reports async function."""

    def test_generate_report_single_config(
        self, themefinder_result: dict[str, Any], single_report_config: dict[str, Any]
    ) -> None:
        """Test report generation with single report config."""
        with patch("survey_assist_themes.report_generator.load_json_from_gcs") as mock_load:
            mock_load.return_value = themefinder_result

            with patch("survey_assist_themes.report_generator.get_report_config") as mock_config:
                mock_config.return_value = single_report_config

                with patch("vertexai.init"):
                    with patch("survey_assist_themes.report_generator.GenerativeModel"):
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report"
                        ) as mock_generate:
                            import asyncio

                            asyncio.run(
                                generate_reports(
                                    themefinder_output_path="gs://bucket/output.json",
                                    question="Test question?",
                                    output_bucket="output-bucket",
                                    project="test-project",
                                    location="europe-west2",
                                    config_path="gs://bucket/report_config.json",
                                )
                            )

                            mock_load.assert_called_once_with("bucket", "output.json")

                            assert mock_generate.call_count == 1

    def test_generate_reports_multiple_configs(
        self, themefinder_result: dict[str, Any], multiple_reports_config: dict[str, Any]
    ) -> None:
        """Test report generation with multiple report configs."""
        with patch("survey_assist_themes.report_generator.load_json_from_gcs") as mock_load:
            mock_load.return_value = themefinder_result

            with patch("survey_assist_themes.report_generator.get_report_config") as mock_config:
                mock_config.return_value = multiple_reports_config

                with patch("vertexai.init"):
                    with patch("survey_assist_themes.report_generator.GenerativeModel"):
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report"
                        ) as mock_generate:
                            import asyncio

                            asyncio.run(
                                generate_reports(
                                    themefinder_output_path="gs://bucket/output.json",
                                    question="Test?",
                                    output_bucket="output",
                                    project="proj",
                                    location="loc",
                                    config_path="gs://bucket/multiple_reports_config.json",
                                )
                            )

                            assert mock_generate.call_count == 2

    def test_generate_reports_parses_gcs_path(
        self, themefinder_result: dict[str, Any], single_report_config: dict[str, Any]
    ) -> None:
        """Test that GCS path is parsed correctly."""
        with patch("survey_assist_themes.report_generator.load_json_from_gcs") as mock_load:
            mock_load.return_value = themefinder_result

            with patch("survey_assist_themes.report_generator.get_report_config") as mock_config:
                mock_config.return_value = single_report_config

                with patch("vertexai.init"):
                    with patch("survey_assist_themes.report_generator.GenerativeModel"):
                        with patch("survey_assist_themes.report_generator._generate_single_report"):
                            import asyncio

                            asyncio.run(
                                generate_reports(
                                    themefinder_output_path="gs://my-bucket/path/to/output.json",
                                    question="Question?",
                                    output_bucket="output",
                                    project="proj",
                                    location="loc",
                                    config_path="gs://bucket/report_config.json",
                                )
                            )

                            mock_load.assert_called_once_with("my-bucket", "path/to/output.json")

    @pytest.mark.asyncio
    async def test_generate_reports_invalid_gcs_path_no_slash(self) -> None:
        """Test error handling for invalid GCS path without slash."""
        with patch("survey_assist_themes.report_generator.load_json_from_gcs"):
            with pytest.raises(ConfigurationError, match="must be in the form"):
                await generate_reports(
                    themefinder_output_path="gs://bucket",
                    question="Q?",
                    output_bucket="out",
                    project="p",
                    location="l",
                    config_path="gs://bucket/report_config.json",
                )

    @pytest.mark.asyncio
    async def test_generate_reports_invalid_gcs_path_no_gs(self) -> None:
        """Test error handling for GCS path without gs:// prefix."""
        with patch("survey_assist_themes.report_generator.load_json_from_gcs"):
            with pytest.raises(ConfigurationError, match="must start with 'gs://'"):
                await generate_reports(
                    themefinder_output_path="bucket/blob",
                    question="Q?",
                    output_bucket="out",
                    project="p",
                    location="l",
                    config_path="gs://bucket/report_config.json",
                )


class TestIntegration:
    """Integration tests for full workflow."""

    def test_full_report_generation_workflow(
        self, integration_themefinder_result: dict[str, Any], single_report_config: dict[str, Any]
    ) -> None:
        """Test complete report generation workflow with mocked dependencies."""
        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value=integration_themefinder_result,
        ):
            with patch("survey_assist_themes.report_generator.get_report_config") as mock_config:
                mock_config.return_value = single_report_config

                with patch("vertexai.init"):
                    with patch("survey_assist_themes.report_generator.GenerativeModel"):
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report"
                        ) as mock_generate:
                            import asyncio

                            asyncio.run(
                                generate_reports(
                                    themefinder_output_path="gs://input/output.json",
                                    question="Feedback?",
                                    output_bucket="output-bucket",
                                    project="test-project",
                                    location="europe-west2",
                                    config_path="gs://bucket/report_config.json",
                                )
                            )

                            assert mock_generate.call_count == 1

                            call_config = mock_generate.call_args[1]["config"]
                            assert call_config["output_bucket"] == "output-bucket"
                            assert call_config["title"] == "Executive Summary"

    def test_full_workflow_with_multiple_themes(self, single_report_config: dict[str, Any]) -> None:
        """Test workflow handles multiple themes correctly."""
        themefinder_result = {
            "themes": [
                {
                    "topic_id": "A",
                    "topic": "Theme A",
                    "source_topic_count": 1,
                },
                {
                    "topic_id": "B",
                    "topic": "Theme B",
                    "source_topic_count": 1,
                },
                {
                    "topic_id": "C",
                    "topic": "Theme C",
                    "source_topic_count": 1,
                },
            ],
            "mapping": [
                {"response_id": 1, "response": "Friendly and professional", "labels": ["A"]},
                {"response_id": 2, "response": "Seen promptly", "labels": ["B"]},
                {"response_id": 3, "response": "Annual health check", "labels": ["C"]},
                {"response_id": 4, "response": "Friendly and prompt", "labels": ["A", "B"]},
            ],
            "sentiment": [
                {
                    "response_id": 1,
                    "response": "Friendly and professional",
                    "position": "AGREEMENT",
                },
                {"response_id": 2, "response": "Seen promptly", "position": "DISAGREEMENT"},
                {"response_id": 3, "response": "Annual health check", "position": "UNCLEAR"},
                {"response_id": 4, "response": "Friendly and prompt", "position": "AGREEMENT"},
            ],
            "detailed_responses": [
                {"response_id": 1, "response": "Friendly and professional", "evidence_rich": "YES"},
                {"response_id": 2, "response": "Seen promptly", "evidence_rich": "NO"},
                {"response_id": 3, "response": "Annual health check", "evidence_rich": "YES"},
                {"response_id": 4, "response": "Friendly and prompt", "evidence_rich": "YES"},
            ],
            "unprocessables": [],
        }
        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value=themefinder_result,
        ):
            with patch("survey_assist_themes.report_generator.get_report_config") as mock_config:
                mock_config.return_value = single_report_config

                with patch("vertexai.init"):
                    with patch("survey_assist_themes.report_generator.GenerativeModel"):
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report"
                        ) as mock_generate:
                            import asyncio

                            asyncio.run(
                                generate_reports(
                                    themefinder_output_path="gs://input/themes.json",
                                    question="Feedback?",
                                    output_bucket="output-bucket",
                                    project="test-project",
                                    location="europe-west2",
                                    config_path="gs://bucket/report_config.json",
                                )
                            )

                            assert mock_generate.call_count == 1

    def test_full_workflow_loads_correct_gcs_file(
        self, integration_themefinder_result: dict[str, Any], single_report_config: dict[str, Any]
    ) -> None:
        """Test that workflow loads the correct file from GCS."""
        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value=integration_themefinder_result,
        ) as mock_load:
            with patch("survey_assist_themes.report_generator.get_report_config") as mock_config:
                mock_config.return_value = single_report_config

                with patch("vertexai.init"):
                    with patch("survey_assist_themes.report_generator.GenerativeModel"):
                        with patch("survey_assist_themes.report_generator._generate_single_report"):
                            import asyncio

                            asyncio.run(
                                generate_reports(
                                    themefinder_output_path="gs://my-input-bucket/survey/results.json",
                                    question="Any feedback?",
                                    output_bucket="my-output-bucket",
                                    project="my-project",
                                    location="us-central1",
                                    config_path="gs://bucket/report_config.json",
                                )
                            )

                            mock_load.assert_called_once_with(
                                "my-input-bucket", "survey/results.json"
                            )

    def test_full_workflow_with_stats_enabled(
        self, integration_themefinder_result: dict[str, Any], single_report_config: dict[str, Any]
    ) -> None:
        """Test workflow generates stats when enabled in config."""
        single_report_config["reports_config"][0]["add_stats"] = True
        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value=integration_themefinder_result,
        ):
            with patch("survey_assist_themes.report_generator.get_report_config") as mock_config:
                mock_config.return_value = single_report_config

                with patch(
                    "survey_assist_themes.report_generator.generate_report_stats"
                ) as mock_stats:
                    mock_stats.return_value = "Statistics summary"

                    with patch("vertexai.init"):
                        with patch("survey_assist_themes.report_generator.GenerativeModel"):
                            with patch(
                                "survey_assist_themes.report_generator._generate_single_report"
                            ):
                                import asyncio

                                asyncio.run(
                                    generate_reports(
                                        themefinder_output_path="gs://input/output.json",
                                        question="Feedback?",
                                        output_bucket="output-bucket",
                                        project="test-project",
                                        location="europe-west2",
                                        config_path="gs://bucket/report_config.json",
                                    )
                                )

                                assert mock_stats.called

    def test_full_workflow_passes_question_to_prompt(
        self, integration_themefinder_result: dict[str, Any], single_report_config: dict[str, Any]
    ) -> None:
        """Test that the survey question is included in the prompt."""
        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value=integration_themefinder_result,
        ):
            with patch("survey_assist_themes.report_generator.get_report_config") as mock_config:
                mock_config.return_value = single_report_config

                with patch("vertexai.init"):
                    with patch("survey_assist_themes.report_generator.GenerativeModel"):
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report"
                        ) as mock_generate:
                            import asyncio

                            survey_question = "What is your overall satisfaction?"

                            asyncio.run(
                                generate_reports(
                                    themefinder_output_path="gs://input/output.json",
                                    question=survey_question,
                                    output_bucket="output-bucket",
                                    project="test-project",
                                    location="europe-west2",
                                    config_path="gs://bucket/report_config.json",
                                )
                            )

                            assert mock_generate.called

                            call_config = mock_generate.call_args[1]["config"]
                            prompt_text = call_config["prompt_part"]
                            assert survey_question in str(prompt_text)

    def test_full_workflow_end_to_end(
        self, integration_themefinder_result: dict[str, Any], single_report_config: dict[str, Any]
    ) -> None:
        """Test complete end-to-end workflow with all components."""
        with patch(
            "survey_assist_themes.report_generator.load_json_from_gcs",
            return_value=integration_themefinder_result,
        ) as mock_load:
            with patch(
                "survey_assist_themes.report_generator.get_report_config"
            ) as mock_get_config:
                mock_get_config.return_value = single_report_config

                with patch("vertexai.init") as mock_init:
                    with patch(
                        "survey_assist_themes.report_generator.GenerativeModel"
                    ) as mock_model_class:
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report"
                        ) as mock_generate:
                            import asyncio

                            asyncio.run(
                                generate_reports(
                                    themefinder_output_path="gs://input/output.json",
                                    question="Feedback?",
                                    output_bucket="output-bucket",
                                    project="test-project",
                                    location="europe-west2",
                                    config_path="gs://bucket/report_config.json",
                                )
                            )

                            mock_load.assert_called_once_with("input", "output.json")
                            mock_get_config.assert_called_once()
                            mock_init.assert_called_once_with(
                                project="test-project", location="europe-west2"
                            )
                            assert mock_model_class.called
                            assert mock_generate.call_count == 1
