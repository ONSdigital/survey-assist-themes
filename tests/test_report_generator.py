"""Tests for report generator module"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, mock_open, patch, call
from datetime import UTC, datetime

import pytest
from vertexai.generative_models import GenerativeModel, Part, Content

from survey_assist_themes.report_generator import (
    get_report_config,
    generate_report_stats,
    _generate_single_report,
    generate_report,
)
from survey_assist_themes.exceptions import ConfigurationError, GCSOperationError, ThemeFinderError


class TestGetReportConfig:
    """Tests for get_report_config function."""

    def test_get_report_config_success(self):
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
    
        with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
            result = get_report_config()
    
        assert result == config_data
        assert result["reports_config"][0]["model"]["model_name"] == "gemini-2.5-flash"
        assert result["reports_config"][0]["model"]["temperature"] == 0.2

    def test_get_report_config_file_not_found(self):
        """Test error handling when config file not found."""
        with patch("builtins.open", side_effect=FileNotFoundError("Config not found")):
            with pytest.raises(ConfigurationError) as exc_info:
                get_report_config()
            assert "Failed to load report configuration" in str(exc_info.value)

    def test_get_report_config_invalid_json(self):
        """Test error handling for invalid JSON."""
        invalid_json = "{ invalid json"
        
        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with pytest.raises(ConfigurationError) as exc_info:
                get_report_config()
            assert "Failed to load report configuration" in str(exc_info.value)

    def test_get_multiple_report_configs(self):
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

        with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
            result = get_report_config()

        assert len(result["reports_config"]) == 2
        assert result["reports_config"][0]["title"] == "Summary"
        assert result["reports_config"][1]["title"] == "Detailed"

    def test_get_report_config_missing_keys(self):
        """Test handling of missing expected keys in config."""
        config_data = {"unexpected_key": "value"}
        with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
            with pytest.raises(ConfigurationError) as exc_info:
                get_report_config()
            assert "Failed to load report configuration" in str(exc_info.value)


class TestGenerateReportStats:
    """Tests for generate_report_stats function."""

    def test_generate_report_stats_basic(self):
        """Test basic stats generation."""
        result = {
            "themes": [
                {"topic_id": "A", "topic": "Theme A"},
                {"topic_id": "B", "topic": "Theme B"},
            ],
            "mapping": [
                {"response_id": 1, "labels": ["A"]},
                {"response_id": 2, "labels": ["A", "B"]},
                {"response_id": 3, "labels": []},
            ],
            "sentiment": [
                {"position": "AGREEMENT"},
                {"position": "DISAGREEMENT"},
                {"position": "UNCLEAR"},
            ],
            "detailed_responses": [
                {"evidence_rich": "YES"},
                {"evidence_rich": "NO"},
                {"evidence_rich": "YES"},
            ],
            "unprocessables": [{"error": "invalid"}],
        }

        stats = generate_report_stats(result)

        assert "Total responses processed: 3" in stats
        assert "Total unprocessables: 1" in stats
        assert "[A] Theme A | Count: 2 (66.7%)" in stats
        assert "[B] Theme B | Count: 1 (33.3%)" in stats
        assert "Responses not mapped to any theme: 1 (33.3%)" in stats
        assert "Responses mapped to multiple themes: 1" in stats
        assert "Sentiment breakdown: 1 Agreement, 1 Disagreement, 1 Unclear" in stats
        assert "Feedback depth: 2 evidence-rich responses vs 1 surface-level" in stats

    def test_generate_report_stats_empty(self):
        """Test stats with empty data."""
        result = {
            "themes": [],
            "mapping": [],
            "sentiment": [],
            "detailed_responses": [],
            "unprocessables": [],
        }

        stats = generate_report_stats(result)

        assert "Total responses processed: 0" in stats
        assert "Total unprocessables: 0" in stats
        assert "Sentiment breakdown: 0 Agreement, 0 Disagreement, 0 Unclear" in stats

    def test_generate_report_stats_no_divisions_by_zero(self):
        """Test that stats handles zero responses without division errors."""
        result = {
            "themes": [{"topic_id": "A", "topic": "Theme"}],
            "mapping": [],
            "sentiment": [],
            "detailed_responses": [],
            "unprocessables": [],
        }

        # Should not raise ZeroDivisionError
        stats = generate_report_stats(result)
        assert "Total responses processed: 0" in stats

    def test_generate_report_stats_all_agreement(self):
        """Test stats when all sentiments are agreement."""
        result = {
            "themes": [{"topic_id": "A", "topic": "Theme A"}],
            "mapping": [
                {"response_id": 1, "labels": ["A"]},
                {"response_id": 2, "labels": ["A"]},
            ],
            "sentiment": [
                {"position": "AGREEMENT"},
                {"position": "AGREEMENT"},
            ],
            "detailed_responses": [
                {"evidence_rich": "YES"},
                {"evidence_rich": "YES"},
            ],
            "unprocessables": [],
        }

        stats = generate_report_stats(result)
        assert "Sentiment breakdown: 2 Agreement, 0 Disagreement, 0 Unclear" in stats

@pytest.fixture
def base_config():
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

class TestGenerateSingleReport:
    """Tests for _generate_single_report async function."""

    def test_generate_single_report_success(self, base_config):
        """Test successful single report generation."""

        mock_response = MagicMock()
        mock_response.text = "Generated report content"
        base_config["model"].generate_content.return_value = mock_response
 
        with patch(
            "survey_assist_themes.report_generator.save_markdown_report_to_gcs"
        ) as mock_save:
            asyncio.run(_generate_single_report(base_config))

        mock_save.assert_called_once()
        call_args = mock_save.call_args
 
        assert call_args[1]["bucket_name"] == "test-bucket"
        assert "Executive_Summary" in call_args[1]["destination_blob_name"]
        assert call_args[1]["report"] == "Generated report c ontent"

    def test_generate_single_report_empty_response(self, base_config):
        """Test error handling when model returns empty response."""

        mock_response = MagicMock()
        mock_response.text = ""
        base_config["model"].generate_content.return_value = mock_response
 
        with pytest.raises(ValueError, match="LLM response missing or empty"):
            asyncio.run(_generate_single_report(base_config))

    def test_generate_single_report_model_error(self, base_config):
        """Test error handling when model generation fails."""

        base_config["model"].generate_content.side_effect = Exception("API Error")
 
        with pytest.raises(ThemeFinderError, match="Model failed to generate report"):
            asyncio.run(_generate_single_report(base_config))

    @pytest.mark.asyncio
    async def test_generate_single_report_gcs_save_error(self, base_config): #TODO; async not working
        """Test error handling when GCS save fails."""
        mock_response = MagicMock()
        mock_response.text = "Valid content"

        with patch("asyncio.to_thread") as mock_to_thread:
            mock_to_thread.return_value = mock_response
            with patch(
                "survey_assist_themes.report_generator.save_markdown_report_to_gcs",
                side_effect=Exception("GCS Error"),
            ):
                with pytest.raises(GCSOperationError, match="Failed to save report to GCS"):
                    asyncio.run(_generate_single_report(base_config))


class TestGenerateReport:
    """Tests for generate_report async function."""

    @pytest.mark.asyncio
    async def test_generate_report_single_config(self):
        """Test report generation with single report config."""
        with patch(
            "survey_assist_themes.report_generator.load_themefinder_output_from_gcs"
        ) as mock_load:
            mock_load.return_value = {
                "themes": [{"topic_id": "A", "topic": "Theme A"}],
                "mapping": [{"response_id": 1, "labels": ["A"]}],
                "sentiment": [{"position": "AGREEMENT"}],
                "detailed_responses": [{"evidence_rich": "YES"}],
                "unprocessables": [],
            }

            with patch(
                "survey_assist_themes.report_generator.get_report_config"
            ) as mock_config:
                mock_config.return_value = {
                    "reports_config": [
                        {
                            "model": {"model_name": "gemini-1.5-pro", "temperature": 0.2},
                            "prompt_text": "Generate report",
                            "system_instructions": "You are helpful",
                            "title": "Executive Summary",
                            "add_stats": False,
                        }
                    ]
                }

                with patch("vertexai.init"):
                    with patch(
                        "survey_assist_themes.report_generator.GenerativeModel"
                    ) as mock_model_class:
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report",
                            new_callable=AsyncMock,
                        ) as mock_generate:
                            await generate_report(
                                themefinder_output_path="gs://bucket/output.json",
                                question="Test question?",
                                output_bucket="output-bucket",
                                project="test-project",
                                location="europe-west2",
                            )

                            mock_load.assert_called_once_with("bucket", "output.json")
                            mock_generate.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_report_multiple_configs(self):
        """Test report generation with multiple report configs."""
        with patch(
            "survey_assist_themes.report_generator.load_themefinder_output_from_gcs"
        ) as mock_load:
            mock_load.return_value = {
                "themes": [],
                "mapping": [],
                "sentiment": [],
                "detailed_responses": [],
                "unprocessables": [],
            }

            with patch(
                "survey_assist_themes.report_generator.get_report_config"
            ) as mock_config:
                mock_config.return_value = {
                    "reports_config": [
                        {
                            "model": {"model_name": "gemini-1.5-pro", "temperature": 0.2},
                            "prompt_text": "Summary prompt",
                            "system_instructions": "You are helpful",
                            "title": "Executive Summary",
                        },
                        {
                            "model": {"model_name": "gemini-1.5-pro", "temperature": 0.3},
                            "prompt_text": "Detail prompt",
                            "system_instructions": "Be detailed",
                            "title": "Detailed Analysis",
                        },
                    ]
                }

                with patch("vertexai.init"):
                    with patch(
                        "survey_assist_themes.report_generator.GenerativeModel"
                    ):
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report",
                            new_callable=AsyncMock,
                        ) as mock_generate:
                            await generate_report(
                                themefinder_output_path="gs://bucket/output.json",
                                question="Test?",
                                output_bucket="output",
                                project="proj",
                                location="loc",
                            )

                            # Should create tasks for both configs
                            assert mock_generate.await_count == 2

    @pytest.mark.asyncio
    async def test_generate_report_with_stats(self):
        """Test report generation including stats generation."""
        with patch(
            "survey_assist_themes.report_generator.load_themefinder_output_from_gcs"
        ) as mock_load:
            mock_load.return_value = {
                "themes": [{"topic_id": "A", "topic": "Theme"}],
                "mapping": [{"response_id": 1, "labels": ["A"]}],
                "sentiment": [{"position": "AGREEMENT"}],
                "detailed_responses": [{"evidence_rich": "YES"}],
                "unprocessables": [],
            }

            with patch(
                "survey_assist_themes.report_generator.get_report_config"
            ) as mock_config:
                mock_config.return_value = {
                    "reports_config": [
                        {
                            "model": {"model_name": "gemini-1.5-pro", "temperature": 0.2},
                            "prompt_text": "Generate",
                            "system_instructions": "Help",
                            "add_stats": True,
                        }
                    ]
                }

                with patch("vertexai.init"):
                    with patch(
                        "survey_assist_themes.report_generator.GenerativeModel"
                    ):
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report",
                            new_callable=AsyncMock,
                        ) as mock_generate:
                            with patch(
                                "survey_assist_themes.report_generator.generate_report_stats"
                            ) as mock_stats:
                                mock_stats.return_value = "Stats summary"

                                await generate_report(
                                    themefinder_output_path="gs://bucket/output.json",
                                    question="Q?",
                                    output_bucket="out",
                                    project="p",
                                    location="l",
                                )

                                mock_stats.assert_called_once()

    def test_generate_report_invalid_gcs_path_no_slash(self):
        """Test error handling for invalid GCS path."""
        with pytest.raises(ConfigurationError, match="must be in the form"):
            asyncio.run(
                generate_report(
                    themefinder_output_path="gs://bucket",
                    question="Q?",
                    output_bucket="out",
                    project="p",
                    location="l",
                )
            )

    def test_generate_report_invalid_gcs_path_no_gs(self):
        """Test error handling for path without gs:// prefix."""
        with pytest.raises(ConfigurationError, match="must be in the form"):
            asyncio.run(
                generate_report(
                    themefinder_output_path="bucket",
                    question="Q?",
                    output_bucket="out",
                    project="p",
                    location="l",
                )
            )


class TestIntegration:
    """Integration tests for full workflow."""

    @pytest.mark.asyncio
    async def test_full_report_generation_workflow(self):
        """Test complete report generation workflow with mocked dependencies."""
        themefinder_result = {
            "themes": [
                {"topic_id": "A", "topic": "Inadequate Appointment System"},
                {"topic_id": "B", "topic": "Consultation Experience"},
            ],
            "mapping": [
                {"response_id": 1, "labels": ["A"]},
                {"response_id": 2, "labels": ["A", "B"]},
            ],
            "sentiment": [
                {"position": "AGREEMENT"},
                {"position": "DISAGREEMENT"},
            ],
            "detailed_responses": [
                {"evidence_rich": "YES"},
                {"evidence_rich": "YES"},
            ],
            "unprocessables": [],
        }

        with patch(
            "survey_assist_themes.report_generator.load_themefinder_output_from_gcs",
            return_value=themefinder_result,
        ):
            with patch(
                "survey_assist_themes.report_generator.get_report_config"
            ) as mock_config:
                mock_config.return_value = {
                    "reports_config": [
                        {
                            "model": {
                                "model_name": "gemini-1.5-pro",
                                "temperature": 0.2,
                            },
                            "prompt_text": "Summarize the themes",
                            "system_instructions": "You generate reports",
                            "title": "Summary",
                            "add_stats": True,
                        }
                    ]
                }

                with patch("vertexai.init"):
                    with patch(
                        "survey_assist_themes.report_generator.GenerativeModel"
                    ):
                        with patch(
                            "survey_assist_themes.report_generator._generate_single_report",
                            new_callable=AsyncMock,
                        ) as mock_generate:
                            await generate_report(
                                themefinder_output_path="gs://input/output.json",
                                question="Feedback?",
                                output_bucket="output-bucket",
                                project="test-project",
                                location="europe-west2",
                            )

                            # Verify the report generation was called
                            mock_generate.assert_called_once()

                            # Verify config was passed correctly
                            call_config = mock_generate.call_args[1]["config"]
                            assert call_config["output_bucket"] == "output-bucket"
                            assert call_config["title"] == "Summary"