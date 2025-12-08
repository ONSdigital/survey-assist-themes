"""Custom exception types for survey-assist-themes."""

from __future__ import annotations


class SurveyAssistThemesError(Exception):
    """Base exception for survey-assist-themes errors."""

    pass


class ConfigurationError(SurveyAssistThemesError):
    """Raised when there is a configuration error."""

    pass


class GCSOperationError(SurveyAssistThemesError):
    """Raised when a GCS operation fails."""

    pass


class DataProcessingError(SurveyAssistThemesError):
    """Raised when data processing fails."""

    pass


class ThemeFinderError(SurveyAssistThemesError):
    """Raised when ThemeFinder processing fails."""

    pass
