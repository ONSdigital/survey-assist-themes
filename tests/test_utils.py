from copy import deepcopy
from typing import Any

import pandas as pd

from survey_assist_themes.utils.file_utils import (
    build_theme_table_df,
    rationalise_themefinder_output,
)


def test_build_theme_table_df() -> None:
    result = {
        "question": "Why did you rate your GP practice experience as poor?",
        "responses": {
            "1": {
                "text": "Impossible to get seen",
                "sentiment": "NEGATIVE",
                "evidence_rich": True,
                "labels": ["A"],
                "processable": True,
            },
            "2": {
                "text": "Phones always engaged",
                "sentiment": "NEGATIVE",
                "evidence_rich": True,
                "labels": ["A", "B"],
                "processable": True,
            },
        },
        "themes": {
            "A": {
                "topic": "Inadequate Appointment System",
                "source_topic_count": 2,
            },
            "B": {
                "topic": "Consultation Experience",
                "source_topic_count": 1,
            },
        },
    }

    id_mapping_df = pd.DataFrame(
        [
            {"response_id": 1, "participant_key": 1, "original_id": "STP00001"},
            {"response_id": 2, "participant_key": 1, "original_id": "STP00001"},
        ]
    )

    actual = build_theme_table_df(result, id_mapping_df)

    expected = pd.DataFrame(
        [
            {
                "response_id": 1,
                "original_id": "STP00001",
                "response": "Impossible to get seen",
                "theme_description": "Inadequate Appointment System",
                "topic_id": "A",
            },
            {
                "response_id": 2,
                "original_id": "STP00001",
                "response": "Phones always engaged",
                "theme_description": "Inadequate Appointment System",
                "topic_id": "A",
            },
            {
                "response_id": 2,
                "original_id": "STP00001",
                "response": "Phones always engaged",
                "theme_description": "Consultation Experience",
                "topic_id": "B",
            },
        ]
    )

    # Ignore index to avoid assertion failure due to different row order
    actual = actual.sort_values(["response_id", "topic_id"], ignore_index=True)
    expected = expected.sort_values(["response_id", "topic_id"], ignore_index=True)

    pd.testing.assert_frame_equal(actual, expected)


def test_rationalise_themefinder_output() -> None:
    data = {
        "question": "How was your appointment?",
        "themes": [
            {
                "topic_id": "A",
                "topic": "Inadequate Appointment System",
                "source_topic_count": 2,
            }
        ],
        "mapping": [
            {
                "response_id": 1,
                "response": "Impossible to get seen",
                "labels": ["A"],
            }
        ],
        "sentiment": [
            {
                "response_id": 1,
                "response": "Impossible to get seen",
                "position": "DISAGREEMENT",
            }
        ],
        "detailed_responses": [
            {
                "response_id": 1,
                "response": "Impossible to get seen",
                "evidence_rich": "YES",
            }
        ],
        "unprocessables": [],
    }

    actual = rationalise_themefinder_output(data)

    assert actual == {
        "question": "How was your appointment?",
        "themes": {
            "A": {
                "topic": "Inadequate Appointment System",
                "source_topic_count": 2,
            }
        },
        "responses": {
            "1": {
                "text": "Impossible to get seen",
                "sentiment": "DISAGREEMENT",
                "evidence_rich": True,
                "labels": ["A"],
                "processable": True,
            }
        },
    }


def test_rationalise_themefinder_output_marks_unprocessables() -> None:
    data = {
        "question": "How was your appointment?",
        "themes": [],
        "mapping": [],
        "sentiment": [],
        "detailed_responses": [],
        "unprocessables": [
            {
                "response_id": 2,
                "response": "Just bad",
            }
        ],
    }

    actual = rationalise_themefinder_output(data)

    assert actual["responses"]["2"]["processable"] is False
    assert actual["responses"]["2"] == {
        "text": "Just bad",
        "sentiment": None,
        "evidence_rich": False,
        "labels": [],
        "processable": False,
    }


def test_rationalise_themefinder_output_does_not_mutate_input() -> None:
    """Avoid side effects when adapting ThemeFinder output."""
    data: dict[str, Any] = {
        "question": "How was your appointment?",
        "themes": [
            {
                "topic_id": "A",
                "topic": "Appointment access",
                "source_topic_count": 1,
            }
        ],
        "mapping": [
            {
                "response_id": 1,
                "response": "Impossible to get seen",
                "labels": ["A"],
            }
        ],
        "sentiment": [],
        "detailed_responses": [],
        "unprocessables": [],
    }
    original = deepcopy(data)

    rationalise_themefinder_output(data)

    assert data == original


def test_rationalise_themefinder_output_converts_dataframe_values() -> None:
    """Support raw ThemeFinder outputs that still contain Pandas DataFrames."""
    data: dict[str, Any] = {
        "question": "How was your appointment?",
        "themes": pd.DataFrame(
            [
                {
                    "topic_id": "A",
                    "topic": "Appointment access",
                    "source_topic_count": 1,
                }
            ]
        ),
        "mapping": pd.DataFrame(
            [
                {
                    "response_id": 1,
                    "response": "Impossible to get seen",
                    "labels": ["A"],
                }
            ]
        ),
        "sentiment": pd.DataFrame(
            [
                {
                    "response_id": 1,
                    "response": "Impossible to get seen",
                    "position": "DISAGREEMENT",
                }
            ]
        ),
        "detailed_responses": pd.DataFrame(
            [
                {
                    "response_id": 1,
                    "response": "Impossible to get seen",
                    "evidence_rich": "YES",
                }
            ]
        ),
        "unprocessables": pd.DataFrame(
            [
                {
                    "response_id": 2,
                    "response": "Just bad",
                }
            ]
        ),
    }

    actual = rationalise_themefinder_output(data)

    assert actual == {
        "question": "How was your appointment?",
        "themes": {
            "A": {
                "topic": "Appointment access",
                "source_topic_count": 1,
            }
        },
        "responses": {
            "1": {
                "text": "Impossible to get seen",
                "sentiment": "DISAGREEMENT",
                "evidence_rich": True,
                "labels": ["A"],
                "processable": True,
            },
            "2": {
                "text": "Just bad",
                "sentiment": None,
                "evidence_rich": False,
                "labels": [],
                "processable": False,
            },
        },
    }

    assert data["themes"].shape == (1, 3)
    assert data["mapping"].shape == (1, 3)
    assert data["sentiment"].shape == (1, 3)
    assert data["detailed_responses"].shape == (1, 3)
    assert data["unprocessables"].shape == (1, 2)
