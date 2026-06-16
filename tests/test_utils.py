from copy import deepcopy
from typing import Any

import pandas as pd

from survey_assist_themes.utils.file_utils import (
    build_theme_table_df,
    rationalise_themefinder_output,
)


def test_build_theme_table_df() -> None:
    result = {
        "mapping": [
            {
                "response_id": 1,
                "response": "Impossible to get seen",
                "labels": ["A"],
            },
            {
                "response_id": 2,
                "response": "Phones always engaged",
                "labels": ["A", "B"],
            },
        ],
        "themes": [
            {
                "topic_id": "A",
                "topic": "Inadequate Appointment System",
            },
            {
                "topic_id": "B",
                "topic": "Consultation Experience",
            },
        ],
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


def test_rationalise_themefinder_output_includes_unprocessables() -> None:
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
        "unprocessables": [
            {
                "response_id": 2,
                "response": "N/A",
            }
        ],
    }

    actual = rationalise_themefinder_output(data)

    assert actual["responses"]["1"]["processable"] is True
    assert actual["responses"]["2"] == {
        "text": "N/A",
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
        "unprocessables": pd.DataFrame([]),
    }

    actual = rationalise_themefinder_output(data)

    assert actual["responses"]["1"]["labels"] == ["A"]
    assert actual["responses"]["1"]["sentiment"] == "DISAGREEMENT"
    assert actual["responses"]["1"]["evidence_rich"] is True
    assert actual["themes"]["A"]["topic"] == "Appointment access"


def test_rationalise_themefinder_output_dataframe_shape_consistency() -> None:
    """Catch unexpected ThemeFinder output shape changes early."""
    data: dict[str, Any] = {
        "question": "How was your appointment?",
        "themes": pd.DataFrame(
            [
                {
                    "topic_id": "A",
                    "topic": "Appointment access",
                    "source_topic_count": 1,
                },
                {
                    "topic_id": "B",
                    "topic": "Consultation quality",
                    "source_topic_count": 1,
                },
            ]
        ),
        "mapping": pd.DataFrame(
            [
                {
                    "response_id": 1,
                    "response": "Impossible to get seen",
                    "labels": ["A"],
                },
                {
                    "response_id": 2,
                    "response": "Doctor was helpful",
                    "labels": ["B"],
                },
            ]
        ),
        "sentiment": pd.DataFrame(
            [
                {
                    "response_id": 1,
                    "response": "Impossible to get seen",
                    "position": "DISAGREEMENT",
                },
                {
                    "response_id": 2,
                    "response": "Doctor was helpful",
                    "position": "AGREEMENT",
                },
            ]
        ),
        "detailed_responses": pd.DataFrame(
            [
                {
                    "response_id": 1,
                    "response": "Impossible to get seen",
                    "evidence_rich": "YES",
                },
                {
                    "response_id": 2,
                    "response": "Doctor was helpful",
                    "evidence_rich": "NO",
                },
            ]
        ),
        "unprocessables": pd.DataFrame(
            [
                {
                    "response_id": 3,
                    "response": "N/A",
                }
            ]
        ),
    }

    actual = rationalise_themefinder_output(data)

    assert len(actual["themes"]) == len(data["themes"])
    assert len(actual["responses"]) == (len(data["mapping"]) + len(data["unprocessables"]))
    assert set(actual["responses"]) == {"1", "2", "3"}
    assert actual["responses"]["3"]["processable"] is False
