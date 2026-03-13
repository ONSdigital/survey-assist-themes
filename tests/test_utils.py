import pandas as pd

from survey_assist_themes.utils.file_utils import build_theme_table_df


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
