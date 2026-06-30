# Survey Assist Themes

This code uses the [i.Ai ThemeFinder](https://github.com/i-dot-ai/themefinder) python package to determine common themes, sentiment and evidence detail from survey respondent free text feedback.

## Prerequisites

It is assumed you have installed:

- Poetry 2.1.3
- Google Cloud SDK
- PyEnv

To run the code locally you will need to have access to a GCP Project that has Vertex Ai API enabled.

You should be able to authenticate with the project using Application Default Credentials:

```bash
gcloud auth application-default login
```

### Input Data

The input data should be in a CSV format with pipe (|) delimiter.  

Headings are expected to be set as "user"|"feedback_comments", **csv parsing will fail** otherwise.

There is an expectation that the "user" format is either STPxxxxx or STPxxxx-xxxx (where x is a number).  The code will **fail to parse the CSV otherwise**.

There should be two columns, the user column will be converted to an **int** which uniquely identifies a respondent and the second is a ***string** which is the users feedback for analysis.

Example input data:

```bash
user|feedback_comments
STP00821-01|
STP00561-01|No 
STP00017-01|All great
STP12303-01|none
STP01847-01|
STP91885-01|Very easy to navigate
```

### Question to Evaluate

The code defaults to a stock evaluation question of _Do you have any other feedback about this survey?_

This can be changed by setting an environment variable.

### Environment Variables

The following environment variables are supported, it is recommended to use a .env file in the root directory.

```bash
export INPUT_BUCKET=<INPUT_BUCKET_NAME>
export INPUT_FILE=<INPUT_FOLDER>/<INPUT_FILENAME.CSV>
export OUTPUT_BUCKET=<OUTPUT_BUCKET_NAME>
export QUESTION=<Question String>
export GENERATE_THEMES_CSV=<TRUE/FALSE>
```

## Install

Clone the repo and then set local python using pyenv and activate the environment:

```bash
pyenv local 3.12.4
python3 -m venv .venv
source .venv/bin/activate
```

Install the project:

```bash
poetry install
```

## Run the application

Ensure you are set to the relevant GCP project and logged in with ADC (see above).

Check you have the environment variables set appropriately.

Start the application:

```bash
poetry run python -m survey_assist_themes.themefinder_vertexai
```

## Output

Two files will be saved in the destination bucket you specified in the environment variable OUTPUT_BUCKET.

### ThemeFinder Output

The first file is the JSON formatted output from ThemeFinder. And is structured as follows:

```json
{
  "question": "Why did you rate your GP practice experience as poor?",
  "responses": {
    "1": {
      "text": "Garbage.",
      "sentiment": "UNCLEAR",
      "evidence_rich": false,
      "labels": ["D"],
      "processable": true
    },
    "3": {
      "text": "Just bad",
      "sentiment": "UNCLEAR",
      "evidence_rich": false,
      "labels": [],
      "processable": false
    }
  },
  "themes": {
    "A": {
      "topic": "Appointment access is difficult: Patients experience significant challenges...",
      "source_topic_count": 1
    },
    "B": {
      "topic": "Clinical care quality is poor: Clinical consultations are often rushed...",
      "source_topic_count": 2
    }
  }
}
```

### Response ID Mapping File

The second file is a JSON file which records the mapping between Response ID and the Original Source ID.

| Field | Description |
|------|-------------|
| response_id | Sequential integer assigned to each input row, starting from 1. |
| original_id | Original Source ID |
| participant_key | Sequential integer assigned per unique `original_id`. Duplicate original IDs share the same participant_key. |

The file name will match the ThemeFinder Output but will include the suffix: 

```
_id_mapping.json
```

The structure of the file as follows:

```json
[
  {
    "response_id": 1,
    "participant_key": 1,
    "original_id": "STP00001"
  },
  {
    "response_id": 2,
    "participant_key": 1,
    "original_id": "STP00001"
  },
  {
    "response_id": 3,
    "participant_key": 2,
    "original_id": "STP00002"
  },
  {
    "response_id": 4,
    "participant_key": 3,
    "original_id": "STP00003"
  }
]
```

## Theme CSV Outputs

The ThemeFinder JSON specifies the themes found under the themes list. Each theme has a topic_id (e.g A, B, C).
A CSV file per theme can be generated.

To enable the CSV file generation you need to set:
```
GENERATE_THEMES_CSV=TRUE
```

The file name will match the ThemeFinder Output but will include the suffix: 

```
_theme_<TOPIC_ID>.csv
```

### Example theme CSV output

| response_id | original_id | response | theme_description |
|-------------|-------------|----------|-------------------|
| 1 | STP00001 | Impossible to get seen | Inadequate Appointment System |
| 2 | STP00002 | Phones always engaged | Inadequate Appointment System |
| 3 | STP00003 | Doctors were helpful | Inadequate Appointment System |