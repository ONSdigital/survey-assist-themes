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

The input data should be in a CSV format with pipe (|) delimiter.  Headings can be specified in code, but defaults are:  "user"|"feedback_comments"

There should be two columns, the first is an **int** which uniquely identifies a respondent and the second is a ***string** which is the users feedback for analysis.

### Question to Evaluate

The code defaults to a stock evaluation question of _Do you have any other feedback about this survey?_

This can be changed by setting an environment variable.

### Environment Variables

The following environment variables are supported, it is recommended to use a .env file in the root directory.

```bash
export INPUT_BUCKET=<INPUT_BUCKET_NAME>
export INPUT_FILE=<INPUT_FOLDER>/<INPUT_FILENAME.CSV>
export OUTPUT_BUCKET=<OUTPUT_BUCKET_NAME>
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
poetry run python -m survey_assist_themes.demo_themefinder_vertexai
```

## Output

A JSON structured output will be saved in the destination bucket you specified in the environment variable OUTPUT_BUCKET.

The JSON structure is as follows:

```json
{
  "question": "Do you have any other feedback about this survey?",
  "sentiment": [
    {
      "response_id": 4521,
      "response": "No ",
      "position": "UNCLEAR"
    },
    {
      "response_id": 417,
      "response": "All great",
      "position": "AGREEMENT"
    },
    {
      "response_id": 2303,
      "response": "none",
      "position": "UNCLEAR"
    },
    {
      "response_id": 1885,
      "response": "Very easy to navigate",
      "position": "AGREEMENT"
    },
    ...
  ],
  "themes": [
    {
      "topic": "Survey design is effective: The survey is easy to navigate, complete, and understand, featuring clear, concise, and well-designed questions, and suitable automated follow-up questions.",
      "source_topic_count": 8,
      "topic_id": "A"
    },
    ...
  ],
  "mapping": [
    {
      "response_id": 4521,
      "response": "No ",
      "labels": [
        "G"
      ]
    },
    {
      "response_id": 417,
      "response": "All great",
      "labels": [
        "A"
      ]
    },
    {
      "response_id": 2303,
      "response": "none",
      "labels": [
        "G"
      ]
    },
    {
      "response_id": 1885,
      "response": "Very easy to navigate",
      "labels": [
        "A",
        "B"
      ]
    },
    ...
    "detailed_responses": [
    {
      "response_id": 4521,
      "response": "No ",
      "evidence_rich": "NO"
    },
    {
      "response_id": 417,
      "response": "All great",
      "evidence_rich": "NO"
    },
    ...
    ],
  "unprocessables": [
    {
      "response_id": 5323,
      "response": "I have to think a bit but what can you do. "
    }
  ]
```
