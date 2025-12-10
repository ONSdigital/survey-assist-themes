"""Themes Cloud Run Job."""

import hashlib
import math
import os
import time

import google.auth
from google.cloud import storage

storage_client = storage.Client()

_, PROJECT_ID = google.auth.default()

STAGING_BUCKET  = os.environ.get("STAGING_BUCKET", "")
STAGING_FILE    = os.environ.get("STAGING_FILE", "")
OUTPUT_BUCKET   = os.environ.get("OUTPUT_BUCKET", "")

def process():
    """Run Cloud Run Job."""
        # Output useful information about the processing starting.
    
    print(
        f"Staging bucket/file details gs://{STAGING_BUCKET}/{STAGING_FILE}"
        f"Output bucket details g://{OUTPUT_BUCKET}"
    )

    # Download the Cloud Storage object
    bucket = storage_client.bucket(STAGING_BUCKET)
    blob = bucket.blob(STAGING_FILE)

    # Split blog into a list of strings.
    contents = blob.download_as_string().decode("utf-8")
    print(contents)

    print(
        f"Cloud run job finished.)"
    )


if __name__ == "__main__":
    process()