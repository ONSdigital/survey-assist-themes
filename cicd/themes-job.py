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
        f"Staging bucket/file details gs://{STAGING_BUCKET}/{STAGING_FILE}\n"
        f"Output bucket details g://{OUTPUT_BUCKET}"
    )

    # Download the Cloud Storage object
    staging_bucket = storage_client.bucket(STAGING_BUCKET)
    staging_file = staging_bucket.blob(STAGING_FILE)
    # Split blog into a list of strings.
    contents = staging_file.download_as_string().decode("utf-8")
    print(contents)

    print("Process ...")

    output_bucket = storage_client.bucket(OUTPUT_BUCKET)
    output_file = output_bucket.blob(STAGING_FILE)
    output_file.upload_from_string(contents)

    print(
        f"Cloud run job finished.)"
    )


if __name__ == "__main__":
    process()