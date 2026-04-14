#!/bin/bash

# This script is only intended for use in the Sandbox environment.
#
# Please set the environment variable CICD_PROJECT_ID i.e. export CICD_PROJECT_ID=

if [[ ! -v CICD_PROJECT_ID ]]; then 
   echo "Please set the environment variable CICD_PROJECT_ID i.e. export CICD_PROJECT_ID="
   exit 1
fi

ENV_NAME=sandbox # Sandbox use only

GIT_SHA=$(git rev-parse --short HEAD)
sandbox_config=$(gcloud parametermanager parameters versions describe $ENV_NAME --parameter=infra-test-config --location=global --project $CICD_PROJECT_ID --format=json | python3 -c "import sys, json; print(json.load(sys.stdin)['payload']['data'])" | base64 --decode)
PROJECT_ID=$(echo $sandbox_config | python3 -c "import sys, json; print(json.load(sys.stdin)['project-id'])")
CICD_SA=$(echo $sandbox_config | python3 -c "import sys, json; print(json.load(sys.stdin)['cicd-sa-email'])")
REGION=$(echo $sandbox_config | python3 -c "import sys, json; print(json.load(sys.stdin)['region'])")
CB_BUCKET=gs://${PROJECT_ID}_cloudbuild/soc-api

gcloud beta builds submit . --config=cicd/cloudbuild_dev_and_sandbox.yaml \
	--project $CICD_PROJECT_ID \
	--service-account projects/$CICD_PROJECT_ID/serviceAccounts/$CICD_SA \
	--gcs-source-staging-dir $CB_BUCKET \
	--substitutions=SHORT_SHA=$GIT_SHA,_ENV_NAME=$ENV_NAME \
	--region $REGION
