#! /bin/bash

gcloud beta builds submit . --config=cloudbuild.yaml \
	--project ons-cicd-surveyassist \
	--service-account projects/ons-cicd-surveyassist/serviceAccounts/ons-cicd-surveyassist@ons-cicd-surveyassist.iam.gserviceaccount.com \
	--gcs-source-staging-dir gs://ons-cicd-surveyassist_cloudbuild/themes-job \
	--region europe-west2