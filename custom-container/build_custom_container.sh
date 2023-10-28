#!/usr/bin/env bash

set -e
set -o pipefail
set -u

echo "#######Building Custom Container Docker image"

gcloud builds submit --tag "$_LOCATION-docker.pkg.dev/$PROJECT_ID/$_REPO_NAME/$_IMAGE_NAME/$_CI_SERVICE_NAME-custom-container:$COMMIT_SHA" .