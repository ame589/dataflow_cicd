#!/usr/bin/env bash

set -e
set -o pipefail
set -u

echo "#######Building Dataflow Flex Template Docker image with all the dependencies installed inside"

gcloud builds submit --tag "$_LOCATION-docker.pkg.dev/$PROJECT_ID/$_REPO_NAME/$_IMAGE_NAME/$_CI_SERVICE_NAME:$COMMIT_SHA" .