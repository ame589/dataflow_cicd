#!/usr/bin/env bash

set -e
set -o pipefail
set -u

echo "#######Creating spec file with flex-template-build"

gcloud dataflow flex-template build "$_METADATA_TEMPLATE_FILE_PATH/$_CI_SERVICE_NAME.json" \
  --image "$_LOCATION-docker.pkg.dev/$PROJECT_ID/$_REPO_NAME/$_IMAGE_NAME/$_CI_SERVICE_NAME:$COMMIT_SHA" \
  --sdk-language "$_SDK_LANGUAGE" \
  --metadata-file "$_METADATA_FILE"
