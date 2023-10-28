#!/usr/bin/env bash

set -e
set -o pipefail
set -u

# Define the Dataflow job name
JOB_NAME="$_CI_SERVICE_NAME"
echo "Finding job name: $JOB_NAME"
# Check if an existing job with the same name is running
RUNNING_JOB=$(gcloud dataflow jobs list \
  --filter "NAME=$JOB_NAME AND STATE=Running" \
  --format "value(JOB_ID)" \
  --region "$_LOCATION" || true)

if [ -n "$RUNNING_JOB" ]; then
  # If there is a running job, drain it
  echo "Draining existing job: $RUNNING_JOB"
  gcloud dataflow jobs drain "$RUNNING_JOB" --region "$_LOCATION"
  echo "Waiting for existing job to drain..."

  # Wait until the job is no longer running
  while true; do
    JOB_STATUS=$(gcloud dataflow jobs list --filter "NAME=$JOB_NAME" --format "get(state)" --region "$_LOCATION" | head -n 1)

    if [ "$JOB_STATUS" == "Drained" ]; then
      echo "Existing job drained successfully."
      break
    fi
    sleep 60
  done
fi

echo "#######Run the Dataflow Flex Template pipeline"

gcloud dataflow flex-template run "$JOB_NAME" \
  --template-file-gcs-location "$_METADATA_TEMPLATE_FILE_PATH/$JOB_NAME.json" \
  --project="$PROJECT_ID" \
  --region="$_LOCATION" \
  --temp-location="$_TEMP_LOCATION" \
  --staging-location="$_STAGING_LOCATION" \
  --service-account-email="$_SERVICE_ACCOUNT" \
  --subnetwork="$_SUBNETWORK" \
  --network="$_NETWORK" \
  --max-workers="$_MAX_WORKERS" \
  --disable-public-ips \
  --enable-streaming-engine \
  --parameters configuration_json_file="$_CONFIGURATION_JSON_FILE" \
  --parameters sdk_container_image="$_LOCATION-docker.pkg.dev/$PROJECT_ID/$_REPO_NAME/$_IMAGE_NAME/$_CI_SERVICE_NAME-custom-container:$COMMIT_SHA"