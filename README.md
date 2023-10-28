# Dataflow Job: c360-ingestion-pod

This README provides an overview of the `dataflow-cicd` dataflow job, explaining its components, functionality, and configuration.

## Overview

The `dataflow-cicd` dataflow job is designed to ingest data from Google Cloud Storage (GCS), validate the records against Avro schemas retrieved from Google Cloud Pub/Sub topics, and publish valid and invalid records to corresponding Pub/Sub topics. The job is built using Apache Beam and runs on Google Cloud Dataflow.

## Components

The dataflow job consists of the following major components:

1. **`copy_file` DoFn**: This component copies a GCS object from a source bucket to a destination bucket, decompresses the object, and yields its contents line by line.

2. **`validate_record` DoFn**: This component validates records against an Avro schema. It checks the number of fields and data types in the record. Valid records are tagged as "valid", while invalid records are tagged as "dlq" (dead-letter queue).

3. **`encode_message` DoFn**: This component encodes valid records into binary Avro format using the corresponding schema.

4. **Main Pipeline**: The main pipeline reads messages from a Google Cloud Pub/Sub subscription, parses them as JSON, extracts the file path, filters messages based on the entity name, processes the files using the `copy_file` component, validates the records using the `validate_record` component, encodes valid records using the `encode_message` component, and publishes valid and invalid records to appropriate Pub/Sub topics.

## Configuration

The job is configured using the `configuration-<env>.json` file, which specifies various settings and entity details.
For local development pass --configuration_json_file=configuration-local.json as script parameter.

### Dockerfile

The Dockerfile, inside custom-container folder, sets up the environment for the Dataflow job by using the `apache/beam_python3.11_sdk:2.49.0` base image. It installs required packages from the specified `requirements.txt` file and sets the environment variable `PIP_INDEX_URL` for a custom package repository. The entry point is set to run the `pipeline.py` script.

### Configuration JSON

The `configuration.json` file contains job configuration details, including:

- **Project**: The Google Cloud project where the job runs.
- **Runner**: The Dataflow runner to execute the job.
- **Streaming**: Whether the job processes data in streaming mode.
- **Job Name**: The name of the Dataflow job.
- **Staging Location**: GCS location for staging temporary files.
- **Temp Location**: GCS location for temporary files.
- **Region**: The region where the job runs.
- **Network** and **Subnetwork**: Network and subnetwork settings.
- **Save Main Session**: Whether to save the main Python session.
- **Requirements File**: Path to the requirements file.
- **Max Number of Workers**: The maximum number of workers for the job.
- **Enable Hot Key Logging**: Whether to enable hot key logging.
- **Update**: Whether to update the job if it already exists.
- **Service Account Email**: The email of the service account used by the job.
- **SDK Container Image**: The custom container image for the SDK harness.
- **SDK Location**: The location of the SDK: "container" indicates using the custom container image.


### Entities

The `entities` section of the `configuration-<env>.json` file lists the specific entity configurations that the dataflow job will process. Each entity configuration consists of various fields, including the entity name, deduplication attribute ID, topics, schema ID, GCS event subscription, source bucket, and target bucket.

## Usage

1. Update the `configuration-<env>.json` file with specific configuration details.

2. Build the Docker container using the provided Dockerfile and command:

   ```shell
   gcloud builds submit --tag your-docker-image-tag .
   ```

3. Run the Dataflow job using the custom container image with the command:

   ```shell
   python pipeline.py
   ```

Make sure you have the necessary credentials and access permissions to run the job on Google Cloud Dataflow.

## Conclusion

The `dataflow-cicd` dataflow job efficiently ingests, validates, and processes data from GCS, using Google Cloud Pub/Sub for schema retrieval and record publishing. Its modular structure allows for easy customization and extension to handle various data sources and validation requirements, while the provided Dockerfile facilitates containerization for easy deployment and execution.
