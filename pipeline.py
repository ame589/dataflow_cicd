import json
from apache_beam.options.pipeline_options import PipelineOptions
from google.api_core.exceptions import NotFound
from google.cloud.pubsub import SchemaServiceClient
from utils.utils import beam, logging, filter_entity, filter_subfolder, \
    extract_message_data, EncodeMessage, CopyFile, ValidateRecord, log_stacktrace, FILE_EXTENSION
from utils.pipeline_options import IngestionPodOptions


def run():
    pipeline_options = PipelineOptions().view_as(IngestionPodOptions)
    try:
        with open(f'conf/{pipeline_options.configuration_json_file}', 'r') as f:
            data = json.load(f)
            entities = data['entities']
            configs = data['job-configs']
            project = configs['project']

        if pipeline_options.configuration_json_file == 'configuration-local.json':
            with_attributes = False
            options = PipelineOptions(
                project=configs['project'],
                streaming=configs['streaming'],
                job_name=configs['job_name'],
                staging_location=configs['staging_location'],
                temp_location=configs['temp_location'],
                region=configs['region'],
                save_main_session=configs['save_main_session'],
                max_num_workers=configs['max_num_workers'],
                setup_file=configs['setup_file'],
                service_account_email=configs['service_account_email'],
                sdk_container_image=configs['sdk_container_image'],
                sdk_location=configs['sdk_location'],
                use_public_ips=configs['use_public_ips'],
                default_sdk_harness_log_level=configs['log_level']
            )
        else:
            with_attributes = True
            options = PipelineOptions(
                save_main_session=configs['save_main_session'],
                sdk_location=configs['sdk_location'],
                streaming=configs['streaming']
            )

        pipeline = beam.Pipeline(options=options)

    except OSError as ose:
        log_stacktrace(ose)
    except KeyError as ke:
        log_stacktrace(ke)

    else:
        schema_client = SchemaServiceClient()

        for entity in entities:
            try:
                entity_name, entity_info = list(entity.items())[0]
                # deduplication_attribute_id = entity_info['deduplicationAttributeId']
                topic = entity_info['topic']
                dlq_topic = entity_info['dlqTopic']
                schema_id = entity_info['schema_id']
                gcs_event_subscription = entity_info['gcsEventSubscription']
                source_bucket = entity_info['source_bucket']
                target_bucket = entity_info['target_bucket']
                target_project = entity_info['target_project']
                input_file_split_char = entity_info['input_file_split_char']
                input_file_split_char_entity_position = entity_info['input_file_split_char_entity_position']

                schema_path = schema_client.schema_path(
                    project=project,
                    schema=schema_id
                )

            except IndexError as ie:
                log_stacktrace(ie)
            except KeyError as ke:
                log_stacktrace(ke)

            else:
                try:
                    """
                    Refs: 
                    1) https://cloud.google.com/blog/products/data-analytics/handling-duplicate-data-in-streaming-pipeline-
                        using-pubsub-dataflow
                    """
                    avro_schema = \
                        schema_client.get_schema(
                            request={"name": schema_path}
                        )

                    # print(f"Got a schema:\n{schema}")
                    output = (pipeline |
                              "Read from PubSub event topic {}".format(entity_name) >>
                              beam.io.ReadFromPubSub(
                                  subscription=f"projects/{project}/subscriptions/{gcs_event_subscription}",
                                  with_attributes=True if with_attributes else False,
                                  id_label="objectId" if with_attributes else None
                              )
                              | "Extract message data from PubSub message {}".format(entity_name) >>
                              beam.Map(lambda message: extract_message_data(message, with_attributes))
                              | "Parse PubSub message event {}".format(entity_name) >>
                              beam.Map(lambda message: json.loads(message))
                              | "Extract file path {}".format(entity_name) >>
                              beam.Map(lambda message: message.get('name'))
                              | "Filter by subfolder {}".format(entity_name) >>
                              beam.Filter(filter_subfolder, entity_name)
                              | "Filter message {}".format(entity_name) >>
                              beam.Filter(filter_entity, entity_name, input_file_split_char, input_file_split_char_entity_position)
                              | "Decompress and copy file {}".format(entity_name) >>
                              beam.ParDo(CopyFile(
                                  source_bucket=source_bucket,
                                  file_path_source=entity_name+FILE_EXTENSION,
                                  destination_bucket=target_bucket,
                                  entity_name=entity_name,
                                  avro_schema=avro_schema,
                                  file_name=entity_name+FILE_EXTENSION,
                                  project_id=configs['project'],
                                  target_project_id=target_project))
                              | "Validate data {}".format(entity_name) >>
                              beam.ParDo(ValidateRecord(avro_schema=avro_schema, entity=entity_name))
                              .with_outputs("dlq", "valid")
                              )

                    (output.valid
                     | "Encode message to binary {}".format(entity_name) >> beam.ParDo(
                                EncodeMessage(avro_schema=avro_schema))
                     | "Publish message to PubSub{}".format(entity_name) >> beam.io.WriteToPubSub(
                                f"projects/{configs['project']}/topics/{topic}")
                     )

                    (output.dlq
                     | "Encode DLQ message to binary {}".format(entity_name) >> beam.Map(
                                lambda message: json.dumps(message).encode('utf-8'))
                     | "Publish DLQ message to PubSub{}".format(entity_name) >> beam.io.WriteToPubSub(
                                f"projects/{configs['project']}/topics/{dlq_topic}")
                     )

                except NotFound as nf:
                    logging.error(f"Schema not found in {schema_path}.")
                    log_stacktrace(nf)
                except Exception as e:
                    logging.error("Something went wrong in the construction of the pipeline.")
                    log_stacktrace(e)

        pipeline.run().wait_until_finish()


if __name__ == "__main__":
    run()