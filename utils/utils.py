import gzip
import io
import json
import logging
import traceback
from abc import ABC
from datetime import datetime
from typing import List, Any

import apache_beam as beam
import avro.schema as schema
import avro.errors
from apache_beam import pvalue
from avro.io import BinaryEncoder, DatumWriter
from google.cloud import storage
from google.cloud import exceptions

""" ------------ CONSTANTS ------------------- """
FILE_EXTENSION = ".dat.gz"
""" ------------------------------- """


class EncodeMessage(beam.DoFn, ABC):

    def __init__(self, avro_schema, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.avro_schema = avro_schema

    def process(self, element, *args, **kwargs):
        bout = io.BytesIO()
        schema_parsed = schema.parse(self.avro_schema.definition)
        writer = DatumWriter(schema_parsed)
        encoder = BinaryEncoder(bout)

        try:
            writer.write(element, encoder)
        except avro.errors.IONotReadyException as ioe:
            logging.error(f"{self.__class__.__qualname__}: {ioe}")

        data = bout.getvalue()
        yield data


class CopyFile(beam.DoFn, ABC):
    # https://beam.apache.org/documentation/transforms/python/elementwise/pardo/

    def __init__(self, source_bucket, file_path_source, destination_bucket, entity_name, avro_schema, file_name,
                 project_id, target_project_id, *unused_args, **unused_kwargs):

        super().__init__(*unused_args, **unused_kwargs)
        self.file_obj = io.BytesIO()
        self.storage_client = None
        self.storage_client_destination = None
        self.source_bucket = source_bucket
        self.file_path_source = file_path_source
        self.destination_bucket = destination_bucket
        self.entity_name = entity_name
        self.blob_copy = None
        # https://cloud.google.com/python/docs/reference/storage/latest/generation_metageneration#using-ifgenerationmatch
        self.destination_generation_match_precondition = None

        self.avro_schema = avro_schema
        self.file_name = file_name
        self.file_path_suffix = None
        self.project_id = project_id
        self.target_project_id = target_project_id

    def setup(self):
        self.storage_client = storage.Client()
        self.storage_client_destination = storage.Client(project=self.target_project_id)
        self.file_obj = io.BytesIO()

    def process(self, file_path, *args, **kwargs) -> None:
        self.file_name = file_path
        self.file_path_suffix = self.file_name.split('/')[-1]
        try:
            self.reading_file_from_bucket(file_path, self.file_path_suffix)

            with gzip.GzipFile(fileobj=self.file_obj, mode='rb') as gz:
                for line in io.TextIOWrapper(gz, encoding='utf-8', errors='replace'):
                    record = self.parse_and_enrich(str_to_parse=line, separator='\x1f')
                    yield record

        except gzip.BadGzipFile:
            logging.error(f"{self.__class__.__qualname__}: File {self.file_name} is not a valid gzipped file.")

    def finish_bundle(self) -> None:
        if self.blob_copy:
            if self.blob_copy.exists():

                logging.info(f"{self.__class__.__qualname__}: "
                             f"Moving input/{self.entity_name}/{self.file_path_suffix} to "
                             f"processed/{self.entity_name}/{self.file_path_suffix}")

                destination_bucket = self.storage_client.bucket(self.destination_bucket)

                try:
                    destination_bucket.copy_blob(self.blob_copy, destination_bucket,
                                                 f'processed/{self.entity_name}/{self.file_path_suffix}',
                                                 if_generation_match=self.destination_generation_match_precondition)

                    destination_bucket.delete_blob(f'input/{self.entity_name}/{self.file_path_suffix}')

                except exceptions.NotFound as nfe:
                    logging.error(f"{self.__class__.__qualname__}: Destination bucket not found ! {nfe}")

                except exceptions.GoogleCloudError as gce:
                    logging.error(
                        f"{self.__class__.__qualname__}: Something went wrong during the deletion of the blob. {gce}")

            else:
                logging.error(f"{self.__class__.__qualname__}: \
                    No such object input/{self.entity_name}/{self.file_path_suffix}")

    def reading_file_from_bucket(self, file_path: str, file_path_suffix: str) -> None:
        """
        Download the file from the bucket into a BytesIO object.

        :param file_path: Path of the file to process
        :param file_path_suffix: Name of the file to process
        :return: None
       Refs:
        1) https://cloud.google.com/storage/docs/samples/storage-copy-file?hl=it#storage_copy_file-python
        2) https://cloud.google.com/storage/docs/samples/storage-stream-file-download?hl=it#storage_stream_file_download
           -python

       """
        source_bucket = self.storage_client.bucket(self.source_bucket)
        source_blob = source_bucket.blob(file_path)
        destination_bucket = self.storage_client_destination.bucket(self.destination_bucket)
        file_path_destination = f'input/{self.entity_name}/{file_path_suffix}'

        try:
            self.blob_copy = source_bucket.copy_blob(
                source_blob, destination_bucket, file_path_destination,
                if_generation_match=self.destination_generation_match_precondition
            )

            self.blob_copy.download_to_file(self.file_obj)
            # Rewind the stream to the beginning.
            self.file_obj.seek(0)

        except exceptions.NotFound as nfe:
            logging.error(f"{self.__class__.__qualname__}: Destination bucket not found ! {nfe}")

        except exceptions.GoogleCloudError as gce:
            logging.error(
                f"{self.__class__.__qualname__}: Something went wrong during the download of the file {self.file_name}!"
                f" {gce}")

    def parse_and_enrich(self, str_to_parse: str, separator: str) -> List[str]:
        """
        Parse the line, given a separator and enrich it by adding custom fields:
            1) load_dt
            2) file_name

        :param str_to_parse: Record of the processed file
        :param separator: Separator used to split the processed file
        :return: list
        """
        values = str_to_parse.split(separator)
        values[-1] = values[-1].strip()

        values.insert(0, str(datetime.now()))
        values.insert(1, self.file_name)

        return values


class ValidateRecord(beam.DoFn, ABC):

    def __init__(self, avro_schema, entity, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.avro_schema = avro_schema
        self.entity = entity

    def process(self, record, *args, **kwargs):
        try:
            logging.debug(f"{self.__class__.__qualname__}: Validating the following record => {record}")
            validated_record = self.validate_schema(input_to_validate=record)
            yield pvalue.TaggedOutput("valid", validated_record)

        except Exception as e:
            logging.error(
                f"{self.__class__.__qualname__}: Error while validating '{self.entity}' record against the schema: {e}")
            yield pvalue.TaggedOutput("dlq", record)

    def validate_schema(self, input_to_validate: list[str]) -> dict[str, Any]:
        """
        Validate each line of the processed file against the Avro Schema and transform it in a dictionary type.

        :param input_to_validate: Record of the processed file
        :return: dict
        """
        const_str, const_bool = "string", "boolean"
        const_int, const_long, const_float, const_double = "int", "long", "float", "double"

        record_dict = {}
        cleaned_schema = self.avro_schema.definition.replace('\n', '')
        avro_schema_json = json.loads(cleaned_schema)

        if len(avro_schema_json['fields']) != len(input_to_validate):
            raise Exception(
                f"{self.__class__.__qualname__}: The record's number of fields must be equal to the PubSub "
                f"Avro Schema's fields number."
                f"The number of Avro schema fields is {len(avro_schema_json['fields'])} "
                f"while file contains {len(input_to_validate)} fields"
            )

        for field in avro_schema_json['fields']:
            field_name = field['name']
            field_type = field['type']
            try:
                # Union type. (Used to represent nullable fields)
                if isinstance(field_type, list):

                    if field_type[0] == const_str or field_type[1] == const_str:
                        value = '' if input_to_validate[0] is None else input_to_validate[0]

                    elif (field_type[0] == const_int or field_type[1] == const_int) \
                            or (field_type[0] == const_long or field_type[1] == const_long):
                        value = None if (input_to_validate[0] is None or input_to_validate[0] == '') else \
                            int(input_to_validate[0])

                    elif field_type[0] == const_float or field_type[1] == const_float \
                            or field_type[0] == const_double or field_type[1] == const_double:
                        value = None if (input_to_validate[0] is None or input_to_validate[0] == '') else \
                            float(input_to_validate[0])

                    elif field_type[0] == const_bool or field_type[1] == const_bool:
                        value = None if input_to_validate[0] is None else input_to_validate[0].lower()

                    else:
                        raise ValueError(f"One of the following types in the union is not"
                                         f" supported: {field_type[0]} or {field_type[1]}")

                    input_to_validate.pop(0)
                    record_dict[field_name] = value

                else:
                    if field_type == const_str:
                        value = input_to_validate[0]

                    elif field_type == const_int or field_type == const_long:
                        value = int(input_to_validate[0])

                    elif field_type == const_float or field_type == const_double:
                        value = float(input_to_validate[0])

                    elif field_type == const_bool:
                        value = input_to_validate[0].lower() == 'true'

                    else:
                        raise ValueError(f"Type not supported: {field_type}")

                    input_to_validate.pop(0)
                    record_dict[field_name] = value

            except Exception as e:
                raise Exception(
                    f"{self.__class__.__qualname__}: Something went wrong during the validation of the record."
                    f" {e}. Sending {input_to_validate} to DLQ")

        return record_dict


def filter_subfolder(elem, entity_name: str) -> bool:
    """
    Check if the element path corresponds to an entity file uploaded in processed bucket subfolder.

    :param elem: Represents the file name
    :param entity_name: Name of the entity presents in the configuration file
    :return: bool
    """
    return elem.startswith(f"{entity_name}/processed/") and elem.endswith(FILE_EXTENSION)


def filter_entity(elem, entity_name: str, input_file_split_char: str, input_file_split_char_entity_position: int) -> bool:
    """
    Check if the element corresponds to an entity handled in the configuration file.

    :param elem: Represents the file name
    :param entity_name: Name of the entity presents in the configuration file
    :param input_file_split_char: String character used in input filename
    :param input_file_split_char_entity_position: Index position
    :return: bool
    """

    try:
        return str(elem.split(input_file_split_char)[input_file_split_char_entity_position]).upper() == entity_name.upper()
    except IndexError:
        logging.error(f"filter_entity: Something went wrong. Are you sure the used splitting character is valid ?"
                      f"Name of the file: '{elem}'. \n"
                      f"Character used to split: '{input_file_split_char}'")
        return False


def extract_message_data(pubsub_message, with_attributes) -> str:
    return pubsub_message.data.decode("utf-8") if with_attributes else pubsub_message.decode("utf-8")


def log_stacktrace(exception):
    tb_lines = traceback.format_exception(exception.__class__, exception, exception.__traceback__)
    tb_text = ''.join(tb_lines)

    logging.error(tb_text)
    raise
