from apache_beam.options.pipeline_options import PipelineOptions


class IngestionPodOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--configuration_json_file", help="Input Json file path", required=True)