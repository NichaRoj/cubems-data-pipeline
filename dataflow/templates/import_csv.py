from __future__ import absolute_import
from io import StringIO
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions

def import_csv(argv=None):
  from cubems_utils.schema import schema_default, get_names_from_schema
  from cubems_utils.functions import string_to_dict, milli_to_datetime

  class ImportOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument(
        '--input_path',
        required=True
      )
      parser.add_value_provider_argument(
        '--output_path',
        required=True
      )

  options = PipelineOptions()
  # gcp_options = options.view_as(GoogleCloudOptions)
  # gcp_options.project = 'cubems-data-pipeline'
  # gcp_options.region = 'asia-east1'
  # gcp_options.job_name = 'testjob'
  # gcp_options.temp_location = 'gs://cubems-data-pipeline.appspot.com/temp'
  # gcp_options.staging_location = 'gs://cubems-data-pipeline.appspot.com/staging'
  # options.view_as(StandardOptions).runner = 'DataflowRunner'
  options.view_as(SetupOptions).setup_file = './setup.py'

  p = beam.Pipeline(options=options)
  runtimeParams = options.view_as(ImportOptions)

  (p
   | 'Read CSV' >> beam.io.ReadFromText(runtimeParams.input_path, skip_header_lines=1)
   | 'Transform string to dictionary' >> beam.Map(lambda s: string_to_dict(get_names_from_schema(schema_default), s))
   | 'Transform string to valid timestamp' >> beam.Map(lambda s: milli_to_datetime(s))
   | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
      runtimeParams.output_path, 
      schema=schema_default,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
  )
  
  p.run()
