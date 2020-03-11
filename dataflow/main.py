from __future__ import absolute_import
import logging
from templates.import_csv import import_csv
# from io import StringIO
# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions

# def import_csv(input_path, output_path):
#   from cubems_utils.schema import schema_floor1, get_names_from_schema
#   from cubems_utils.functions import string_to_dict, string_to_timestamp

#   options = PipelineOptions()
#   gcp_options = options.view_as(GoogleCloudOptions)
#   gcp_options.project = 'cubems-data-pipeline'
#   gcp_options.region = 'asia-east1'
#   gcp_options.job_name = 'testjob'
#   gcp_options.temp_location = 'gs://cubems-raw-data/temp_location'
#   options.view_as(StandardOptions).runner = 'DataflowRunner'
#   options.view_as(SetupOptions).setup_file = './setup.py'

#   p = beam.Pipeline(options=options)

#   (p
#    | 'Read CSV' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
#    | 'Transform string to dictionary' >> beam.Map(lambda s: string_to_dict(get_names_from_schema(schema_floor1), s))
#    | 'Transform string to valid timestamp' >> beam.Map(lambda s: string_to_timestamp(s))
#    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
#       output_path, 
#       schema=schema_floor1,
#       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
#   )
  
#   p.run().wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  import_csv()