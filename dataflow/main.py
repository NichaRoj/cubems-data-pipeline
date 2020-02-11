from __future__ import absolute_import
import logging
from io import StringIO
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

def import_csv(input_path, output_path):
  def string_to_dict(col_names, string_input):
    """
    Transform each row of PCollection, which is one string from reading,
    to dictionary which can be read by BigQuery
    """
    import re
    values = re.split(',', re.sub('\r\n', '', re.sub(u'"', '', string_input)))
    row = dict(zip(col_names, values))
    return row

  def string_to_timestamp(input):
    """
    Transform "Date" from CUBEMS (YYYY-MM-DD HH:mm:ss) to
    BigQuery read-able format Timestamp (YYYY-MM-DDTHH:mm:ss)
    """
    import re
    output = input.copy()
    output['timestamp'] = re.sub(' ', 'T', output['timestamp'])
    return output

  def get_names_from_schema(input):
    return list(map(lambda field: field['name'], input['fields']))

  schema_floor1 = {
    'fields': [
      { 'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
      { 'name': 'z1_light', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z1_plug', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z2_ac1', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z2_ac2', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z2_ac3', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z2_ac4', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z2_light', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z2_plug', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z3_light', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z3_plug', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
      { 'name': 'z4_light', 'type': 'NUMERIC', 'mode': 'NULLABLE'}
    ]
  }

  options = PipelineOptions()
  gcp_options = options.view_as(GoogleCloudOptions)
  gcp_options.project = 'cubems-data-pipeline'
  gcp_options.region = 'asia-east1'
  gcp_options.job_name = 'testjob'
  gcp_options.temp_location = 'gs://cubems-raw-data/temp_location'
  options.view_as(StandardOptions).runner = 'DataflowRunner'

  p = beam.Pipeline(options=options)

  (p
   | 'Read CSV' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
   | 'Transform string to dictionary' >> beam.Map(lambda s: string_to_dict(get_names_from_schema(schema_floor1), s))
   | 'Transform string to valid timestamp' >> beam.Map(lambda s: string_to_timestamp(s))
   | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
      output_path, 
      schema=schema_floor1,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
  )
  
  p.run().wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  print('Starting pipeline...')
  import_csv(
    'gs://cubems-raw-data/cham5/floor1/2018Floor1.csv',
    'cubems-data-pipeline:test.floor1'
  )
  print('Finishing pipeline...')