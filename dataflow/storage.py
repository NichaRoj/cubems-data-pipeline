from __future__ import absolute_import
import argparse
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def csv_to_bq(string_input):
  values = re.split(",", re.sub('\r\n', '', re.sub(u'"', '', string_input)))
  row = dict(zip(('date', 'temperature'), values))
  return row

def import_csv(input_path, output_path, argv=None):
  parser = argparse.ArgumentParser()

  # input
  parser.add_argument(
    '--input',
    dest='input',
    required=False,
    help='Target file to read for input.',
    default=input_path
  )

  # output
  parser.add_argument(
    '--output',
    dest='output',
    required=False,
    help='Target BigQuery table for output.',
    default=output_path
  )

  known_args, pipeline_args = parser.parse_known_args(argv)
  p = beam.Pipeline(options=PipelineOptions(pipeline_args))

  (p
   | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
   | 'Transform CSV to BigQuery table' >> beam.Map(lambda s: csv_to_bq(s))
   | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
      known_args.output, 
      schema='date:TIMESTAMP,temperature:NUMERIC',
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
  )
  p.run().wait_until_finish()

  # BigQuery: for 1st phrase lets import all data and do basic transformation to replicate CUBEMS website 
  # --> do I need surrogate key or id? cant I just name the table to the sensor id?
  # but sensor id should be stored as well so shouldnt it be beneficial to make them keys as well