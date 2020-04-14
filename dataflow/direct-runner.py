from __future__ import absolute_import
import logging
from io import StringIO
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
from dotenv import load_dotenv
load_dotenv()


def run(argv=None):
    import re
    import datetime

    def string_to_dict(col_names, string_input):
        """
            Transform each row of PCollection, which is one string from reading,
            to dictionary which can be read by BigQuery
            """
        values = re.split(',', re.sub(
            '\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(zip(col_names, values))
        return row

    def milli_to_datetime(input):

        output = input.copy()
        dt = datetime.datetime.fromtimestamp(int(input['timestamp'])//1000)
        output['timestamp'] = dt.strftime('%Y-%m-%d %H:%M:%S')
        return output

    def get_names_from_schema(input):
        return map(lambda field: field['name'], input['fields'])

    schema = {
        'fields': [
            {'name': 'timestamp', 'type': 'DATETIME', 'mode': 'REQUIRED'},
            {'name': 'value', 'type': 'NUMERIC', 'mode': 'NULLABLE'}
        ]
    }

    # Command Line Options
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        help='Cloud Storage path to input file e.g. gs://cubems-data-pipeline.appspot.com/test.csv'
    )

    parser.add_argument(
        '--output',
        help='Output BigQuery table to write to e.g. set.table1'
    )

    args = parser.parse_args()

    # Direct Runner options
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'cubems-data-pipeline'
    gcp_options.region = 'asia-east1'
    gcp_options.job_name = 'testjob'
    gcp_options.temp_location = 'gs://cubems-data-pipeline.appspot.com/temp_location'
    options.view_as(StandardOptions).runner = 'DirectRunner'

    p = beam.Pipeline(options=options)

    (p
     | 'Read CSV' >> beam.io.ReadFromText(args.input, skip_header_lines=1)
     | 'Transform string to dictionary' >> beam.Map(lambda s: string_to_dict(get_names_from_schema(schema), s))
     | 'Transform string to valid timestamp' >> beam.Map(lambda s: milli_to_datetime(s))
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         args.output,
         schema=schema,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
     )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
