from __future__ import absolute_import
import logging
from io import StringIO
from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.options.value_provider import StaticValueProvider


class ImportOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input',
            type=str
        )


def run():
    import re
    import datetime
    import time

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
            {'name': 'path', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'timestamp', 'type': 'DATETIME', 'mode': 'REQUIRED'},
            {'name': 'value', 'type': 'NUMERIC', 'mode': 'NULLABLE'}
        ]
    }

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    runtime_params = PipelineOptions().view_as(ImportOptions)
    p = beam.Pipeline(options=pipeline_options)
    (p
     | 'Read CSV' >> beam.io.ReadFromText(runtime_params.input, skip_header_lines=1)
     | 'Transform string to dictionary' >> beam.Map(lambda s: string_to_dict(get_names_from_schema(schema), s))
     | 'Transform string to valid timestamp' >> beam.Map(lambda s: milli_to_datetime(s))
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         'cubems-data-pipeline:raw_data.first_imported_data',
         schema=schema,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
     )

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
