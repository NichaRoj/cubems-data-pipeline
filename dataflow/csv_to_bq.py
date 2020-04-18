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
    from pytz import timezone

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
        dt = datetime.datetime.fromtimestamp(
            int(input['datetime'])//1000, timezone('Asia/Bangkok'))
        lc = datetime.datetime.fromtimestamp(
            int(input['last_created'])//1000, timezone('Asia/Bangkok'))
        output['datetime'] = dt.strftime('%Y-%m-%d %H:%M:%S')
        output['date'] = dt.strftime('%Y-%m-%d')
        output['time'] = dt.strftime('%H:%M:%S')
        output['last_created'] = lc.strftime('%Y-%m-%d %H:%M:%S+07:00')
        return output

    def get_names_from_schema(input):
        return map(lambda field: field['name'], input['fields'])

    schema = {
        'fields': [
            {'name': 'path', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'building', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'floor', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'zone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'area', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sub_area', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sensor', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'pointid', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'datetime', 'type': 'DATETIME', 'mode': 'REQUIRED'},
            {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'time', 'type': 'TIME', 'mode': 'NULLABLE'},
            {'name': 'value', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'last_created', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
        ]
    }

    def round_value(input):
        output = input.copy()
        rounded_value = round(float(input['value']), 9)
        output['value'] = rounded_value
        return output

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    runtime_params = PipelineOptions().view_as(ImportOptions)
    p = beam.Pipeline(options=pipeline_options)
    (p
     | 'Read CSV' >> beam.io.ReadFromText(runtime_params.input, skip_header_lines=1)
     | 'Transform string to dictionary' >> beam.Map(lambda s: string_to_dict(get_names_from_schema(schema), s))
     | 'Transform string to valid timestamp' >> beam.Map(lambda s: milli_to_datetime(s))
     | 'Round value to 9 decimal palces' >> beam.Map(lambda s: round_value(s))
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         'cubems-data-pipeline:raw_chamchuri5.temp_data',
         schema=schema,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
     )

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
