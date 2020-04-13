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
            type=str,
            required=True
        )
        parser.add_argument(
            '--output',
            type=str,
            required=True
        )


class StringToTableRefFn(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self):
        s = self.output_path.get()
        yield s
        # project_id = s.split(':')[0]
        # dataset_id = s.split(':')[1].split('.')[0]
        # table_id = s.split(':')[1].split('.')[1]

        # yield bigquery.TableReference(
        #   projectId=project_id,
        #   datasetId=dataset_id,
        #   tableId=table_id
        # )


def run():
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'cubems-data-pipeline'
    gcp_options.region = 'asia-east1'
    # gcp_options.job_name = 'import_csv'
    gcp_options.temp_location = 'gs://cubems-data-pipeline.appspot.com/temp'
    gcp_options.staging_location = 'gs://cubems-data-pipeline.appspot.com/staging'
    gcp_options.template_location = 'gs://cubems-data-pipeline.appspot.com/templates/import_csv'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(SetupOptions).setup_file = './setup.py'

    p = beam.Pipeline(options=options)
    runtimeParams = options.view_as(ImportOptions)
    # tableRef = string_to_table_ref(StaticValueProvider(str, runtimeParams.output_path))
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


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
