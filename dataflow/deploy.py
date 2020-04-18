import argparse
import os
from dotenv import load_dotenv
load_dotenv()

# Use for deploying Dataflow templates.


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--projectID',
        default='cubems-data-pipeline'
    )

    parser.add_argument(
        '--template',
        required=True
    )

    parser.add_argument(
        '--bucket',
        default='cubems-data-pipeline_asia-southeast1'
    )

    parser.add_argument('--requirements_file')

    known_args, pipeline_args = parser.parse_known_args(argv)

    requirements = '--requirements_file {requirements_file}'.format(
        requirements_file=known_args.requirements_file) if known_args.requirements_file else ''

    command = '''
    python3 -m {template_name} \
      --runner DataflowRunner \
      --project {project} \
      {requirements} \
      --staging_location gs://{bucket}/staging \
      --temp_location gs://{bucket}/temp \
      --template_location gs://{bucket}/templates/{template_name} \
      --region asia-east1 \
      --no_use_public_ips \
      --subnetwork https://www.googleapis.com/compute/v1/projects/cubems-data-pipeline/regions/asia-east1/subnetworks/default \
      --service_account_email=dataflow-service@cubems-data-pipeline.iam.gserviceaccount.com \
      {pipeline_args}
  '''.format(project=known_args.projectID, bucket=known_args.bucket, template_name=known_args.template, pipeline_args=' '.join(pipeline_args), requirements=requirements)
    os.system(command)

    os.system('''
    gsutil cp {template_name}_metadata \
      gs://{bucket}/templates/
  '''.format(bucket=known_args.bucket, template_name=known_args.template))


if __name__ == '__main__':
    run()
