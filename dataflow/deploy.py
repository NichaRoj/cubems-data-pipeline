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
        default='cubems-data-pipeline.appspot.com'
    )

    args = parser.parse_args()

    command = '''
    python -m {template_name} \
      --runner DataflowRunner \
      --project {project} \
      --requirements_file requirements.txt \
      --staging_location gs://{bucket}/staging \
      --temp_location gs://{bucket}/temp \
      --template_location gs://{bucket}/templates/{template_name} \
      --region asia-east1
  '''.format(project=args.projectID, bucket=args.bucket, template_name=args.template)
    os.system(command)
    os.system('''
    gsutil cp {template_name}_metadata \
      gs://{bucket}/templates/
  '''.format(bucket=args.bucket, template_name=args.template))


if __name__ == '__main__':
    run()
