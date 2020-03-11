import argparse
import os

### Use for deploy Dataflow templates.
def deploy(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--projectID',
    required=True
  )

  parser.add_argument(
    '--template',
    required=True
  )

  parser.add_argument(
    '--bucket',
    required=True
  )

  args = parser.parse_args()

  command = '''
    python -m {template_name} \
      --runner DataflowRunner \
      --project {project} \
      --requirements_file requirements.txt \
      --staging_location gs://{bucket}/staging \
      --temp_location gs://{bucket}/temp \
      --template_location \
          gs://{bucket}/templates/{template_name}
  '''.format(project=args.projectID, bucket=args.bucket, template_name=args.template)
  os.system(command)
  os.system('''
    gsutil cp {template_name}_metadata \
      gs://{bucket}/templates/
  '''.format(bucket=args.bucket, template_name=args.template))

if __name__ == '__main__':
  deploy()