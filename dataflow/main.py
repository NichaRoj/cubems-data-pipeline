import logging
from storage import import_csv

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  print('Starting pipeline...')
  import_csv(
    'gs://cubems-raw-data/8.csv',
    'cubems-data-pipeline:test.temperature'
  )