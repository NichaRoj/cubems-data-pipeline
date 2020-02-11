import re
from io import StringIO

def string_to_dict(col_names, string_input):
  """
  Transform each row of PCollection, which is one string from reading,
  to dictionary which can be read by BigQuery
  """
  values = re.split(',', re.sub('\r\n', '', re.sub(u'"', '', string_input)))
  row = dict(zip(col_names, values))
  return row

def string_to_timestamp(input):
  """
  Transform "Date" from CUBEMS (YYYY-MM-DD HH:mm:ss) to
  BigQuery read-able format Timestamp (YYYY-MM-DDTHH:mm:ss)
  """
  output = input.copy()
  output['timestamp'] = re.sub(' ', 'T', output['timestamp'])
  return output