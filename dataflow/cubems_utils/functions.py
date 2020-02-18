from __future__ import absolute_import
from io import StringIO

def string_to_dict(col_names, string_input):
  import re
  """
  Transform each row of PCollection, which is one string from reading,
  to dictionary which can be read by BigQuery
  """
  values = re.split(',', re.sub('\r\n', '', re.sub(u'"', '', string_input)))
  row = dict(zip(col_names, values))
  return row

def string_to_timestamp(input):
  import re
  """
  Transform "Date" from CUBEMS (YYYY-MM-DD HH:mm:ss) to
  BigQuery read-able format Timestamp (YYYY-MM-DDTHH:mm:ss)
  """
  output = input.copy()
  output['timestamp'] = re.sub(' ', 'T', output['timestamp'])
  return output