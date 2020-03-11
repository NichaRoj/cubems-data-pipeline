from __future__ import absolute_import
from io import StringIO
import datetime

def string_to_dict(col_names, string_input):
  import re
  """
  Transform each row of PCollection, which is one string from reading,
  to dictionary which can be read by BigQuery
  """
  values = re.split(',', re.sub('\r\n', '', re.sub(u'"', '', string_input)))
  row = dict(zip(col_names, values))
  return row

def milli_to_datetime(input):
  import re
  
  output = input.copy()
  dt = datetime.datetime.fromtimestamp(int(input['timestamp'])//1000)
  output['timestamp'] = dt.strftime('%Y-%m-%d %H:%M:%S')
  return output