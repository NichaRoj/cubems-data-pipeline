from __future__ import absolute_import

def get_names_from_schema(input):
  return map(lambda field: field['name'], input['fields'])

schema_default = {
  'fields': [
    { 'name': 'timestamp', 'type': 'DATETIME', 'mode': 'REQUIRED'},
    { 'name': 'value', 'type': 'NUMERIC', 'mode': 'NULLABLE'}
  ]
}