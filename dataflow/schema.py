def get_names_from_schema(input):
  return input['fields'].map(lambda field: field['name'], input)

schema_floor1 = {
  'fields': [
    { 'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    { 'name': 'z1_light', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z1_plug', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z2_ac1', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z2_ac2', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z2_ac3', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z2_ac4', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z2_light', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z2_plug', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z3_light', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z3_plug', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    { 'name': 'z4_light', 'type': 'NUMERIC', 'mode': 'NULLABLE'}
  ]
}