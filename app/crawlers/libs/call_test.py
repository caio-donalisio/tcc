import json
from json_to_sqlite import json_to_sqlite


def main():
  # The string representing the json.
  # You will probably want to read this string in from
  # a file rather than hardcoding it.
  f = open('test_json_data.json', 'rb')
  s = f.read()
  f.close()

  s = s.decode('utf-8')
  # Read the string representing json
  # Into a python list of dicts.
  data = json.loads(s)

  json_to_sqlite('json_test_call_test.db', data)


main()
