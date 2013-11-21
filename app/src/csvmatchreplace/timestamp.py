# Copyright 2013 Google Inc. All Rights Reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""For processing BigQuery timestamp dates."""

import datetime
import logging
import re


import parsedatetime

# Turn off the chatty parsedatetime module's logging
logging.getLogger('parsedatetime').setLevel(logging.ERROR)

# Initialize parsedatetime
pdt_constants = parsedatetime.Constants()
pdt_constants.BirthdayEpoch = 50  # TODO(user) provide a way to set this.
pdt = parsedatetime.Calendar(pdt_constants)

# e.g. 1989-10-02 05:23:48 1958-06-24T12:18:35.5803 1988-08-15T19:06:56.235
TIMESTAMP_RE = re.compile(r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])'
                          r'[ T]([01]\d|2[0-3]):[0-5]\d'
                          r':([0-5]\d|60)(\.\d{3,4})?'  # Leap seconds!
                          r'( [+-][012]\d:[0-5]\d)?$')

# YYYY-MM-DD HH:MM:SS.micro +08:00
OUTPUT_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f %z'

INPUT_TIMESTAMP_FORMATS = (
    '%Y-%m-%d %H:%M:%S.%f %z',
    '%Y/%m/%d %H:%M:%S.%f %z',
    '%m/%d/%Y %H:%M:%S.%f %z',
    '%m/%d/%y %H:%M:%S.%f %z',
    '%y-%m-%d %H:%M:%S.%f %z',
    '%y/%m/%d %H:%M:%S.%f %z',
    '%d%b%y:%H:%M:%S.%f %z',
    '%d%b%Y:%H:%M:%S.%f %z',
    '%d%b%y %H:%M:%S.%f %z',
    '%d%b%Y %H:%M:%S.%f %z',
    # '%d/%m/%y %H:%M:%S.%f %z',  # too ambiguous.
    # '%d/%m/%Y %H:%M:%S.%f %z',  # too ambiguous.
    )

# Converts from strptime % format strings to regexes that match them.
STRPTIME_FORMAT_TO_RE_MAP = {
    'Y': r'\d{4}',  # 4 digit year
    'y': r'\d{2}',  # 2 digit year
    'm': r'(0[1-9]|1[012])',  # 2 digit month
    'b': r'[a-zA-Z]{3}',  # abbreviated month
    'd': r'(0[1-9]|[12]\d|3[01])',  # 2 digit day of month
    'H': r'([01]\d|2[0123])',  # 2 digit hour (24 hr)
    'M': r'[0-5]\d',  # 2 digit minutes
    'S': r'([0-5]\d|60)',  # 2 digit seconds (including leap seconds)
    'f': r'\d{1,6}',  # 1-6 digit microseconds
    'z': r'[+-]\d\d[0-5]\d',  # timezone
    }


def ConvertFmtToRe(fmt, lookup=None):
  """Replace %x with lookup of x in STRPTIME_FORMAT_TO_RE_MAP."""
  if lookup is None:
    lookup = STRPTIME_FORMAT_TO_RE_MAP
  fmt = re.escape(fmt)
  parts = fmt.split(r'\%')
  # TODO(user) support escpaing % inside the string...
  idx = 1
  while idx < len(parts):
    if parts[idx - 1] is None:
      parts[idx - 1] = lookup[parts[idx][0]]
    else:
      parts[idx - 1] += lookup[parts[idx][0]]
    parts[idx] = parts[idx][1:] or None
    idx += 1
  # remove any empty parts
  parts = [part for part in parts if part is not None]
  # now send back a regex string with anything after the first three parts
  # being optional
  return ''.join(parts[0:2]) + '('.join(parts[2:]) + (')?' * (len(parts) - 3))

# Regex strings to match INPUT_TIMESTAMP_FORMATS
INPUT_TIMESTAMP_RES = [ConvertFmtToRe(x) for x in INPUT_TIMESTAMP_FORMATS]


def LooksLikeTimestamp(cell):
  """Does this cell look like a timestamp."""
  if TIMESTAMP_RE.search(cell):
    return True
  for input_re in INPUT_TIMESTAMP_RES:
    if re.search(input_re, cell):
      return True
  return False


def NormalizeTimeStamp(cell):
  """Convert a timestamp like string into a real bigquery timestamp."""
  dt = None
  cell = cell.lower().strip()
  for f in INPUT_TIMESTAMP_FORMATS:
    dt = ParseTimeFormat(f, cell)
    if dt:
      break
  # maybe it's just a number that is a unix timestamp?
  try:
    dt = datetime.datetime.fromtimestamp(int(cell))
  except ValueError:
    pass
  if not dt:
    # logging.debug('trying to parse with parsedatetime')
    pdt_result, pdt_result_type = pdt.parse(cell)
    if pdt_result_type in (1, 2):
      # parsedatetime parsed it as a date or time so it's type struct_time
      dt = datetime.datetime(*pdt_result[:6])
    elif pdt_result_type == 3:
      dt = pdt_result_type

  if not dt:
    raise ValueError('unable to convert %r to timestamp' % cell)

  return dt.strftime(OUTPUT_TIMESTAMP_FORMAT)


def ParseTimeFormat(fmt, cell):
  """Parse a cell using a time format.

  If the format doesn't match at first, keep taking off the last %
  format until you get something that matches. You must have at least
  3 things match.

  Args:
    fmt: a strptime format string
    cell: the cell value to match
  Returns:
    datetime.datetime object or None
  """
  parts = fmt.split('%')
  try:
    # logging.debug('trying to parse with fmt: %r', fmt)
    return datetime.datetime.strptime(cell, fmt)
  except ValueError:
    pass
  # now try to parse on the string as long as at least 3 parts are present
  for p in range(len(parts) - 1, 2, -1):
    try:
      f = '%'.join(parts[:p]) + '%' + parts[p][0]
      # logging.debug('trying to parse with partial fmt: %r', f)
      return datetime.datetime.strptime(cell, f)
    except ValueError:
      pass
  return None
