#!/usr/bin/python  # pylint: disable=invalid-name
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


"""Flask application to enable command execution in REST API."""

import os
import os.path
import subprocess
import tempfile
import threading
import urllib
import uuid

import flask
import werkzeug


app = flask.Flask(__name__)
MAPREDUCE_RESULT_DIR = '/var/mapreduce'


class FileError(Exception):
  """Exception to indicate file download/upload failure."""


class InvalidMapReduceParameter(Exception):
  """Invalid parameters for Hadoop MapReduce."""


def GetKey():
  """Fetches rcpkey from metadata of the instance.

  Returns:
    RPC key in string.
  """
  try:
    response = urllib.urlopen(
        'http://metadata/computeMetadata/v1beta1/instance/attributes/rpckey')
    return response.read()
  except IOError:
    return ''


RPC_KEY = GetKey()


@app.route('/favicon.ico')
def Favicon():
  """Handler of the favicon request."""
  return flask.send_from_directory(app.root_path, 'favicon.ico',
                                   mimetype='image/vnd.microsoft.icon')


@app.route('/call')
def Call():
  """Handler of RCP call requests."""
  app.logger.info('ACCESS URL: %s', flask.request.path)

  if flask.request.values.get('key', '') != RPC_KEY:
    app.logger.error('Key mismatch.')
    return flask.Response('Access Forbidden', status=403)

  command = flask.request.values.get('command', '')
  app.logger.info('COMMAND: %s', command)
  status = 200

  output = ''
  output += 'command: %s\n' % command
  output += '=========================\n'
  command_process = subprocess.Popen(
      command, bufsize=4096,
      stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
      shell=True)
  while True:
    line = command_process.stdout.readline()
    if not line:
      break
    output += line.replace('\r', '\n')

  command_process.wait()
  if command_process.returncode:
    # Command returned non-zero return code.
    status = 400

  return flask.Response(output, mimetype='text/plain', status=status)


def SaveUploadedFile(upload_file, tmpdir):
  """Saves uploaded file to local directory.

  Args:
    upload_file: File object passed in the request.
    tmpdir: Local directory to save the file in.
  Returns:
    Path to the saved file on local machine.
  """
  filename = werkzeug.secure_filename(upload_file.filename)
  path = os.path.join(tmpdir, filename)
  upload_file.save(path)
  return path


def DownloadFile(url, tmpdir):
  """Downloads file on Google Cloud Storage and saves it to local directory.

  Args:
    url: URL of the file on Google Cloud Storage.
    tmpdir: Local directory to save the file in.
  Returns:
    Path to the saved file on local machine.
  Raises:
    FileError: Failed to download the file from Google Cloud Storage.
  """
  filename = os.path.filename(url)
  path = os.path.join(tmpdir, filename)
  return_code = subprocess.call('gsutil cp %s %s' % (url, path), shell=True)
  if return_code:
    app.logger.warning('Failed to download file: %s', url)
    raise FileError('Failed to download file: %s' % url)
  return path


def SetUpMapperOrReducer(name, mr_type, url, mr_file, tmpdir):
  """Sets up mapper or reducer program file in the temporary directory.

  Args:
    name: Name of the file.
    mr_type: Type of mapper/reducer.  'url', 'file' or 'identity'.
    url: URL of the file on Google Cloud Storage.  Only used for 'url' type.
    mr_file: Mapper or reducer file.  Only for 'file' type.
    tmpdir: Temporary directory on local machine.
  Returns:
    Path to the mapper or reducer on local disk.
  Raises:
    FileError: Failed to download or receive the file.
  """
  if mr_type == 'url':
    app.logger.info('Download %s from: %s', name, url)
    mr_local = DownloadFile(url, tmpdir)
  elif mr_type == 'file':
    if not mr_file:
      raise FileError('%s not uploaded' % name)
    mr_local = SaveUploadedFile(mr_file, tmpdir)
  else:
    # Identity mapper/reducer.
    mr_local = 'cat'

  return mr_local


def PerformMapReduce(input_gcs, output_gcs,
                     mapper_type, mapper_url, mapper_file, mapper_count,
                     reducer_type, reducer_url, reducer_file, reducer_count):
  """Function to perform Hadoop MapReduce task.

  Args:
    input_gcs: Input directory on Google Cloud Storage.
    output_gcs: Output directory on Google Cloud Storage.
    mapper_type: Type of mapper.  'url', 'file', or 'identity'.
    mapper_url: URL to the mapper on Google Cloud Storage.
    mapper_file: File object for mapper.
    mapper_count: Number of mappers.
    reducer_type: Type of reducer.  'url', 'file', or 'identity'.
    reducer_url: URL to the reducer on Google Cloud Storage.
    reducer_file: File object for reducer.
    reducer_count: Number of reducers.
  Yields:
    Log messages from MapReduce task.
  """
  SCRIPT_DIRECTORY = '/tmp/hadoop_package/hadoop_scripts'
  tmpdir = tempfile.mkdtemp(prefix='hadoop-mr-')
  os.chmod(tmpdir, 0755)

  yield '==== MapReduce ====\n'

  try:
    if not input_gcs:
      raise InvalidMapReduceParameter('Input should not be empty')
    if not output_gcs:
      raise InvalidMapReduceParameter('Output should not be empty')

    input_gcs = 'gs://' + input_gcs
    output_gcs = 'gs://' + output_gcs

    # Remove trailing '/'
    if input_gcs[-1] == '/':
      input_gcs = input_gcs[:-1]
    if output_gcs[-1] == '/':
      output_gcs = output_gcs[:-1]

    yield 'Input: %s\n' % input_gcs
    yield 'Output: %s\n' % output_gcs

    yield 'Mapper type: %s\n' % mapper_type
    if mapper_type == 'url':
      yield 'Mapper URL: %s\n' % mapper_url
    yield 'Mapper count: %d\n' % mapper_count
    yield 'Reducer type: %s\n' % reducer_type
    if reducer_type == 'url':
      yield 'Reducer URL: %s\n' % reducer_url
    yield 'Reducer count: %d\n' % reducer_count

    mapper_local = SetUpMapperOrReducer(
        'mapper', mapper_type, mapper_url, mapper_file, tmpdir)
    reducer_local = SetUpMapperOrReducer(
        'reducer', reducer_type, reducer_url, reducer_file, tmpdir)
  except (FileError, InvalidMapReduceParameter) as e:
    yield '\n'
    yield 'ERROR: %s\n' % str(e)
    yield '\n'
    yield 'ERROR: Cannot start MapReduce\n'
    return

  command = [
      'sudo -u hadoop',
      os.path.join(SCRIPT_DIRECTORY, 'mapreduce__at__master.sh'),
      mapper_local, str(mapper_count), reducer_local, str(reducer_count),
      input_gcs, output_gcs]

  command_concatenated = ' '.join(command)
  yield 'MapReduce command: %s\n' % command_concatenated

  mapreduce_process = subprocess.Popen(
      command_concatenated, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
      shell=True)
  while True:
    line = mapreduce_process.stdout.readline()
    if not line:
      break
    yield line

  if mapreduce_process.returncode:
    yield '\n'
    yield ('ERROR: MapReduce process finished with %d\n' %
           mapreduce_process.returncode)
  else:
    yield 'MapReduce finished\n'
    yield '------------------\n'


def MapReduceResultFile(mapreduce_id):
  return os.path.join(MAPREDUCE_RESULT_DIR, mapreduce_id)


def AsyncPerformMapReduce(
    mapreduce_id, input_gcs, output_gcs,
    mapper_type, mapper_url, mapper_file, mapper_count,
    reducer_type, reducer_url, reducer_file, reducer_count):
  """Function to perform Hadoop MapReduce task asynchronously.

  The function is meant to be passed to threading.Thread object constructor
  as target parameter.

  Args:
    mapreduce_id: MapReduce ID of this MapReduce job.
    input_gcs: Input directory on Google Cloud Storage.
    output_gcs: Output directory on Google Cloud Storage.
    mapper_type: Type of mapper.  'url', 'file', or 'identity'.
    mapper_url: URL to the mapper on Google Cloud Storage.
    mapper_file: File object for mapper.
    mapper_count: Number of mappers.
    reducer_type: Type of reducer.  'url', 'file', or 'identity'.
    reducer_url: URL to the reducer on Google Cloud Storage.
    reducer_file: File object for reducer.
    reducer_count: Number of reducers.
  """
  final_result_file = MapReduceResultFile(mapreduce_id)
  intermediate_file = final_result_file + '.intermediate'

  with open(intermediate_file, 'w') as result_writer:
    for message in PerformMapReduce(
        input_gcs, output_gcs,
        mapper_type, mapper_url, mapper_file, mapper_count,
        reducer_type, reducer_url, reducer_file, reducer_count):
      result_writer.write(message)

  os.rename(intermediate_file, final_result_file)


@app.route('/mapreduceasync', methods=['GET', 'POST'])
def MapReduceAsync():
  """Handler of MapReduce asynchronous request."""
  app.logger.info('ACCESS URL: %s', flask.request.path)

  try:
    os.mkdir(MAPREDUCE_RESULT_DIR)
  except OSError:
    # The result directory already exists.  Do nothing.
    pass
  mapreduce_id = str(uuid.uuid4())

  threading.Thread(
      target=AsyncPerformMapReduce,
      args=(mapreduce_id,
            flask.request.values.get('input', ''),
            flask.request.values.get('output', ''),
            flask.request.values.get('mapper_type', 'identity'),
            flask.request.values.get('mapper_url', ''),
            flask.request.files.get('mapper_file', None),
            int(flask.request.values.get('mapper_count', 5)),
            flask.request.values.get('reducer_type', 'identity'),
            flask.request.values.get('reducer_url', ''),
            flask.request.files.get('reducer_file', None),
            int(flask.request.values.get('reducer_count', 1)))).start()

  return flask.Response(mapreduce_id, mimetype='text/plain')


@app.route('/mapreduceresult', methods=['GET', 'POST'])
def MapReduceResult():
  """Handler of asynchronous MapReduce result."""
  mapreduce_id = flask.request.values.get('id', '')
  if not mapreduce_id:
    return flask.Response(
        'MapReduce ID must be specified', mimetype='text/plain', status=400)

  result_file = MapReduceResultFile(mapreduce_id)
  if not os.path.exists(result_file):
    return flask.Response(
        'MapReduce in Progress', mimetype='text/plain', status=404)
  return flask.send_file(result_file)


@app.route('/mapreduce', methods=['GET', 'POST'])
def MapReduce():
  """Handler of MapReduce request."""
  app.logger.info('ACCESS URL: %s', flask.request.path)

  return flask.Response(
      PerformMapReduce(
          flask.request.values.get('input', ''),
          flask.request.values.get('output', ''),
          flask.request.values.get('mapper_type', 'identity'),
          flask.request.values.get('mapper_url', ''),
          flask.request.files.get('mapper_file', None),
          int(flask.request.values.get('mapper_count', 5)),
          flask.request.values.get('reducer_type', 'identity'),
          flask.request.values.get('reducer_url', ''),
          flask.request.files.get('reducer_file', None),
          int(flask.request.values.get('reducer_count', 1))),
      direct_passthrough=False, mimetype='text/plain')


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=80, debug=True)
