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

"""Module to initiate CSV conversion by Hadoop MapReduce."""

import json
import logging
import os.path
import re
import time
import urllib2

import jinja2

from src.clients import gcs
from src.hadoop import datastore


jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))


class HadoopError(Exception):
  """Exception raised upon error on Hadoop MapReduce."""


class HadoopCsvTransformer(object):
  """Class to transform CSV with Hadoop MapReduce."""

  def __init__(self, config):
    """Constructor.

    Args:
      config: Transform configuration in Python dictionary.
    Raises:
      HadoopError: When no Hadoop cluster is available.
    """
    self.config = config
    self.boundary = 'Hadoop_MapReduce_Request_Http_Boundary'
    self.cloud_storage_client = gcs.Gcs()

    cluster_query = datastore.ClusterInfo.query()
    if not cluster_query.count():
      raise HadoopError('No Hadoop cluster available')
    # For now, always use the first Hadoop cluster.
    # TODO(user): Make configurable which Hadoop cluster to use.
    hadoop_cluster = cluster_query.fetch(1)[0]
    logging.info('Starting Hadoop MapReduce on cluster "%s"',
                 hadoop_cluster.name)
    self.master_ip = hadoop_cluster.GetMasterIpAddress()

  def StartTransform(self):
    """Starts CSV transformation on Hadoop cluster."""
    self._LoadMapper()

    gcs_dir = self.config['hadoopTmpDir']
    hadoop_input_filename = '%s/inputs/input.csv' % gcs_dir

    logging.info('Starting Hadoop transform from %s to %s',
                 self.config['sources'][0], self.config['sinks'][0])
    logging.debug('Hadoop input file: %s', hadoop_input_filename)

    output_file = self.cloud_storage_client.OpenObject(
        self.config['sinks'][0], mode='w')
    # Copy input file to Cloud Storage MapReduce input directory.
    input_file = self.cloud_storage_client.OpenObject(self.config['sources'][0])
    hadoop_input = self.cloud_storage_client.OpenObject(
        hadoop_input_filename, mode='w')

    line_count = 0
    for line in input_file:
      if line_count < self.config['skipLeadingRows']:
        output_file.write(line)
      else:
        hadoop_input.write(line)
      line_count += 1

    hadoop_input.close()
    input_file.close()

    mapreduce_id = self._StartHadoopMapReduce(gcs_dir)

    self._WaitForMapReduce(mapreduce_id)

    bucket, hadoop_dir = gcs.Gcs.UrlToBucketAndName(gcs_dir)

    tab_strip_pattern = re.compile('\t\r?\n')

    for hadoop_result in self.cloud_storage_client.ListBucket(
        '/%s' % bucket, prefix='%s/outputs/part-' % hadoop_dir):
      logging.debug('Hadoop result file: %s', hadoop_result)
      hadoop_output = self.cloud_storage_client.OpenObject(hadoop_result)
      for line in hadoop_output:
        # Since Hadoop MapReduce output is key-value pair separated by a tab,
        # and Hadoop transformer always outputs a line as a key,
        # trim the trailing tab before EOL.
        output_file.write(tab_strip_pattern.sub('\n', line))

    output_file.close()

  def _LoadMapper(self):
    """Loads mapper script and fills in transform configuration."""
    template = jinja_environment.get_template('csv_transformer_mapper_tmpl.py')
    self.mapper = template.render({'transform_config': json.dumps(self.config)})

  def _AddParameter(self, body, name, value):
    """Adds a parameter value in the body of multipart HTTP.

    Args:
      body: HTTP body to append the parameter to.
      name: Name of the parameter.
      value: Value of the parameter.
    Returns:
      New HTTP body.
    """
    body += '--%s\r\n' % self.boundary
    body += 'Content-Disposition: form-data; name="%s"\r\n' % name
    body += '\r\n'
    body += '%s\r\n' % value
    return body

  def _AttachFile(self, body, name, filename, content):
    """Adds a file to multipart HTTP body.

    Args:
      body: HTTP body to append the parameter to.
      name: Name of the parameter.
      filename: Filename of the attached file.
      content: Content of the file to attach.
    Returns:
      New HTTP body.
    """
    body += '--%s\r\n' % self.boundary
    body += 'Content-Disposition: form-data; name="%s"; filename="%s"\r\n' % (
        name, filename)
    body += 'Content-Type: application/octet-stream\r\n'
    body += '\r\n'
    body += '%s\r\n' % content
    return body

  def _StartHadoopMapReduce(self, gcs_dir):
    """Sends HTTP request to initiate Hadoop MapReduce for CSV transformation.

    Args:
      gcs_dir: Directory on Cloud Storage where inputs and outputs of MapReduce
          are stored.
    Returns:
      MapReduce ID generated by Hadoop master.
    Raises:
      HadoopError: When no Hadoop cluster is available.
    """
    http_request = urllib2.Request('http://%s/mapreduceasync' % self.master_ip)
    # To attach a file, Content-Type must be multipart/form-data.
    # Python libraries (urllib, urllib2, httplib) don't support
    # multipart/form-data by themselves.  Build multipart request manually.
    http_request.add_header('Content-Type',
                            'multipart/form-data; boundary=%s' % self.boundary)

    # gcs_dir starts with 'gs://'.  Remove it, since MapReduce script
    # expects path without prefix ('gs://')
    bare_gcs_dir = gcs_dir
    if gcs_dir.startswith('gs://'):
      bare_gcs_dir = gcs_dir[5:]

    mapreduce_params = {
        'mapper_type': 'file',
        'mapper_count': 5,
        'reducer_type': 'identity',
        'reducer_count': 0,
        'input': bare_gcs_dir + '/inputs/',
        'output': bare_gcs_dir + '/outputs/',
    }

    body = ''
    for key, value in mapreduce_params.items():
      body = self._AddParameter(body, key, value)

    # Attach mapper and reducer as files.
    body = self._AttachFile(
        body, 'mapper_file', 'datapipeline_transform.py', self.mapper)

    body += '--%s--\r\n' % self.boundary

    logging.debug(body)

    response = urllib2.urlopen(http_request, body, timeout=60)
    mapreduce_id = response.read()
    logging.info('MapReduce ID: %s', mapreduce_id)
    return mapreduce_id

  def _WaitForMapReduce(self, mapreduce_id):
    """Waits for the asynchronous MapReduce task to finish on Hadoop cluster.

    Args:
      mapreduce_id: Asynchronous MapReduce ID assigned by Hadoop master.
    Raises:
      HadoopError: When MapReduce on Hadoop doesn't finish normally.
    """
    for _ in xrange(60):
      time.sleep(30)
      try:
        response = urllib2.urlopen(
            'http://%s/mapreduceresult?id=%s' % (self.master_ip, mapreduce_id),
            timeout=60)
        logging.debug(response.read())
        return
      except urllib2.URLError as e:
        if e.code == 404:
          # /mapreduceresult returns 404 when MapReduce result is not yet ready.
          logging.info('Hadoop MapReduce in progress.')
        else:
          logging.error('%d: %s', e.code, e.read())
          raise HadoopError('Hadoop MapReduce server error')

    raise HadoopError('Hadoop MapReduce time out')
