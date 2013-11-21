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

"""S3 client library.

The S3 client library only supports basic read-only operations and uses boto.
"""


import cStringIO as StringIO
import urlparse

import boto


class S3(object):
  """S3 utility class.

  This class currently supports read-only operations.
  """
  CHUNK_SIZE_8MB = 1 << 23
  READ_CHUNK_SIZE = CHUNK_SIZE_8MB
  DEFAULT_CONTENT_TYPE = 'binary/octet-stream'

  def __init__(self, config=None, key=None, secret=None):
    """Initialize an S3 class.

    Args:
      config: A pipeline stage config credentials dict.
      key: AWS access key.
      secret: AWS access secret.
    """
    if config:
      key = config.get('accessKey')
      secret = config.get('accessSecret')

    self.service = boto.connect_s3(key, secret)

  @staticmethod
  def UrlToBucketAndName(url):
    """Returns a S3 bucket and object name from a URL.

    Args:
      url: The URL to parse. Must be of form 's3://bucket/object'

    Returns:
      A tuple (bucket, object)

    Raises:
      ValueError: Malformed S3 URL.
    """
    url = urlparse.urlparse(url)
    if not url.scheme or url.scheme != 's3':
      raise ValueError('Unsupported protocol scheme: %s.', url.scheme)
    return (url.netloc, url.path[1:])

  @staticmethod
  def MakeUrl(bucket, obj):
    """Returns a URL to an S3 bucket and object.

    Args:
      bucket: the S3 bucket
      obj: the S3 object name

    Returns:
      A string (s3://bucket/obj)
    """
    return ''.join(['s3://', bucket, '/', obj])

  def ListBucket(self, bucket, prefix=None):
    """List all objects in a bucket filtered optionally by prefix.

    Args:
      bucket: required, specifies the S3 bucket.
      prefix: optional, specifies object [path] prefix to filter against.

    Returns:
      A list of object names.
    """
    # TODO(user): paginate/iterate?
    return [o['name'] for o in self.service.get_bucket(bucket).list(prefix)]

  def StatObject(self, url=None, bucket=None, obj=None):
    """Reads some information about an object in Gcs.

    Args:
      url: Full URL of the object. Use either this or bucket and object
      bucket: Bucket name. Use either this and object or url.
      obj: Object name. Use either this and bucket or url.

    Returns:
      A dict with size
    """
    if url:
      (bucket, obj) = S3.UrlToBucketAndName(url)
    boto_key = self.service.get_bucket(bucket).get_key(obj)
    return {
        'size': boto_key.size
    }

  def ReadObject(self, url=None, bucket=None, obj=None, handler=None,
                 start=None, length=None):
    """Reads the contents of an object from S3.

    Args:
      url: Full URL of the object. Use either this or bucket and object
      bucket: Bucket name. Use either this and object or url.
      obj: Object name. Use either this and bucket or url.
      handler: Optional callback for processing object in chunks. If provided,
        the caller is responsible for consuming the object data from the buffer
        passed into the handler. This method should look like:
        f(buffer, bytes_read, bytes_remaining, content_type)
        where bytes is the length of the data in buffer.
        If missing, the entire object contents will be returned by the method.
      start: Optional starting byte offset
      length: Optional byte length, required with start

    Returns:
      Either the object contents in an IO buffer or None if handler was provided
    """
    if url:
      (bucket, obj) = S3.UrlToBucketAndName(url)
    boto_key = self.service.get_bucket(bucket).get_key(obj)

    headers = None
    if not length:
      length = boto_key.size
    if start:
      headers = {'Range': 'bytes=%d-%d' % (start, start + length - 1)}

    buf = None
    wrapped = None
    callback = None
    max_chunks = 0
    if handler:
      wrapped = _WrappedHandler(handler, length, boto_key.content_type)
      callback = wrapped.Handle
      buf = wrapped.buffer
      max_chunks = 1 + int(float(length) / self.READ_CHUNK_SIZE)

    buf = buf or StringIO.StringIO()
    boto_key.get_contents_to_file(buf,
                                  headers=headers,
                                  cb=callback,
                                  num_cb=max_chunks)

    if wrapped:
      # ensure last chunk gets handled
      rem = wrapped.bytes_read - boto_key.size
      if rem > 0:
        wrapped.Handle(rem, boto_key.size)
      return None
    else:
      buf.seek(0)
      return buf


class _WrappedHandler(object):
  """Wraps a boto reader callback."""

  def __init__(self, handler, total_expected_bytes, content_type):
    self.buffer = StringIO.StringIO()
    self.content_type = content_type
    self.handler = handler
    self.bytes_read = 0
    self.bytes_remaining = total_expected_bytes

  def Handle(self, total_bytes_read, unused_total_s3_bytes):
    """The boto callback."""
    delta = total_bytes_read - self.bytes_read
    if delta:
      self.bytes_read += delta
      self.bytes_remaining -= delta
      self.buffer.seek(0)
      self.handler(self.buffer, delta, self.bytes_remaining, self.content_type)
      self.buffer.truncate(0)
