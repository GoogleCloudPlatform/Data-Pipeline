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

"""Google Cloud Storage client library."""

import contextlib
import fnmatch
import logging
import math
import urlparse
import uuid

from apiclient.errors import HttpError

import cloudstorage
from src import auth


class Gcs(object):
  """Google Cloud Storage utility class.

  This class makes use of both the AppEngine storage API as well as the
  library that uses the JSON API. Some operations are only available
  or optimized (e.g. bucket-to-bucket object copying) using the JSON API. Others
  are used depending on the credentials provided.
  """
  SERVICE_NAME = 'storage'
  SERVICE_VERSION = 'v1beta2'
  AUTH_SCOPE = 'https://www.googleapis.com/auth/devstorage.read_write'
  MAX_COMPOSABLE_OBJECTS = 32  # max objects we can compose in one call
  MAX_TOTAL_COMPOSABLE_OBJECTS = 1024  # total composed count limit
  CHUNK_SIZE_8MB = 1 << 23
  READ_CHUNK_SIZE = CHUNK_SIZE_8MB
  DEFAULT_CONTENT_TYPE = 'binary/octet-stream'

  def __init__(self):
    self._service = None

  def _AcquireService(self):
    """Acquires the storage service."""
    if not self._service:
      self._service = auth.Service.FromServiceAccount(self.SERVICE_NAME,
                                                      self.SERVICE_VERSION,
                                                      self.AUTH_SCOPE)
    return self._service

  @staticmethod
  def UrlCreator(bucket, prefix=''):
    """Provides a function for creating unique URLs for new GCS objects.

    Args:
      bucket: Required GCS bucket.
      prefix: Optional path/name prefix. Be sure to append '/' if path-only.

    Returns:
      A well-formed URL pointing to a (new) unique GCS object.
    """
    prefix = prefix or ''
    return lambda: ''.join(['gs://', bucket, '/', prefix, str(uuid.uuid4())])

  @staticmethod
  def UrlToBucketAndName(url):
    """Returns a GCS bucket and object name from a URL.

    Args:
      url: The URL to parse. Must be of form 'gs://bucket/object'

    Returns:
      A tuple (bucket, object)

    Raises:
      ValueError: Malformed GCS URL.
    """
    url = urlparse.urlparse(url)
    if not url.scheme or url.scheme != 'gs':
      raise ValueError('Unsupported protocol scheme: %s.', url.scheme)
    return (url.netloc, url.path[1:])

  @staticmethod
  def UrlToBucketAndNamePath(url):
    """Returns a path to a GCS bucket and object name from a URL.

    Args:
      url: The URL to parse. Must be of form 'gs://bucket/object'

    Returns:
      A string (/bucket/object)

    Raises:
      ValueError: Malformed GCS URL.
    """
    return Gcs.MakeBucketAndNamePath(*Gcs.UrlToBucketAndName(url))

  @staticmethod
  def MakeBucketAndNamePath(bucket, obj):
    """Returns a path to a GCS bucket and object name for use by GCS client API.

    Args:
      bucket: the GCS bucket
      obj: the GCS object name

    Returns:
      A string (/bucket/obj)
    """
    return ''.join(['/', bucket, '/', obj])

  @staticmethod
  def MakeUrl(bucket, obj):
    """Returns a URL to a GCS bucket and object.

    Args:
      bucket: the GCS bucket
      obj: the GCS object name

    Returns:
      A string (gs://bucket/obj)
    """
    return ''.join(['gs://', bucket, '/', obj])

  def InsertBucket(self, project_id, bucket, location='US'):
    """Create a new GCS bucket.

    Args:
      project_id: Google Cloud Platform project to own the bucket.
      bucket: Name of the new bucket.
      location: Optional region location for the bucket.

    Returns:
      A bucket resource.
    """
    req = self._AcquireService().buckets().insert(
        project=project_id, body={'name': bucket, 'location': location})
    return req.execute()

  def ListBucket(self, bucket, prefix=None, glob=None):
    """List all objects in a bucket filtered optionally by prefix and regex.

    Args:
      bucket: required, specifies the GCS bucket.
      prefix: optional, specifies object [path] prefix to filter against.
      glob: optional, glob filter for the object list.

    Returns:
      A list of object names.
    """
    # TODO(user): paginate and return an iterator instead of a flat list.
    items = cloudstorage.listbucket(bucket, prefix=prefix)

    return ['gs:/' + i.filename for i in items
            if not glob or fnmatch.fnmatch(i.filename, glob)]

  def DeleteBucket(self, bucket):
    """Removes an existing GCS bucket."""
    return self._AcquireService().buckets().delete(bucket=bucket).execute()

  def StatObject(self, url=None, bucket=None, obj=None):
    """Reads some information about an object in Gcs.

    Args:
      url: Full URL of the object. Use either this or bucket and object
      bucket: Bucket name. Use either this and object or url.
      obj: Object name. Use either this and bucket or url.

    Returns:
      A dict with size, md5, contentType and metadata keys.
    """
    stat = None
    if url:
      stat = cloudstorage.stat(Gcs.UrlToBucketAndNamePath(url))
    else:
      stat = cloudstorage.stat(Gcs.MakeBucketAndNamePath(bucket, obj))
    return {
        'size': stat.st_size,
        'md5Hash': stat.etag,
        'contentType': stat.content_type,
        'metadata': stat.metadata
    }

  def CopyObject(self, src, dest):
    """Copies an object from one bucket to another.

    Args:
      src: must specify both the bucket and object (e.g. gs://bucketA/obj1).
      dest: must specify the bucket and may specify object
        (e.g. gs://bucketB or gs://bucketB/obj2).

    Returns:
      The destination object resource.

    Raises:
      ValueError: if src or dest not provided or are malformed.
    """
    if not src:
      raise ValueError('No source specified.')
    elif not dest:
      raise ValueError('No destination specified.')
    elif src is dest:
      return dest

    (src_bucket, src_obj) = self.UrlToBucketAndName(src)
    (dest_bucket, dest_obj) = self.UrlToBucketAndName(dest)
    if not src_bucket or not src_obj or not dest_bucket:
      raise ValueError('Input URL is malformed.')

    if not dest_obj:
      dest_obj = src_obj

    req = self._AcquireService().objects().copy(sourceBucket=src_bucket,
                                                sourceObject=src_obj,
                                                destinationBucket=dest_bucket,
                                                destinationObject=dest_obj,
                                                body={})
    return req.execute()

  def OpenObject(self, url=None, bucket=None, obj=None, mode='r'):
    """Opens an object for reading from Gcs.

    Args:
      url: Full URL of the object. Use either this or bucket and object
      bucket: Bucket name. Use either this and object or url.
      obj: Object name. Use either this and bucket or url.
      mode: Open mode of the Object.  'r' (default) or 'w'.

    Returns:
      A file-like object with which the object data can be read (should be
      closed after use).
    """
    target = ''
    if url:
      target = Gcs.UrlToBucketAndNamePath(url)
    else:
      target = Gcs.MakeBucketAndNamePath(bucket, obj)
    return cloudstorage.open(target, mode)

  def InsertObject(self, stream, url=None, bucket=None, obj=None):
    """Writes a stream as the contents of an object in Gcs.

    Args:
      stream: Any io.RawIOBase (will NOT be closed by this function).
      url: Full URL of the object. Use either this or bucket and object.
      bucket: Bucket name. Use either this and object or url.
      obj: Object name. Use either this and bucket or url.
    """
    path = ''
    if url:
      path = Gcs.UrlToBucketAndNamePath(url)
    else:
      path = Gcs.MakeBucketAndNamePath(bucket, obj)
    with contextlib.closing(cloudstorage.open(path, 'w')) as obj:
      while True:
        buf = stream.read(self.READ_CHUNK_SIZE)
        if buf and len(buf):
          obj.write(buf)
        else:
          return

  def DeleteObject(self, bucket, obj, ignore_missing_files=True):
    """Removes an existing GCS object."""
    try:
      self._AcquireService().objects().delete(bucket=bucket,
                                              object=obj).execute()
    except HttpError as err:
      if err.resp.status == 404 and ignore_missing_files:
        logging.info('ignoring missing file (404) error deleting gs://%s/%s %r',
                     bucket, obj, err)
      else:
        raise err

  def ComposeObjects(self, bucket, src_objects, dest_obj, content_type):
    """Composes multiple objects into a one.

    Source objects must be located in the same bucket.

    Args:
      bucket: specifies the GCS bucket.
      src_objects: a list of objects to compose.
      dest_obj: the name of the composite object.
      content_type: the content/MIME type of the destination object.

    Returns:
      The destination object resource.
    """
    src_objects_len = len(src_objects)
    if src_objects_len < 1:
      return {}
    elif src_objects_len <= self.MAX_COMPOSABLE_OBJECTS:
      body = {'sourceObjects': [{'name': s} for s in src_objects],
              'destination': {'contentType': content_type}}
      logging.info('calling gcs composit with %d objects', len(src_objects))
      req = self._AcquireService().objects().compose(destinationBucket=bucket,
                                                     destinationObject=dest_obj,
                                                     body=body)
      return req.execute()
    elif src_objects_len <= self.MAX_TOTAL_COMPOSABLE_OBJECTS:
      # A composed object can store all these src_objects
      tmp = []
      for chunk in SplitEvenly(src_objects, self.MAX_COMPOSABLE_OBJECTS):
        tmp.append(self.UrlToBucketAndName(self.UrlCreator(bucket)())[1])
        self.ComposeObjects(bucket, chunk, tmp[-1], content_type)
      r = self.ComposeObjects(bucket, tmp, dest_obj, content_type)
      # Clean up temporary objects
      for t in tmp:
        self.DeleteObject(bucket, t)
      return r
    else:
      # A composed object will have too many parts to make this in one compose.
      # So we make a few objects, then copy them to reset the component count.
      tmp = []
      for chunk in SplitEvenly(src_objects, self.MAX_TOTAL_COMPOSABLE_OBJECTS):
        tmp.append(self.UrlToBucketAndName(self.UrlCreator(bucket)())[1])
        self.ComposeObjects(bucket, chunk, tmp[-1], content_type)
      # now compress those temp files to reset the composed object count
      for dest_obj in tmp:
        self.CompressObject(self.MakeUrl(*dest_obj))
      r = self.ComposeObjects(bucket, tmp, dest_obj, content_type)
      # Clean up temporary objects
      for t in tmp:
        self.DeleteObject(bucket, t)
      return r

  def CompressObject(self, src):
    """Compresses an object to reset the composite-ness of it.

    Warning: this is expensive and slow since right now the only way
    to do this is to download and reupload the file.

    Args:
      src: must specify both the bucket and object (e.g. gs://bucketA/obj1).
    Raises:
      ValueError: if src not provided or are malformed.

    """
    if not src:
      raise ValueError('No source specified.')

    (src_bucket, src_object) = self.UrlToBucketAndName(src)
    tmp = self.UrlCreator(bucket=src_bucket)

    logging.info('Compressing %s to %s', src, tmp)

    # copy the file contents.
    src_path = Gcs.UrlToBucketAndNamePath(src)
    tmp_path = Gcs.UrlToBucketAndNamePath(src)

    count = 0
    with cloudstorage.open(src_path) as src_obj:
      with cloudstorage.open(tmp_path, 'w') as tmp_obj:
        while True:
          count += 1
          if count % 128 == 0:
            logging.info('still compressing... done %d 8M chunks', count)
          buf = src_obj.read(self.READ_CHUNK_SIZE)
          if buf and len(buf):
            tmp_obj.write(buf)
          else:
            break

    # remove the src
    logging.info('Compressing replacing %s with %s', src, tmp)
    self.DeleteObject(src_bucket, src_object)
    self.CopyObject(tmp, src)
    logging.info('Compressing %s DONE', src)


def SplitEvenly(arr, max_size):
  """Split an array into even chunks that are no larger than max_size."""
  arr_len = len(arr)
  if arr_len < 1:
    return
  split_size = int(math.ceil(float(arr_len) /
                             (math.ceil(float(arr_len) / max_size))))
  logging.info('split size is %d for len %d max_size %d',
               split_size, arr_len, max_size)
  idx = 0
  while idx < arr_len:
    yield arr[idx:idx + split_size]
    idx += split_size
