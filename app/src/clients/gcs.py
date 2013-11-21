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
import math
import urlparse
import uuid

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
  MAX_COMPOSABLE_OBJECTS = 32
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

  def DeleteObject(self, bucket, obj):
    """Removes an existing GCS object."""
    self._AcquireService().objects().delete(bucket=bucket, object=obj).execute()

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
    l = len(src_objects)
    if l < 1:
      return {}
    elif l <= self.MAX_COMPOSABLE_OBJECTS:
      body = {'sourceObjects': [{'name': s} for s in src_objects],
              'destination': {'contentType': content_type}}
      req = self._AcquireService().objects().compose(destinationBucket=bucket,
                                                     destinationObject=dest_obj,
                                                     body=body)
      return req.execute()
    else:
      n = int(math.ceil(float(l) / self.MAX_COMPOSABLE_OBJECTS))
      tmp = []
      for i in range(n):
        tmp.append(self.UrlToBucketAndName(self.UrlCreator(bucket)())[1])
        self.ComposeObjects(bucket,
                            src_objects[i * self.MAX_COMPOSABLE_OBJECTS:
                                        (i + 1) * self.MAX_COMPOSABLE_OBJECTS],
                            tmp[i],
                            content_type)
      r = self.ComposeObjects(bucket, tmp, dest_obj, content_type)
      # Clean up temporary objects
      for t in tmp:
        self.DeleteObject(bucket, t)
      return r
