# Data Pipeline Solution

## Overview

Data Pipeline is a self-hosted [Google App Engine] sample
application that enables its users to easily define and execute data flows
across different [Google Cloud Platform] products. It is
intended as a reference for connecting multiple cloud services together, and as
a head start for building custom data processing solutions.

Currently, the application supports:

* Reading data from [Google Cloud Storage],
[Amazon S3] and arbitrary HTTP endpoints,

* Querying data from [Google Cloud Datastore],

* Transforming data on Google App Engine using the [Google App Engine Pipeline API], and
[Google Compute Engine] using [Apache Hadoop].

* Composing and storing data in Google Cloud Storage,

* Loading data into [Google BigQuery]

### Copyright

Copyright 2013 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

[http://www.apache.org/licenses/LICENSE-2.0]

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

### Disclaimer

This is not an official Google product.

## Installation Guide

If you don't have it already, install the Google App Engine SDK and follow the
[installation instructions]. As noted previously,
*Data Pipeline* use App Engine Modules which were introduced in App Engine 1.8.3
so you must install at least that version.

### Open Source Libraries

The following packages should be installed in the same directory as this
README.md file. The contents of the following code block and be copied and
pasted into shell:

```json
mkdir third_party

# dateutil
curl -o - http://labix.org/download/python-dateutil/python-dateutil-1.5.tar.gz |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/python-dateutil-1.5/dateutil dateutil)

# jquery UI Layout
curl -o app/static/js/jquery.layout-latest.min.js http://layout.jquery-dev.net/lib/js/jquery.layout-latest.min.js

# Google Application Utilities for Python
curl -o - https://google-apputils-python.googlecode.com/files/google-apputils-0.4.0.tar.gz |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/google-apputils-0.4.0/google/apputils google_apputils)

 # Rename the package so it doesn't conflict with google.appengine.
perl -p -i~ -e 's/google\.apputils/google_apputils/g' app/google_apputils/*.py

# Mock
curl -o - https://pypi.python.org/packages/source/m/mock/mock-1.0.1.tar.gz#md5=c3971991738caa55ec7c356bbc154ee2 |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/mock-1.0.1 mock)
(cd app/mock; ln -s mock.py __init__.py)

# Google Cloud Storage Client
curl -o - https://pypi.python.org/packages/source/G/GoogleAppEngineCloudStorageClient/GoogleAppEngineCloudStorageClient-1.8.3.1.tar.gz |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/GoogleAppEngineCloudStorageClient-1.8.3.1/cloudstorage)

# Google App Engine MapReduce
curl -o - https://pypi.python.org/packages/source/G/GoogleAppEngineMapReduce/GoogleAppEngineMapReduce-1.8.3.1.tar.gz |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/GoogleAppEngineMapReduce-1.8.3.1/mapreduce)

# JSON.Minify
curl -o third_party/jsonminify.zip https://codeload.github.com/getify/JSON.minify/zip/master
(cd third_party; unzip jsonminify.zip)
(cd app; ln -s ../third_party/JSON.minify-master jsonminify)
touch app/jsonminify/__init__.py

# parsedatetime
curl -o third_party/parsedatetime.zip https://codeload.github.com/bear/parsedatetime/zip/master
(cd third_party; unzip parsedatetime.zip)
(cd app; ln -s ../third_party/parsedatetime-master/parsedatetime)

# Google API Client Library for Python
curl -o - https://google-api-python-client.googlecode.com/files/google-api-python-client-1.2.tar.gz |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/google-api-python-client-1.2/apiclient)
(cd app; ln -s ../third_party/google-api-python-client-1.2/oauth2client)
(cd app; ln -s ../third_party/google-api-python-client-1.2/uritemplate)

# httplib2
curl -o - https://httplib2.googlecode.com/files/httplib2-0.8.tar.gz |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/httplib2-0.8/python2/httplib2)

# boto
curl -o - https://codeload.github.com/boto/boto/tar.gz/2.13.3 |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/boto-2.13.3/boto)

# Markdown
curl -o - https://pypi.python.org/packages/source/M/Markdown/Markdown-2.2.0.tar.gz |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/Markdown-2.2.0/markdown)

# Pygments
curl -o - https://bitbucket.org/birkenfeld/pygments-main/get/1.5.tar.gz |
    tar -zxv -C third_party -f -
(cd app; ln -s ../third_party/birkenfeld-pygments-main-eff3aee4abff/pygments)
```

### Unit Tests

Running the bundled unit tests helps verify that all the libraries have been
installed correctly. To run the unit tests locally you'll need to have the App
Engine SDK libraries in your Python path.

```json
# For example, suppose the App Engine SDK was installed in /usr/local/google_appengine

GAE_PATH=/usr/local/google_appengine; export PYTHONPATH=$GAE_PATH:.; for fil in $GAE_PATH/lib/*; do export PYTHONPATH=$fil:$PYTHONPATH; done;
```


You can run the unit tests with:

```json
(cd app; python -m unittest discover src '*_test.py')
```


### Installation

1. Make an app at appengine.google.com (we use an app id of `example` for this
document).

2. [Enable billing].

3. Set up a Google Cloud Storage bucket (if you don't have already have it,
[install gsutil]. If you do have it, you might need to run
`gsutil config `to set up the credentials):

```json

gsutil mb gs://example/
gsutil acl ch -u example@appspot.gserviceaccount.com:FC  gs://example
gsutil defacl ch -u example@appspot.gserviceaccount.com:FC  gs://example
gsutil cp app/static/examples/languagecodes.csv gs://example
```


4. Go to `Application Setting` for your app on appengine.google.com

5. Copy the service account `example@appspot.gserviceaccount.com`

6. Click on the `Google APIs Console Project Number`Click on the Google APIs
Console Project Number

7. Add the service account under `Permissions`.

8. Click on `APIs and Auth` and turn on BigQuery, Google Cloud Storage and
Google Cloud Storage JSON API.

9. Replace the application name in the .yaml files. So for example, if your app
is called example.appspot.com:

```json
perl -p -i~ -e 's/INSERT_YOUR_APPLICATION_NAME_HERE/example/' app/app.yaml app/backend.yaml
```


1. Now publish your application:

```json
appcfg.py update --oauth2 app/app.yaml app/backend.yaml
```


1. You can now connect to your application and verify it:

    1. Click the little cog and add your default bucket of `gs://example` (be
    sure to substitute your bucket name here). You probably want to add prefix
    (e.g. `tmp/`) to isolate any temporary objects used to move data between
    stages.

    2. Now create a new pipeline and upload the contents of
    *app/static/examples/gcstobigquery.json*.

    3. Run the pipeline. It should successfully run to completion.

    4. Go to [BigQuery] and view your dataset and table.

### Set up a Hadoop Environment

As in the previous section, here we also assume `gs://example` for your bucket;
and `gce-example` is a project that has enough quota for Google Compute Engine
to host your Hadoop cluster. The quota size (instances and CPUs) depends on the
Hadoop cluster size you will be using. We can use the same project as we did for
BigQuery. As before, the following script can be copied and pasted into a shell
as-is:

```json
# Setup variables
BUCKET=gs://example  # Change this.
PROJECT=gce-example  # Change this.
PACKAGE_DIR=$BUCKET/hadoop

# Download Hadoop
curl -O http://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1.tar.gz

# Download additional Debian packages required for Hadoop
mkdir deb_packages

(cd deb_packages ; curl -O http://security.debian.org/debian-security/pool/updates/main/o/openjdk-6/openjdk-6-jre-headless_6b27-1.12.6-1~deb7u1_amd64.deb)
(cd deb_packages ; curl -O http://security.debian.org/debian-security/pool/updates/main/o/openjdk-6/openjdk-6-jre-lib_6b27-1.12.6-1~deb7u1_all.deb)
(cd deb_packages ; curl -O http://http.us.debian.org/debian/pool/main/n/nss/libnss3-1d_3.14.3-1_amd64.deb)
(cd deb_packages ; curl -O http://http.us.debian.org/debian/pool/main/n/nss/libnss3_3.14.3-1_amd64.deb)
(cd deb_packages ; curl -O http://http.us.debian.org/debian/pool/main/c/ca-certificates-java/ca-certificates-java_20121112+nmu2_all.deb)
(cd deb_packages ; curl -O http://http.us.debian.org/debian/pool/main/n/nspr/libnspr4_4.9.2-1_amd64.deb)
(cd deb_packages ; curl -O http://http.us.debian.org/debian/pool/main/p/patch/patch_2.6.1-3_amd64.deb)

# Download and setup Flask and other packages
mkdir -p rpc_daemon

ln app/static/hadoop_scripts/rpc_daemon/__main__.py rpc_daemon/
ln app/static/hadoop_scripts/rpc_daemon/favicon.ico rpc_daemon/

curl -o - https://pypi.python.org/packages/source/F/Flask/Flask-0.9.tar.gz |
    tar zxf - -C rpc_daemon/
curl -o - https://pypi.python.org/packages/source/J/Jinja2/Jinja2-2.6.tar.gz |
    tar zxf - -C rpc_daemon/
curl -o - https://pypi.python.org/packages/source/W/Werkzeug/Werkzeug-0.8.3.tar.gz |
    tar zxf - -C rpc_daemon/

(
  cd rpc_daemon ;
  ln -s Flask-*/flask . ;
  ln -s Jinja2-*/jinja2 . ;
  ln -s Werkzeug-*/werkzeug . ;
  zip -r ../rpc-daemon.zip __main__.py favicon.ico flask jinja2 werkzeug
)

# Create script package
tar zcf hadoop_scripts.tar.gz -C app/static  \
    hadoop_scripts/gcs_to_hdfs_mapper.sh  \
    hadoop_scripts/hdfs_to_gcs_mapper.sh  \
    hadoop_scripts/mapreduce__at__master.sh

# Create SSH key
mkdir -p generated_files/ssh-key
ssh-keygen -t rsa -P '' -f generated_files/ssh-key/id_rsa
tar zcf generated_files.tar.gz generated_files/

# Upload to Google Cloud Storage
gsutil -m cp -R hadoop-1.2.1.tar.gz deb_packages/ $PACKAGE_DIR/
gsutil -m cp  \
    app/static/hadoop_scripts/startup-script.sh  \
    app/static/hadoop_scripts/*.patch  \
    hadoop_scripts.tar.gz  \
    generated_files.tar.gz  \
    rpc-daemon.zip  \
    $PACKAGE_DIR/

# Setup a firewall rule
gcutil --project=$PROJECT addfirewall datapipeline-hadoop  \
    --description="Hadoop for Datapipeline"  \
    --allowed="tcp:50070,tcp:50075,tcp:50030,tcp:50060,tcp:80"
```

In order for this App Engine application to launch Google Compute Engine
instances in the project, the service account of the App Engine application must
be granted `edit` permissions. To do this, follow these steps:

1. Go to `Application Settings` on in the App Engine console and copy the value
(should be an email address) indicated the `Service Account Name` field.

2. Go to the [Cloud Console] of the project for which Google
Compute Engine will be used.

3. Go to the `Permissions` page, and click the red `ADD MEMBER` button on the
top.

4. Paste the value from step #1 as the email address. Make sure the account has
`can edit` permission.  Click the `Add` button to save the change.



[Google Cloud Platform]: https://cloud.google.com/
[Google App Engine]: https://cloud.google.com/products/app-engine
[Amazon S3]: http://aws.amazon.com/s3/
[Google Cloud Storage]: https://cloud.google.com/products/cloud-storage
[Google Cloud Datastore]: https://developers.google.com/datastore/
[Apache Hadoop]: http://hadoop.apache.org/
[Google Compute Engine]: https://cloud.google.com/products/compute-engine
[Google App Engine Pipeline API]: http://code.google.com/p/appengine-pipeline/
[Google BigQuery]: https://cloud.google.com/products/big-query
[http://www.apache.org/licenses/LICENSE-2.0]: http://www.apache.org/licenses/LICENSE-2.0
[installation instructions]: https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python
[Enable billing]: https://developers.google.com/appengine/docs/pricing#first_time
[install gsutil]: https://developers.google.com/storage/docs/gsutil_install
[BigQuery]: https://bigquery.cloud.google.com/
[Cloud Console]: https://cloud.google.com/console

