# Data Pipeline Solution

## Overview

Data Pipeline is a self-hosted [Google App Engine] sample
application that enables its users to easily define and execute data flows
across different [Google Cloud Platform][Google Cloud Platform] products. It is
intended as a reference for connecting multiple cloud services together, and as
a head start for building custom data processing solutions.

Currently, the application supports:

* Reading data from [Google Cloud Storage], [Amazon S3] and arbitrary HTTP endpoints,

* Querying data from [Google Cloud Datastore],

* Transforming data on Google App Engine using the [Google App Engine Pipeline API],
and [Google Compute Engine] using [Apache Hadoop].

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

## Design

### Concepts

Dataflow is a technique of expressing computational dependency among multiple
components. In this solution, dependencies are expressed in terms of moving data
between and processing data within various cloud services. We use the notion of
a *data **pipeline* to capture these connections and invocations of services. A
data pipeline is defined by a simple JSON configuration document that describes:

* *inputs* - from which service(s) the data should be obtained,

* *transforms* - into which service(s) the data should be loaded and processed,

* *outputs* - to which service(s) the results should be stored.

Each of the above is nothing more than a way to classify the nature of the
different *stages* that make up the data pipeline. Generally speaking however,
there is no restriction on into which section a given stage maybe appear. A
stage is a construct that encapsulates use of a service, for example, copying a
file from a website to Google Cloud Storage (GCS) or loading a dataset into
Google BigQuery. Stages are independent service invocations and share no
knowledge about one another. The "glue" that holds the data pipeline together is
Google Cloud Storage. Stages can be explicit in their data inputs and outputs or
they can rely on the application to bind them together using intermediate
objects in GCS. These bindings are known as *sources* and *sinks*. A typical
stage will load its data from one or more source objects and store its results
to a sink object. Certainly there are exceptions: input stages usually have
specific properties that describe how to obtain the data (for example,
downloading from an S3 bucket or executing a query on the Google Cloud
Datastore), and output stages may, instead of dumping the results to storage,
push them to BigQuery for interactive analysis.

### Architecture

Data Pipeline is built on top of Google App Engine and its Pipeline API and
makes use of both
[automatic and basic instance scaling].
This allows users to run multiple pipelines asynchronously and provides a UI for
monitoring status and progress. The App Engine Pipeline API is basis for
[App Engine MapReduce] and, while it can
support many types of execution graphs, this sample applications currently
provides only a simple *push pipeline*. In this design, execution of stages runs
one-way and the results of one stage are pushed as the input to the next.

![Architecture Diagram](/static/img/help/image_0.png)

Input stages are executed in parallel by the runtime and fanned-into the first
transform; likewise, the last transform fans-out its results to all output
stages, which are executed parallel.

### Linting

The application automatically lints data pipeline configurations as you edit
them. You will not be able to save your configuration unless it is both
syntactically correct and satisfies each stage's requirements (e.g. the stage
type matches a known class, all necessary parameters are provided, etc).
Roughly, the linting process performs the following steps:

1. Strip any comments and validate JSON formatting,

2. Add any *default options* to the configuration,

3. Substitute values for any embedded variables,

4. Ensure at least one input or output stage is defined,

5. For each configured stage:

    1. Ensure the `type` field exists and refers to a instantiable class,

    2. Check sources and sinks,

    3. Run stage `Lint` function.

Configuration documents are stored in the Datastore. The `Datastore Viewer` in
the App Engine Console can be used to examine various metadata about the data
pipeline including its `api_key` and timestamp of last modification.

### Running

When a data pipeline is run, its configuration is read from the Datastore. It
will be then sent through a scrubbing process that performs any automatic stage
wiring and transient sink/source URL generation. A new pipeline object will then
be created with the complete configuration and executed. The pipeline itself is
little more than a stage orchestrator: instantiating new stage objects and
running them with their own configurations.

![Process Diagram](/static/img/help/image_1.png)



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
[App Engine MapReduce]: https://code.google.com/p/appengine-mapreduce/
[automatic and basic instance scaling]: https://developers.google.com/appengine/docs/python/modules/#Python_Instance_scaling_and_class
