# Data Pipeline Solution

## Examples

In this section, we'll start with a simple data pipeline and build it up to span
multiple services, even reaching beyond Google Cloud Platform for data. This
should give you a good feel for what's going on under the hood, how
configurations are processed and how to build your own stages.

### Google Cloud Storage to Google BigQuery

One of the example data pipelines included in this package is called
*[gcstobigquery.json]* and it does pretty much what you'd expect: loads a CSV file
from Google Cloud Storage into a BigQuery table. It uses an example source file
*languagecodes.csv* that you could copy from the examples folder into your GCS
bucket. Let's take a look:

```json
{
  "inputs": [
    {
      "type": "CsvMatchReplace",
      "fieldDelimiter": ",",
      "sources": ["gs://{{ storage.bucket }}/languagecodes.csv"],
      "columns": [
        {
          "wanted": true,
          "type": "STRING",
          "name": "Language",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "code",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "INTEGER",
          "name": "id",
          "transformations": []
        }
      ]
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      "destinationTable": {
        "projectId": "{{ app.id }}",
        "tableId": "languagecodes",
        "datasetId": "examples"
      },
      "createDisposition": "CREATE_IF_NEEDED",
      "writeDisposition": "WRITE_APPEND",
      "schema": {
        "fields": [
          {
            "type": "STRING",
            "name": "Language"
          },
          {
            "type": "STRING",
            "name": "code"
          },
          {
            "type": "INTEGER",
            "name": "id"
          }
        ]
      }
    }
  ]
}
```


Looking first at the `CsvMatchReplace` stage, we can tell this will load the
comma-delimited `languages.csv` file from our default bucket and effectively do
nothing to it (since we're keeping all the columns). The `BigQueryOutput` stage
is going to add some data into a table in BigQuery, using a schema that looks
exactly the columnar output from the previous stage. But hang on, how does this
stage know what data to write into the table? One can see that neither does our
input stage provide `sinks` nor does our output stage provide `sources`. The
application will automatically generate a temporary object in GCS and inject the
missing parameters into each stage's configuration. When the `CsvMatchReplace`
stage is run, it will have the URL of this temporary GCS object as a sink, and
likewise, when the `BigQueryOutput` stage is run, it will the same URL as a
source.

Let's play around with this a little bit to see how we could accomplish the same
objective in a few different ways.

#### Use only one stage

A pipeline configuration is valid as long as it has either an input or an output
stage. Given this trivial example, we could achieve the same end result by
actually removing the `inputs` section entirely from the JSON file and copying
the `sources` parameter from `CsvMatchReplace` into the `BigQueryOutput` stage.

#### Add a new input stage

What's interesting is that `CsvMatchReplace` is used an input stage in the first
place. Clearly it's possible to do and works as expected but this stage is more
naturally thought of as a transformation. So let's extend the pipeline
configuration by adding `GcsInput` as our input stage and moving
`CsvMatchReplace` into a `transforms` section:

```json
{
  "inputs": [
    {
      "type": "GcsInput",
      "object": "gs://{{ storage.bucket }}/languagecodes.csv"
    }
  ],
  "transforms": [
    {
      "type": "CsvMatchReplace",
      "fieldDelimiter": ",",
      "columns": [
        {
          "wanted": true,
          "type": "STRING",
          "name": "Language",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "code",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "INTEGER",
          "name": "id",
          "transformations": []
        }
      ]
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      "destinationTable": {
        "projectId": "{{ app.id }}",
        "tableId": "languagecodes",
        "datasetId": "examples"
      },
      "createDisposition": "CREATE_IF_NEEDED",
      "writeDisposition": "WRITE_APPEND",
      "schema": {
        "fields": [
          {
            "type": "STRING",
            "name": "Language"
          },
          {
            "type": "STRING",
            "name": "code"
          },
          {
            "type": "INTEGER",
            "name": "id"
          }
        ]
      }
    }
  ]
}
```


### Google Cloud Datastore to Google BigQuery

Let's make this pipeline slightly less boring by having it actually do something
to the incoming data. And while we're at it, instead of pulling static data from
a file, we'll obtain our input data by querying the Datastore.

```json
{
  "inputs": [
    {
      "type": "DatastoreInput",
      "gql": "SELECT * FROM Pipeline"
    }
  ],
  "transforms": [
    {
      "type": "CsvMatchReplace",
      "fieldDelimiter": ",",
      "columns": [
        {
          "wanted": true,
          "type": "TIMESTAMP",
          "name": "last_updated",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "name",
          "transformations": [{
            "match": "_",
            "replace": "-"
          }]
        },
        {
          "wanted": true,
          "type": "TIMESTAMP",
          "name": "created",
          "transformations": []
        },
        {
          "wanted": false,
          "name": "running_pipeline_ids"
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "api_key",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "config",
          "transformations": []
        }
      ]
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      "destinationTable": {
        "projectId": "{{ app.id }}",
        "tableId": "pipelines",
        "datasetId": "examples"
      },
      "createDisposition": "CREATE_IF_NEEDED",
      "writeDisposition": "WRITE_APPEND",
      "schema": {
        "fields": [
          {
            "type": "TIMESTAMP",
            "name": "last_updated"
          },
          {
            "type": "STRING",
            "name": "name"
          },
          {
            "type": "TIMESTAMP",
            "name": "created"
          },
          {
            "type": "STRING",
            "name": "api_key"
          },
          {
            "type": "STRING",
            "name": "config"
          }
        ]
      }
    }
  ]
}
```


Alright so this is getting a little better. We've embedded a query in the
pipeline itself (we could have also loaded it from a GCS object) and we're
ignoring and modifying some of the data. Again it's worth pointing out there is
no need to explicitly identify the sources and sinks for each stage. However,
now our pipeline is actually stashing different bits in each of the intermediate
objects.

#### Better querying

We can actually make the Datastore do some work for us. By defining a
[projection query] with only the fields we're interested, we
can reduce the amount of data coming into our transform.

```json
{
  "inputs": [
    {
      "type": "DatastoreInput",
      "gql": "SELECT * FROM Pipeline",
      "params": {
        "projection": ["name", "created", "api_key", "config"]
      }
    }
  ],
  "transforms": [
    {
      "type": "CsvMatchReplace",
      "fieldDelimiter": ",",
      "columns": [
        {
          "wanted": true,
          "type": "STRING",
          "name": "name",
          "transformations": [{
            "match": "_",
            "replace": "-"
          }]
        },
        {
          "wanted": true,
          "type": "TIMESTAMP",
          "name": "created",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "api_key",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "config",
          "transformations": []
        }
      ]
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      "destinationTable": {
        "projectId": "{{ app.id }}",
        "tableId": "pipelines",
        "datasetId": "examples"
      },
      "createDisposition": "CREATE_IF_NEEDED",
      "writeDisposition": "WRITE_APPEND",
      "schema": {
        "fields": [
          {
            "type": "STRING",
            "name": "name"
          },
          {
            "type": "TIMESTAMP",
            "name": "created"
          },
          {
            "type": "STRING",
            "name": "api_key"
          },
          {
            "type": "STRING",
            "name": "config"
          }
        ]
      }
    }
  ]
}
```


### Multiple Inputs and Outputs

For this next section, let's suppose you have a successful mobile game running
on Google App Engine that you built with the
[Mobile Backend Starter].
We'll start with doing some log aggregation; and assume that
another process is [downloading the daily logs] and
storing them in GCS. The `GcsInput` stage can use an object name prefix
and glob pattern to specify multiple input objects:

```json
{
  "type": "GcsInput",
  "objects": {
    "bucket": "mobilegame_bucket",
    "prefix": "logs/daily/",
    "glob": "*.txt"
  }
}
```

#### Composing objects

Next, we need to decide how we want to handle all this data. We might want a
stage that can iterate over all of its `sinks` and processes each object
independently but none of the transforms that ship with the application offer
this functionality. So instead, we need some way to combine all of the input
together. Fortunately, Google Cloud Storage supports
[object composition] and Data Pipeline provides a stage that
can make use of this feature. Fleshing out the pipeline a little more:

```json
{
  "inputs": [
    {
      "type": "GcsInput",
      "objects": {
        "bucket": "mobilegame_bucket",
        "prefix": "logs/daily/",
        "glob": "*.txt"
      }
    }
  ],
  "transforms": [
    {
      "type": "GcsCompositor",
      "contentType": "text/plain",
      "sinks": "[gs://mobilegame_bucket/logs/processing/daily_agg.txt]"
    }
  ]
}
```


We've added the `GcsCompositor` as a transform since we'll be extending the
pipeline but it is certainly possible to use it as an output stage. By
explicitly defining a sink, we know exactly where the aggregated output will end
up.

#### Multiple inputs

Moving forward in our scenario, suppose we actually have several games running
on multiple cloud providers and we would like to analyze all of the daily logs
in a given month across all of the games. Let's assume for simplicity that the
log files from other clouds are in the same format as ours from App Engine.

```json
{
  "inputs": [
    {
      "type": "GcsInput",
      "objects": {
        "bucket": "mobilegame_bucket",
        "prefix": "logs/daily/oct"
      },
      "s3Credentials": {
        "accessKey": "ABC",
        "accessSecret": "123"
      }
    },
    {
      "type": "HttpInput",
      "url": "http://ourother.gamedata.com/logs/2013/monthly/oct.txt"
    },
      "type": "S3Input",
      "objects": {
        "bucket": "mobilegame_bucket",
        "prefix": "logs/daily/",
        "glob": "10_??_2013.txt"
      }
  ],
  "transforms": [
    {
      "type": "GcsCompositor",
      "contentType": "text/plain"
    },
    {
      "type": "CsvMatchReplace",
      ...
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      ...
    }
  ]
}
```

Now when the pipeline runs, it will download the log data from external sources
into temporary GCS objects, then compose them all together into a single object
which is passed along to the next transform. So that's pretty useful but what if
we need something more interesting like combining this with live data from our
backend?

```json
{
  "inputs": [
    {
      "type": "GcsInput",
      "objects": {
        "bucket": "mobilegame_bucket",
        "prefix": "logs/daily/oct"
      },
      "s3Credentials": {
        "accessKey": "ABC",
        "accessSecret": "123"
      }
    },
    {
      "type": "HttpInput",
      "url": "http://ourother.gamedata.com/logs/2013/monthly/oct.txt"
    },
    {
      "type": "S3Input",
      "objects": {
        "bucket": "mobilegame_bucket",
        "prefix": "logs/daily/",
        "glob": "10_??_2013.txt"
      }
    }
  ],
  "transforms": [
    {
      "type": "GcsCompositor",
      "contentType": "text/plain"
      "sinks": "gs://{{ storage.bucket }}/temp/oct_logs_agg.txt"
    },
    {
      "type": "DatastoreInput",
      "object": "gs://mobilegame_bucket/data/qry/monthly.gql",
      "sources": null,
      "sinks": "gs://{{ storage.bucket }}/temp/oct_live_qry.csv"
    },
    {
      "type": "AnalysisEngine",
      ...
      "sources": ["gs://{{ storage.bucket }}/temp/oct_logs_agg.txt",
                  "gs://{{ storage.bucket }}/temp/oct_live_qry.csv"]
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      ...
    }
  ]
}
```


The first thing to notice is we're being more explicit about wiring the stages
together - both `GcsCompositor` and `DatastoreInput` declare their own `sinks`
that are in turn used as the `sources` for the new `AnalysisEngine` transform.
If we didn't do this manually, the application would only bind the
`DatastoreInput` to the `AnalysisEngine`. Also notice that the `sources`
parameter is defined as `null`. This prevents the application from incorrectly
adding the `S3Input` sink as the source for `DatastoreInput` instead of
`GcsCompositor`.

#### Multiple outputs

What if we don't actually have that `AnalysisEngine` stage we used above, and we
instead prefer to leverage BigQuery? Fortunately this is pretty easy - just
remove the transform and add a second 'BigQueryOutput' stage:

```json
{
  "inputs": [
    {
      "type": "GcsInput",
      "objects": {
        "bucket": "mobilegame_bucket",
        "prefix": "logs/daily/oct"
      },
      "s3Credentials": {
        "accessKey": "ABC",
        "accessSecret": "123"
      }
    },
    {
      "type": "HttpInput",
      "url": "http://ourother.gamedata.com/logs/2013/monthly/oct.txt"
    },
    {
      "type": "S3Input",
      "objects": {
        "bucket": "mobilegame_bucket",
        "prefix": "logs/daily/",
        "glob": "10_??_2013.txt"
      }
    }
  ],
  "transforms": [
    {
      "type": "GcsCompositor",
      "contentType": "text/plain"
      "sinks": "gs://{{ storage.bucket }}/temp/oct_logs_agg.txt"
    },
    {
      "type": "DatastoreInput",
      "object": "gs://mobilegame_bucket/data/qry/monthly.gql",
      "sources": null,
      "sinks": "gs://{{ storage.bucket }}/temp/oct_live_qry.csv"
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      ...
      "sources": ["gs://{{ storage.bucket }}/temp/oct_logs_agg.txt"]
    },
    {
      "type": "BigQueryOutput",
      ...
      "sources": ["gs://{{ storage.bucket }}/temp/oct_live_qry.csv"]
    }
  ]
}
```


### Using Hadoop

Every stage is run on Google App Engine application [module] instances
including those that perform actual processing. Sharding work across these
instance is one technique for applying more compute power to a large
transformation. Another is offloading processing entirely to a Hadoop cluster
running on Google Compute Engine.

#### Disclaimer

This solution makes use of an proxy server to intermediate between App Engine
and Hadoop running on Compute Engine and as such is insecure. It is provided as
an example only and not intended for production deployments.

#### Cluster lifetime management

Two pipeline stages, `HadoopSetup` and `HadoopShutdown`, can be used to manage
the Hadoop cluster. For ad hoc analysis, these stages can be added to the
`input` and `output` sections and the cluster's lifetime will be bounded by the
duration of the pipeline execution.

```json
{
  "inputs": [
    {
      "type": "HadoopSetup",
      "project": "example",
      "prefix": "datapipeline",
      "machineType": "n1-highcpu-2",
      "zone": "us-central1-a",
      "numWorkers": 3,
      "sinks": null
    }
  ],
  "transforms": [
    {
      "type": "HadoopCsvMatchReplace",
      "fieldDelimiter": ",",
      "skipLeadingRows": 1,
      "hadoopTmpDir": "gs://{{ storage.bucket }}/tmp",
      "sources": ["gs://{{ storage.bucket }}/languagecodes.csv"],
      "sinks": ["gs://{{ storage.bucket }}/transformoutput.csv"],
      "columns": [
        {
          "wanted": false,
          "type": "INTEGER",
          "name": "ID",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "Name",
          "transformations": []
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "Country",
          "transformations": [
            {
              "match": "Japan",
              "replace": "JP"
            }
          ]
        },
        {
          "wanted": true,
          "type": "STRING",
          "name": "Language",
          "transformations": []
        }
      ]
    }
  ],
  "outputs": [
    {
      "type": "HadoopShutdown",
      "project": "example",
      "prefix": "datapipeline",
      "sources": null,
    }
  ]
}
```


To reuse a long lived cluster, just be sure to include the `HadoopSetup` stage
as an `input` and define a separate pipeline containing only `HadoopShutdown`
that can be run whenever necessary.

```json
{
  "inputs": [
    {
      "type": "HadoopSetup",
      "project": "example",
      "prefix": "datapipeline",
      "sinks": null
    },
    {
      "type": "HttpInput",
      "url": "http://hci.stanford.edu/jheer/workshop/data/worldbank/worldbank.csv"
    }
  ],
  "transforms": [
    {
      "type": "HadoopCsvMatchReplace",
      "sinks": ["gs://{{ storage.bucket }}/transformoutput.csv"],
      ...
    }
  ]
}
```

[gcstobigquery.json]: /static/examples/gcstobigquery.json
[projection query]: https://developers.google.com/appengine/docs/python/datastore/projectionqueries
[downloading the daily logs]: https://developers.google.com/appengine/docs/python/tools/uploadinganapp#Python_Downloading_logs
[Mobile Backend Starter]: https://developers.google.com/cloud/samples/mbs/
[object composition]: https://developers.google.com/storage/docs/composite-objects#_Compose
[module]: https://developers.google.com/appengine/docs/python/modules/
[installation instructions]: https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python
[Enable billing]: https://developers.google.com/appengine/docs/pricing#first_time
[install gsutil]: https://developers.google.com/storage/docs/gsutil_install
[BigQuery]: https://bigquery.cloud.google.com/
[Cloud Console]: https://cloud.google.com/console

