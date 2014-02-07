# Cloud Deployment History Solution

## Overview

Large production systems usually consist of hundreds of dynamic components such
as instances, disks, networks, and firewall rules. These components are
transient in nature. Often times it is important to understand the changes over
time. For example, a time series view of how many instances with a given tag
have been created and deleted the past week can be helpful to understand the
usage of the cloud deployment. This solution aims to solve many history related
questions posed by developers of large systems.

This solution utilizes the data pipeline solution to create a workflow that
captures the compute engine deployment configuration into BigQuery.
By using the *[App Engine Cron Service]*, deployment configuration
can be captured as time series data and queried using BigQuery. [Sample
BigQuery queries](#cookbook) are provided for you to get started on the
analysis. The workflow is best illustrated with the following diagram:

![Cloud Deployment History Architecture](/static/img/help/cloud_history_arch.png)

## Data Pipelines

Two sample pipelines are provided to log the Compute Engine instances, disks and
zone operations data into BigQuery. The instances and disks data are defined as
one pipeline.  The zone operations is defined as a separate pipeline. The reason
why they are separate pipeline is because they have different requirements for
how frequently the pipeline should be run. Instances and Disks data is transient
in nature so the pipeline should be scheduled to run more frequently. The data
for zone operations is not transient, so the pipleline does not have to be run
as frequently.

#### Compute Engine instances and disks pipeline

This sample pipeline included in this package is called
*[cloud_history_pipeline.json]*. This pipeline reads the current Google Compute
Engine instances and disks data using the Compute Engine API interface. The data
is then transformed to a JSON format that is compatible with BigQuery before
loading into BigQuery.

Let's take a closer look of the pipeline specification:

```json
{
  "inputs": [
    {
      "type": "GceInstancesInput",
      "apiInput": {
        "projectId": "{{ app.id }}"
      },
      "zones": [
        "us-central1-a", "us-central1-b", "europe-west1-b", "europe-west1-b"
      ],
      "fields": "nextPageToken,items(status,kind,machineType,name,zone,tags(items,fingerprint),disks(deviceName,kind,index,boot,source,mode,type),metadata(items(value,key),kind,fingerprint),selfLink,scheduling(automaticRestart,onHostMaintenance),canIpForward,serviceAccounts(scopes,email),networkInterfaces(accessConfigs(kind,type,name,natIP),networkIP,network,name),creationTimestamp,id,statusMessage,description),kind,id,selfLink"
    },
    {
      "type": "GceDisksInput",
      "apiInput": {
        "projectId": "{{ app.id }}"
      },
      "zones": [
        "us-central1-a", "us-central1-b", "europe-west1-b", "europe-west1-b"
      ],
      "fields": "nextPageToken,items(status,sourceSnapshot,description,sourceSnapshotId,creationTimestamp,id,kind,name,sizeGb,zone,options,selfLink,sourceImage,sourceImageId),kind,id,selfLink"
    }
  ],
  "transforms": [
    {
      "type": "GceDataTransformer"
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      "destinationTable": {
        "projectId": "{{ app.id }}",
        "datasetId": "cloud_history",
        "tableId": "Instances"
      },
      "sourceFormat": "NEWLINE_DELIMITED_JSON",
      "createDisposition": "CREATE_IF_NEEDED",
      "writeDisposition": "WRITE_APPEND",
      "schema": {
        "fields": [
          { "name": "snapshotStartTime", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "snapshotEndTime", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "snapshotId", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "status", "type": "STRING", "mode": "REQUIRED" },
          { "name": "statusMessage", "type": "STRING", "mode": "NULLABLE" },
          { "name": "kind", "type": "STRING", "mode": "REQUIRED" },
          { "name": "machineType", "type": "STRING", "mode": "REQUIRED" },
          { "name": "machineTypeName", "type": "STRING", "mode": "REQUIRED" },
          { "name": "image", "type": "STRING", "mode": "NULLABLE" },
          { "name": "imageName", "type": "STRING", "mode": "NULLABLE" },
          { "name": "name", "type": "STRING", "mode": "REQUIRED" },
          { "name": "zone", "type": "STRING", "mode": "REQUIRED" },
          { "name": "zoneName", "type": "STRING", "mode": "REQUIRED" },
          { "name": "tags", "type": "RECORD", "mode": "REQUIRED", "fields":
            [{ "name": "items", "type": "RECORD", "mode": "REPEATED", "fields":
                [{ "name": "item", "type": "STRING", "mode": "REQUIRED" }]
             },
             { "name": "fingerprint", "type": "STRING", "mode": "REQUIRED" }
            ]},
          { "name": "disks", "type": "record", "mode": "REPEATED", "fields":
            [{ "name": "deviceName", "type": "STRING", "mode": "NULLABLE" },
             { "name": "kind", "type": "STRING", "mode": "REQUIRED" },
             { "name": "index", "type": "INTEGER", "mode": "REQUIRED" },
             { "name": "boot", "type": "BOOLEAN", "mode": "NULLABLE" },
             { "name": "source", "type": "STRING", "mode": "NULLABLE" },
             { "name": "sourceName", "type": "STRING", "mode": "NULLABLE" },
             { "name": "mode", "type": "STRING", "mode": "REQUIRED" },
             { "name": "type", "type": "STRING", "mode": "REQUIRED" }
            ]},
          { "name": "canIpForward", "type": "BOOLEAN", "mode": "REQUIRED" },
          { "name": "serviceAccounts", "type": "RECORD", "mode": "REPEATED", "fields":
            [{ "name": "scopes", "type": "STRING", "mode": "REQUIRED" },
             { "name": "email", "type": "STRING", "mode": "REQUIRED" }
            ]},
          { "name": "metadata", "type": "RECORD", "mode": "REQUIRED", "fields":
            [{ "name": "kind", "type": "STRING", "mode": "REQUIRED" },
             { "name": "fingerprint", "type": "STRING", "mode": "REQUIRED" },
             { "name": "items", "type": "RECORD", "mode": "REPEATED", "fields":
               [{ "name": "key", "type": "STRING", "mode": "REQUIRED" },
                { "name": "value", "type": "STRING", "mode": "REQUIRED" }
               ]}
            ]},
          { "name": "scheduling", "type": "RECORD", "mode": "NULLABLE", "fields":
            [{ "name": "automaticRestart", "type": "BOOLEAN", "mode": "NULLABLE" },
             { "name": "onHostMaintenance", "type": "STRING", "mode": "NULLABLE" }
            ]},
          { "name": "networkInterfaces", "type": "RECORD", "mode": "REPEATED", "fields":
            [{ "name": "network", "type": "STRING", "mode": "REQUIRED" },
             { "name": "networkName", "type": "STRING", "mode": "REQUIRED" },
             { "name": "accessConfigs", "type": "RECORD", "mode": "REPEATED", "fields":
               [{ "name": "kind", "type": "STRING", "mode": "REQUIRED" },
                { "name": "type", "type": "STRING", "mode": "REQUIRED" },
                { "name": "name", "type": "STRING", "mode": "REQUIRED" },
                { "name": "natIP", "type": "STRING", "mode": "REQUIRED" }
               ]},
             { "name": "networkIP", "type": "STRING", "mode": "REQUIRED" },
             { "name": "name", "type": "STRING", "mode": "REQUIRED" }
            ]},
          { "name": "creationTimestamp", "type": "STRING", "mode": "REQUIRED" },
          { "name": "id", "type": "STRING", "mode": "REQUIRED" },
          { "name": "selfLink", "type": "STRING", "mode": "REQUIRED" },
          { "name": "description", "type": "STRING", "mode": "NULLABLE" }
        ]
      }
    },
    {
      "type": "BigQueryOutput",
      "destinationTable": {
        "projectId": "{{ app.id }}",
        "datasetId": "cloud_history",
        "tableId": "Disks"
      },
      "sourceFormat": "NEWLINE_DELIMITED_JSON",
      "createDisposition": "CREATE_IF_NEEDED",
      "writeDisposition": "WRITE_APPEND",
      "schema": {
        "fields": [
          { "name": "snapshotStartTime", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "snapshotEndTime", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "snapshotId", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "kind", "type": "STRING", "mode": "REQUIRED" },
          { "name": "id", "type": "STRING", "mode": "REQUIRED" },
          { "name": "creationTimestamp", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "zone", "type": "STRING", "mode": "REQUIRED" },
          { "name": "zoneName", "type": "STRING", "mode": "REQUIRED" },
          { "name": "status", "type": "STRING", "mode": "REQUIRED" },
          { "name": "name", "type": "STRING", "mode": "REQUIRED" },
          { "name": "description", "type": "STRING", "mode": "NULLABLE" },
          { "name": "sizeGb", "type": "INTEGER", "mode": "NULLABLE" },
          { "name": "sourceSnapshot", "type": "STRING", "mode": "NULLABLE" },
          { "name": "sourceSnapshotName", "type": "STRING", "mode": "NULLABLE" },
          { "name": "sourceSnapshotId", "type": "STRING", "mode": "NULLABLE" },
          { "name": "sourceImage", "type": "STRING", "mode": "NULLABLE" },
          { "name": "sourceImageName", "type": "STRING", "mode": "NULLABLE" },
          { "name": "sourceImageId", "type": "STRING", "mode": "NULLABLE" },
          { "name": "options", "type": "STRING", "mode": "NULLABLE" },
          { "name": "selfLink", "type": "STRING", "mode": "REQUIRED" }
        ]
      }
    }
  ]
}
```

The `GceInstancesInput` stage gets the instances data from Compute Engine
for the given zones using the *[Instances: list]* API. The fields parameter is
a filter for the Compute Engine API. It specifies which fields should be
returned from the list instances API call. The `GceDisksInput` stage is very
similar, which get the disks data using the *[Disks: list]* API.

Note that none of the stages specify the sources or sinks location. Data
Pipeline will automatically generate a temporary object in GCS and inject
the missing parameters into each stage's configuration. For example, when the
`GceInstancesInput` or the `GceDisksInput` run, it will each have the URL of
this temporary GCS object as a sink, and when `GceDataTransformer` runs, it
will have the two GCS objects as the sources and two GCS objects as the sinks.
Data Pipeline will map the sink from `GceInstancesInput` through the transform
stage and eventually as the source for the first `BigqueryOutput` and the sink
from `GceDisksInput` to the second `BigqueryOutput`.

The `BigQueryOutput` stage specifies the destintaion BigQuery table and the
corresponding schema. Instances and disks data are loaded into separate tables.

#### Compute Engine zone operations pipeline

This sample pipeline included in this package is called
*[operations_history_pipeline.json]*. This pipeline reads the current Google
Compute Engine zone operations data using the Compute Engine API interface.
The data is then transformed to a JSON format that is compatible with BigQuery
before loading into BigQuery.

Let's take a closer look of the pipeline specification:

```json
{
  "inputs": [
    {
      "type": "GceZoneOperationsInput",
      "destinationTable": {
            "projectId": "{{ app.id }}",
            "tableId": "zoneoperations",
            "datasetId": "cloud_history"
          },
      "zones": [
        "us-central1-a",
        "us-central1-b",
        "europe-west1-b",
        "europe-west1-b"
      ],
      "fields": "nextPageToken,items(targetId,clientOperationId,creationTimestamp,id,zone,insertTime,httpErrorMessage,progress,httpErrorStatusCode,statusMessage,status,operationType,warnings(message,code,data(value,key)),targetLink,startTime,kind,name,region,error(errors(message,code,location)),endTime,selfLink,user),kind,id,selfLink"
    }
  ],
  "transforms": [
    {
      "type": "GceDataTransformer"
    }
  ],
  "outputs": [
    {
      "type": "BigQueryOutput",
      "destinationTable": {
        "projectId": "{{ app.id }}",
        "datasetId": "cloud_history",
        "tableId": "zoneoperations"
      },
      "sourceFormat": "NEWLINE_DELIMITED_JSON",
      "createDisposition": "CREATE_IF_NEEDED",
      "writeDisposition": "WRITE_APPEND",
      "schema": {
        "fields": [
          { "name": "snapshotStartTime", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "snapshotEndTime", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "snapshotId", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "resourceType", "type": "STRING", "mode": "REQUIRED" },
          { "name": "operationType", "type": "STRING", "mode": "REQUIRED" },
          { "name": "kind", "type": "STRING", "mode": "REQUIRED" },
          { "name": "id", "type": "STRING", "mode": "REQUIRED" },
          { "name": "creationTimestamp", "type": "TIMESTAMP", "mode": "NULLABLE" },
          { "name": "name", "type": "STRING", "mode": "REQUIRED" },
          { "name": "zone", "type": "STRING", "mode": "NULLABLE" },
          { "name": "zoneName", "type": "STRING", "mode": "NULLABLE" },
          { "name": "clientOperationId", "type": "STRING", "mode": "NULLABLE" },
          { "name": "targetLink", "type": "STRING", "mode": "REQUIRED" },
          { "name": "targetLinkName", "type": "STRING", "mode": "REQUIRED" },
          { "name": "targetId", "type": "STRING", "mode": "NULLABLE" },
          { "name": "status", "type": "STRING", "mode": "NULLABLE" },
          { "name": "statusMessage", "type": "STRING", "mode": "NULLABLE" },
          { "name": "user", "type": "STRING", "mode": "REQUIRED" },
          { "name": "progress", "type": "INTEGER", "mode": "REQUIRED" },
          { "name": "insertTime", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "startTime", "type": "TIMESTAMP", "mode": "REQUIRED" },
          { "name": "endTime", "type": "TIMESTAMP", "mode": "NULLABLE" },
          { "name": "error", "type": "RECORD", "mode": "NULLABLE", "fields":
            [{ "name": "errors", "type": "RECORD", "mode": "REPEATED", "fields":
               [{ "name": "code", "type": "STRING", "mode": "REQUIRED" },
                { "name": "location", "type": "STRING", "mode": "NULLABLE" },
                { "name": "message", "type": "STRING", "mode": "NULLABLE" }
               ]
             }]},
          { "name": "warnings", "type": "RECORD", "mode": "REPEATED", "fields":
            [{ "name": "code", "type": "STRING", "mode": "REQUIRED" },
             { "name": "message", "type": "STRING", "mode": "NULLABLE" },
             { "name": "data", "type": "RECORD", "mode": "REPEATED", "fields":
               [{ "name": "key", "type": "STRING", "mode": "REQUIRED" },
                { "name": "value", "type": "STRING", "mode": "REQUIRED" }
               ]}
            ]},
          { "name": "httpErrorStatusCode", "type": "INTEGER", "mode": "NULLABLE" },
          { "name": "httpErrorMessage", "type": "STRING", "mode": "NULLABLE" },
          { "name": "selfLink", "type": "STRING", "mode": "REQUIRED" },
          { "name": "region", "type": "STRING", "mode": "NULLABLE" }
        ]
      }
    }
  ]
}
```

The `GceZoneOperationsInput` stage gets the operation data from Compute Engine
for the specified zones using the *[ZoneOperations: list]* API. For zone
operations, we do not want to load duplicate data into the BigQuery table.
Hence, the destination is required for the input stage so
`GceZoneOperationsInput` can query the existing set of operations that
already existed in the table. The fields parameter is a filter for the Compute
Engine API. It specifies which fields should be returned from the list
operations API call.

## Installation Instructions

### Create the pipelines

Follow the Data Pipeline instructions to create the pipeline for instances/disks
data using *[cloud_history_pipeline.json]* and the pipeline for zone operation
data using *[operations_history_pipeline.json]*.

### Set up App Engine cron service

For each of the pipeline that you created, copy the URL to run the pipeline from
the 'Run URL' link. The url should be in following format. The pipeline
variables, if defined, will be appended as query parameters.

/run/cloud-history/&lt;unique id&gt;

Change directory to the directory where you downloaded the data pipeline
package.  Create a cron.yaml in your `app` directory (along with app.yaml).
Use the following as a template and replace with your values.

```txt
cron:
- description: instances and disks data
  url: <Run URL for instances and disks data>
  schedule: every 15 minutes
  target: <YOUR APP VERSION>

- description: operations data
  url: <Run URL for operations data>
  schedule: every day 06:00
  timezone: America/Los_Angeles
  target: <YOUR APP VERSION>
```

Update the cron configuration while you are still in your `app` directory:

```shell
appcfg.py update_cron . --oauth2
```

## Cloud Deployment History Analysis

After the pipeline is set up, you can use the *[BigQuery Web User Interface]*
for analysis. Make sure that your project where you loaded the data is the
current project.  You should see the cloud_history dataset. Within the
dataset, there should be three tables - Disks, Instances and zoneoperations.
The following section provides sample queries for your reference.

## <a name="cookbook"></a> Sample Query Cookbook

### Instances
#### What are the instances with production tag T at time range X ?
```txt
SELECT snapshotId as dateTime, name, machineTypeName, zoneName, tags.items.item
FROM [cloud_history.Instances]
WHERE
  tags.items.item = '<INSTANCE TAG>' AND
  (snapshotId >= TIMESTAMP("YYYY-MM-DD hh:mm:ss") AND
   snapshotId <= TIMESTAMP("YYYY-MM-DD hh:mm:ss"))
```

#### What are the instances in stopped/terminated state at time range X ?
```txt
SELECT snapshotId, zoneName, name
FROM [cloud_history.Instances]
WHERE
  status = 'STOPPED' OR status = 'TERMINATED' AND
  (snapshotId >= TIMESTAMP("YYYY-MM-DD hh:mm:ss") AND
   snapshotId <= TIMESTAMP("YYYY-MM-DD hh:mm:ss"))
```

#### What is the number of instances by tags for the past 7 days ?
```txt
SELECT snapshotId, tags.items.item, count(name) as num_instances
FROM [cloud_history.Instances]
WHERE
  status != 'STOPPED' AND status != 'TERMINATED' AND
  tags.items.item != "null" AND
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -7, "DAY") AND
  snapshotId <= CURRENT_TIMESTAMP()
GROUP BY snapshotId, tags.items.item
ORDER BY snapshotId, tags.items.item
```

#### What is the distribution of instances across zone for the past month ?
```txt
SELECT snapshotId, zoneName, count(name) as instance_count
  FROM [cloud_history.Instances]
WHERE
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -1, "MONTH") AND
  snapshotId <= CURRENT_TIMESTAMP()
GROUP BY
  snapshotId, zoneName
```

#### What are the instances on specific networks at time X ?
```txt
SELECT snapshotId, networkInterfaces.networkName as networkName, name
FROM [cloud_history.Instances]
WHERE
  snapshotId == TIMESTAMP("YYYY-MM-DD hh:mm:ss")
ORDER BY snapshotId, networkName, name
```

#### What are all the instances which have used IP X ?
```txt
SELECT name
  FROM [cloud_history.Instances]
WHERE
  networkInterfaces.accessConfigs.natIP = 'xx.xx.xx.xx' AND
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -7, "DAY") AND
  snapshotId <= CURRENT_TIMESTAMP()
GROUP BY name
```

#### What are all the instances which do not have an external IP at time X ?
```txt
SELECT snapshotId, name
  FROM [cloud_history.Instances]
WHERE
  snapshotId == TIMESTAMP("YYYY-MM-DD hh:mm:ss")
GROUP BY snapshotId, name
HAVING
  count(networkInterfaces.accessConfigs.natIP) == 0
```

#### What is the number of instances over time ?
```txt
SELECT snapshotId, COUNT(name)
FROM [cloud_history.Instances]
WHERE
  status != 'STOPPED' AND status != 'TERMINATED' AND
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -7, "DAY") AND
  snapshotId <= CURRENT_TIMESTAMP()
GROUP BY
  snapshotId
ORDER BY
  snapshotId ASC
```

#### What is the daily average number of instances per zone over time ?
```txt
SELECT date, zoneName, SUM(count) / COUNT(count) as AVG
FROM
  (SELECT DATE(snapshotId) date, snapshotId, zoneName, count(name) count
   FROM [cloud_history.Instances]
   GROUP BY
     date, snapshotId, zoneName
   ORDER BY date, zoneName)
GROUP BY date, zoneName
ORDER BY date;
```

#### What is the daily maximum and minimax number of instances for a given zone over time ?
```txt
SELECT A.date date, low, high
FROM
 (SELECT date, instances low
  FROM
   (SELECT date, instances, RANK() OVER (PARTITION BY date ORDER BY instances ASC) rank
    FROM
     (SELECT DATE(snapshotId) date, snapshotId, count(name) instances
      FROM [cloud_history.Instances]
      WHERE zoneName = 'xx-xxxxxxxx-x'
      GROUP BY date, snapshotId
      ORDER BY snapshotId)
   )
  WHERE rank = 1
  GROUP BY date, low
 ) A
JOIN
 (SELECT date, instances high
  FROM
   (SELECT date, instances, RANK() OVER (PARTITION BY date ORDER BY instances DESC) rank
    FROM
     (SELECT DATE(snapshotId) date, snapshotId, count(name) instances
      FROM [cloud_history.Instances]
      WHERE zoneName = 'xx-xxxxxxxx-x'
      GROUP BY date, snapshotId
      ORDER BY snapshotId)
   )
  WHERE rank = 1
  GROUP BY date, high
 ) B
ON A.date = B.date
```

### Disks

#### What is the aggregated size of disks by zone for the past 7 days ?
```txt
SELECT zoneName, snapshotId, SUM(sizeGb) totalSiz
FROM [cloud_history.Disks]
WHERE
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -7, "DAY") AND
  snapshotId <= CURRENT_TIMESTAMP()
GROUP BY zoneName, snapshotId
ORDER BY zoneName, snapshotId
```

#### What is the disk configuration for the past 7 days ?
```txt
SELECT zoneName, snapshotId, SUM(sizeGb) totalSiz
FROM [cloud_history.Disks]
WHERE
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -7, "DAY") AND
  snapshotId <= CURRENT_TIMESTAMP()
GROUP BY zoneName, snapshotId
ORDER BY zoneName, snapshotId
```

#### What are the disks that are unused/unattached at time X ?
```txt
SELECT D.name, D.zoneName
FROM [cloud_history.Disks] D
LEFT OUTER JOIN FLATTEN([cloud_history.Instances], disks) I
  ON D.name = I.disks.deviceName AND D.snapshotId = I.snapshotId
WHERE
  I.disks.deviceName IS NULL AND
  D.snapshotId = TIMESTAMP("YYYY-MM-DD hh:mm:ss")
```

### Operations

#### Who created/deleted a disk / an instance / a firewall X ?
```txt
SELECT startTime, user, operationType
FROM [cloud_history.zoneoperations]
WHERE
  targetLink CONTAINS '/disks/' and targetLinkName = '<disk-name>'

* /disks/ can be replaced by instances or firewalls.
```

#### List the operations history for a zone resource.
```txt
SELECT startTime, user, operationType, targetLinkName
FROM [cloud_history.zoneoperations]
WHERE
  targetLink contains '/disks/' AND
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -7, "DAY") AND
  snapshotId <= CURRENT_TIMESTAMP()

* /disks/ can be replaced by instances.
```

#### What is the aggregated count of resources modified by all users ?
```txt
SELECT user, count(operationType) as num_operations
FROM [cloud_history.zoneoperations]
WHERE
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -7, "DAY") AND
  snapshotId <= CURRENT_TIMESTAMP()
GROUP BY user
ORDER BY num_operations DESC
```

#### What is the aggregated count of each resource modified by a given user ?
```txt
SELECT user, operationType,
  REGEXP_EXTRACT(targetLink,   r'.*\/zones\/[a-z0-9A-Z\-]+\/(.*)\/.*$') as resource,
  COUNT(*) as count
FROM [cloud_history.zoneoperations]
WHERE
  user = "xxxxx@xxxxxx.com" AND
  snapshotId >= DATE_ADD(CURRENT_TIMESTAMP(), -7, "DAY") AND
  snapshotId <= CURRENT_TIMESTAMP()
GROUP BY
  user, operationType, resource
```

[App Engine Cron Service]: https://developers.google.com/appengine/docs/python/config/cron
[BigQuery Web User Interface]: https://bigquery.cloud.google.com
[Disks: list]: https://developers.google.com/compute/docs/reference/latest/disks/list
[Instances: list]: https://developers.google.com/compute/docs/reference/latest/instances/list
[ZoneOperations: list]: https://developers.google.com/compute/docs/reference/latest/zoneOperations/list
[cloud_history_pipeline.json]: /static/examples/cloud_history_pipeline.json
[operations_history_pipeline.json]: /static/examples/operations_history_pipeline.json
