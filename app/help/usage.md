# Data Pipeline Solution

## Usage

Data pipelines are configured using a JSON format configuration file that you
create and edit in the application UI. As you are editing the configuration
document, it will be linted to check it is both syntactically correct and each
stage's preconditions are met. Once saved, clicking the `Run` button will launch
the execution of the pipeline and a new window will open, showing the progress
of the running pipeline.

![Application Screenshot](/static/img/help/image_2.png)

The application's main page shows a list of your stored data pipelines. You will
also notice a gear in the top right corner; clicking it will open a pop-up
window where you can enter some default options.

![Example Pipeline Screenshot](/static/img/help/image_3.png)

Clicking on a data pipeline in your list will open its configuration in an
editor element. Notice at the bottom of the page, the status indicates this
configuration has been successfully linted and may be run.

![Pipeline Linting Failure Screenshot](/static/img/help/image_4.png)

However, if your configuration is invalid, for example in this case, we've
misspelled the `gql` field, the status will provide more context to help you fix
the problem. Also notice that the `Save` and `Run` buttons are now disabled.

### Configuration

Configurations are simple JSON documents with a slight enhancement that they
support `//` and `/**/` style comment blocks. If you want to keep your files as
pure JSON you can also add

comments by adding various dummy keys to dictionary objects:

```json
{
  "#": "this is a comment that nothing will use.",
  "type": "GcsInput",
}
```


The most common error when writing JSON files is to add an extra comma after the
last value in a dictionary. For example in the previous example about comments,
note the extra "," after `GcsInput`. That's a syntax error.

The `options` dictionary contains configuration values for the pipeline. These
can be used to override the default settings which are set in the application by
clicking on the gear at the top right.

#### Sources and sinks

Each stage has `sources` and `sinks` fields in its configuration. Usually these
are not specified in the data pipeline configuration but they certainly can be.
If you do not specify them then the Data Pipeline `Runner` class will create new
GCS object URLs and connect the sink from one stage to the source of the next
stage in the data pipeline automatically. In some cases, you might have a stage
that will not use either a source or a sink. You can explicitly disable the
creation of these temporary files by explicitly declaring `sources` or `sinks`
as `null`:

```json
{
  "type": "NoSinkRequiredStage",
  "sinks": null
}
```


Note that something like the following will generate a linting error:

```json
{
  "type": "NoSinkRequiredStage",
  "sinks": [null]
}
```


#### Data pipeline variables

The following variables are available to use in your data pipeline
configurations. Simply enclose them in double braces like this:

```json
{{ app.id }}
```


* `app.id`: the App Engine application id.

* `app.hostname`: the hostname of this App Engine Application.

* `app.serviceAccountName`: the email address of the service account for the app.

* `storage.bucket`:

* `storage.prefix`:

* `storage.url`:

* `date.y-m-d`: today's date in YYYY-mm-dd format.

* `date.ymd`: today's date in YYYYmmdd format.

These variables are added in the `GetTemplateVariables` method
in `src/pipelines/linter.py` file.

You can also create your own variables in your pipeline config. These
can be useful for choosing regions or specifying output files. Use the
regular double brace syntax and make up new variable names. The UI
will recognize this and provide an area for you to enter variable
values which will be passed as CGI parameters to the run url.


### Running the pipeline

You can run a data pipeline by clicking the `Run` button after selecting it in
the UI. Recall that `Run` in only available if the configuration is valid and
passes a lint check. If the `Run` or `Save` buttons are disabled, check the
bottom of the page for lint errors.

You can also copy the `Run URL` and use that to start your data
pipeline. The `Run URL` contains a random `api_key` (automatically
generated on first save) and does not require any authentication to
access. Once the pipeline is started, a new tab will open and show its
progress. You might want to hit the `Run URL` from a `cron.yaml`
entry, regular `cron` job using `wget` or `curl` or a
[Google Cloud Storage Object Change Notification].

Data Pipeline makes use of [Google App Engine Modules]
to run the stages on a cluster of instances. These instances have fewer
restrictions than regular front-ends in terms of how long they can take to
process requests.

### Adding your own stages

You can create your own stage by adding a new file in
*app/src/pipelines/stages*. The filename should be the lowercase version of your
stage name. There should be a class defined in the file that has the same name
as your stage name and inherits from `pipeline.Pipeline`.

For example, *app/src/pipelines/stages/mystage.py* might look like:

```json
class MyStage(pipeline.Pipeline):
  """MyStage data processing stage."""

  @staticmethod
  def GetHelp():
    return """Process data according to my very own rules.

This stage does some magic custom processing or transforms.

The stage config should look like this:
``json
{
  "type": "MyStage",
  "anyOptions": "option value"
}
``

* Notes on the options go here
"""

  def Lint(self, results):
    linter.FieldCheck('anyOptions', required=True)

  def run(self, config):
    # here is where you look at config and do your work.
```


`GetHelp` is an optional static method you define to return the help
documentation for your stage.  It will be added automatically to the
application's help section about stages. The help text should be in
[GitHub Flavored Markdown] format. If you do not provide the `GetHelp`
method then the *docstring* for the class will be used, failing that
the module *docstring* will be used. **NOTE** in this example we use
double backticks around the example stage config but in your code you
should use standard markdown triple backticks.

`Lint` is another method you may optionally define to help verify that a
configuration for your module is correct. Various utility methods are available
on the results object passed in to allow you to check that configuration fields
are available and report back errors.

The `run` method is where you will do your processing, You can get the source
from `config.get('sources')[0]` and the sink in a similar manner. Now you can
read from and write to those files to process your data. Since Data Pipeline
uses the App Engine Pipeline library for workflow control, your run method can
yield additional Pipelines to be run.

### Sharding

If your pipeline stage can be easily split into smaller stages each doing
exactly the same work on various shards of the input data you can inherit from
the `src.pipelines.ShardStage` class. This class has utility methods to
automatically split up work into shards, run them all in parallel and then
compose the resulting files into a final sink file.

### Data Sizes

When you are processing a large amount of data, it's good to
understand what is going on under the hood. If the work is shardable
then the input file will be processed in chunks and each processing
will produce a smaller chunk of processed data that is then
[composited] into a single GCS file.

Due to the limitations of compositing files in GCS if your work is
split into too many chunks there will be a significant performance hit
while Data Pipeline builds these back into a file for the next stage.

Data Pipeline has successfully processed files as large as 5GB but has
not yet had success with 100GB files.

### Debugging

If something goes wrong, here are the steps to help find out what the problem
might be. For UI issues open up the web developer javascript console
(cmd-option-j in Chrome on a Mac). There might be interesting errors there. You might
also want to check the App Engine logs. For problems with a running pipeline you
need to go to the backend module in the logs.



[src/pipelines/linter.py]: /app/src/pipelines/linter.py
[Google Cloud Storage Object Change Notification]: https://developers.google.com/storage/docs/object-change-notification
[Google App Engine Modules]: https://developers.google.com/appengine/docs/python/modules/
[GitHub Flavored Markdown]: http://github.github.com/github-flavored-markdown/
[composited]: https://developers.google.com/storage/docs/composite-objects
