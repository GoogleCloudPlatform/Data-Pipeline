#!/bin/bash
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


declare -r MAPPER=$1 ; shift
declare -r MAPPER_COUNT=$1 ; shift
declare -r REDUCER=$1 ; shift
declare -r REDUCER_COUNT=$1 ; shift
declare -r INPUT_DIR=$1 ; shift
declare -r OUTPUT_DIR=$1 ; shift

declare -r HADOOP_DIR=hadoop
declare -r HADOOP_HOME=/home/hadoop
declare -r HADOOP_ROOT=/home/hadoop/$HADOOP_DIR
declare -r HADOOP_BIN=$HADOOP_ROOT/bin
declare -r MAPREDUCE_HOME=$HADOOP_HOME/mapreduce
declare -r SCRIPT_DIRECTORY=/tmp/hadoop_package/hadoop_scripts


function mapreduce() {
  local -r job_name=$1 ; shift
  local -r mapper=$1 ; shift
  local -r mapper_count=$1 ; shift
  local -r reducer=$1 ; shift
  local -r reducer_count=$1 ; shift
  local -r input_hdfs=$1 ; shift
  local -r output_hdfs=$1 ; shift

  local file_param

  # Set mapper and reducer as -file parameter if the files exist.
  if [[ -f $mapper ]] ; then
    file_param="$file_param -file $mapper"
  fi

  if [[ -f $reducer ]] ; then
    file_param="$file_param -file $reducer"
  fi

  echo
  echo ".... Starting MapReduce job ...."
  echo

  # Start MapReduce job
  local command="$HADOOP_BIN/hadoop  \
      jar $HADOOP_ROOT/contrib/streaming/hadoop-streaming-*.jar  \
          -D mapred.map.tasks=$mapper_count  \
          -D mapred.reduce.tasks=$reducer_count  \
          -D mapred.job.name=\"$job_name\"  \
          -input $input_hdfs -output $output_hdfs  \
          -mapper $mapper  \
          -reducer $reducer  \
          $file_param  \
          "
  echo "MapReduce command: $command"
  eval $command
}

function do_copy() {
  local -r name=$1 ; shift

  # Use larger of the MAPPER_COUNT and REDUCER_COUNT as mapper size
  # of the copy job.
  local parallel_count=$((MAPPER_COUNT > REDUCER_COUNT ?  \
                          MAPPER_COUNT : REDUCER_COUNT))

  # Initiate MapReduce for copy.
  mapreduce $name  \
      $SCRIPT_DIRECTORY/${name}_mapper.sh $parallel_count  \
      cat 0 $name/inputs $name/outputs

  # Combine results into one and output to stdout.
  echo
  echo "$name copy result"
  $HADOOP_BIN/hadoop dfs -cat "$name/outputs/part-*"
  echo
}

# Copies input files from GCS to HDFS with MapRecuce.
function gcs_to_hdfs() {
  local -r src_gfs=$1 ; shift
  local -r dst_hdfs=$1 ; shift
  local -r name=gcs_to_hdfs

  echo "Clear previous $name input/output."
  $HADOOP_BIN/hadoop dfs -rmr $name/inputs $name/outputs >/dev/null 2>&1

  # Prepare file list as input of GCS-to-HDFS copy MapReduce job.
  gsutil ls $src_gfs |  \
      perl -p -e "s|.*$src_gfs(.*)|\$&\t$dst_hdfs\$1|" |  \
      $HADOOP_BIN/hadoop dfs -put - $name/inputs/file-list
  do_copy $name
}

# Copies output files from HDFS to GCS with MapReduce.
function hdfs_to_gcs() {
  local -r src_hdfs=$1 ; shift
  local -r dst_gfs=$1 ; shift
  local -r name=hdfs_to_gcs

  echo "Clear previous $name input/output."
  $HADOOP_BIN/hadoop dfs -rmr $name/inputs $name/outputs >/dev/null 2>&1

  # Prepare file list as input of HDFS-to-GCS copy MapReduce job.
  # Exclude directories.
  $HADOOP_BIN/hadoop dfs -lsr $src_hdfs | grep -v ^d | awk '{print $8}' |  \
      perl -p -e "s|.*$src_hdfs/(.*)|\$&\t$dst_gfs/\$1|" |  \
      $HADOOP_BIN/hadoop dfs -put - $name/inputs/file-list
  do_copy $name
}

function main() {
  declare -r hdfs_input="inputs"
  declare -r hdfs_output="outputs"

  mkdir -p $MAPREDUCE_HOME

  echo "Clear previous input/output if any."
  $HADOOP_BIN/hadoop dfs -rmr $hdfs_input $hdfs_output >/dev/null 2>&1

  # Copy input
  gcs_to_hdfs $INPUT_DIR $hdfs_input
  # Perform MapReduce
  mapreduce $(basename $MAPPER) $MAPPER $MAPPER_COUNT $REDUCER $REDUCER_COUNT  \
      $hdfs_input $hdfs_output
  # Copy output
  hdfs_to_gcs $hdfs_output $OUTPUT_DIR
}

main
