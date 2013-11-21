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


declare -r HADOOP_USER_ID=5555
declare -r HADOOP_GROUP_ID=5555

# When this start-up script is finished, it writes the result (success/failure)
# into the "done" file, so that start-up script finish status can be
# easily checked from the main script.
declare -r DONE_FILE=/var/log/STARTUP_SCRIPT_DONE

function die() {
  # Message to STDERR goes to start-up script log in the instance.
  echo
  echo "########## ERROR ##########"
  echo "$@"
  echo "###########################"

  # The error message in DONE_FILE will be displayed on the console.
  echo "failure" > $DONE_FILE
  echo "########## START-UP SCRIPT ERROR ##########" >> $DONE_FILE
  echo "$@" >> $DONE_FILE
  echo "###########################################" >> $DONE_FILE
  chmod 0644 $DONE_FILE

  exit 1
}

declare -r METADATA_ROOT=http://metadata/computeMetadata/v1beta1

function get_metadata_value() {
  name=$1
  echo $(curl --silent -f $METADATA_ROOT/instance/attributes/$name)
}

EXTERNAL_IP=$(curl --silent -f  \
    $METADATA_ROOT/instance/network-interfaces/0/access-configs/0/external-ip)

# Increase fd limit, since Hadoop uses many connections among nodes.
ulimit -n 32768
echo hadoop soft nofile 32768 >> /etc/security/limits.conf
echo hadoop hard nofile 32768 >> /etc/security/limits.conf

declare -r HADOOP_ROOT=/hadoop

# Get disk ID from metadata and decide which device to mount for Hadoop data.
# It might be persistent or scratch disk.
declare -r DISK_DEVICE=/dev/disk/by-id/$(get_metadata_value disk-id)

# Make mount point.
mkdir -p $HADOOP_ROOT

/usr/share/google/safe_format_and_mount -m "mkfs.ext4 -F"  \
    $DISK_DEVICE $HADOOP_ROOT

if [ -d $HADOOP_ROOT/hdfs ] ; then
  # hdfs directory already exists.  The disk was already formatted.
  FIRST_TIME_MOUNT=0
else
  FIRST_TIME_MOUNT=1
fi

# Set up user and group
# Since Hadoop uses SSH for the communication between master and workers,
# we need to have the same UID/GID values among all machines in the cluster.
# To ensure this, explicitly specify UID and GID.
groupadd --gid $HADOOP_GROUP_ID hadoop
useradd --uid $HADOOP_USER_ID --gid hadoop --shell /bin/bash -m hadoop

# Prepare directories
mkdir -p $HADOOP_ROOT/hdfs/name
mkdir -p $HADOOP_ROOT/hdfs/data
mkdir -p $HADOOP_ROOT/checkpoint
mkdir -p $HADOOP_ROOT/mapred/history

chown -R hadoop:hadoop $HADOOP_ROOT
chmod -R 755 $HADOOP_ROOT

mkdir -p /run/hadoop
chown hadoop:hadoop /run/hadoop
chmod g+w /run/hadoop

declare -r HADOOP_LOG_DIR=/var/log/hadoop
mkdir -p $HADOOP_LOG_DIR
chgrp hadoop $HADOOP_LOG_DIR
chmod g+w $HADOOP_LOG_DIR

HOSTNAME_PREFIX=$(get_metadata_value 'hostname-prefix')
NUM_WORKERS=$(get_metadata_value 'num-workers')
HADOOP_MASTER=$(get_metadata_value 'hadoop-master')
WORKER_NAME_TEMPLATE=$(get_metadata_value 'hadoop-worker-template')
HADOOP_PATCH=$(get_metadata_value 'hadoop-patch')
TMP_CLOUD_STORAGE=$(get_metadata_value 'tmp-cloud-storage')
CUSTOM_COMMAND=$(get_metadata_value 'custom-command')

declare -r TMP_DIR=/tmp/hadoop_package
declare -r HADOOP_DIR=hadoop-*
declare -r GENERATED_FILES_DIR=generated_files
declare -r DEB_PACKAGE_DIR=deb_packages

declare -r HADOOP_HOME=/home/hadoop
declare -r SCRIPT_DIR=hadoop_scripts
declare -r RPC_DAEMON=rpc-daemon.zip

mkdir -p $TMP_DIR

# Download packages from Cloud Storage.
gsutil -m cp $TMP_CLOUD_STORAGE/$HADOOP_DIR.tar.gz  \
             $TMP_CLOUD_STORAGE/$GENERATED_FILES_DIR.tar.gz  \
             $TMP_CLOUD_STORAGE/$HADOOP_PATCH  \
             $TMP_CLOUD_STORAGE/$SCRIPT_DIR.tar.gz  \
             $TMP_CLOUD_STORAGE/$RPC_DAEMON  \
             $TMP_DIR ||  \
    die "Failed to download Hadoop and generated files packages from "  \
        "$TMP_CLOUD_STORAGE/"

tar zxf $TMP_DIR/$GENERATED_FILES_DIR.tar.gz -C $TMP_DIR ||  \
    die "Failed to extract generated files"
chown $(id -u):$(id -g) -R $TMP_DIR/$GENERATED_FILES_DIR
chmod o+rx $TMP_DIR/$GENERATED_FILES_DIR
chmod o+r $TMP_DIR/$GENERATED_FILES_DIR/*
chmod o+x $TMP_DIR/$GENERATED_FILES_DIR/ssh-key
chmod o+r $TMP_DIR/$GENERATED_FILES_DIR/ssh-key/*
chmod a+r $TMP_DIR/$HADOOP_PATCH

# Unpack scripts for MapReduce.
tar zxf $TMP_DIR/$SCRIPT_DIR.tar.gz -C $TMP_DIR ||  \
    die "Failed to extract MapReduce script files"
chown $(id -u):$(id -g) -R $TMP_DIR/$SCRIPT_DIR
chmod a+rx $TMP_DIR/$SCRIPT_DIR/*

# Set up Java Runtime Environment.  Download .deb packages from Google Cloud
# Storage, and install them by dpkg command.
LOCAL_PACKAGE_DIR=$TMP_DIR/$DEB_PACKAGE_DIR
mkdir -p $LOCAL_PACKAGE_DIR
gsutil -m cp $TMP_CLOUD_STORAGE/$DEB_PACKAGE_DIR/*.deb $LOCAL_PACKAGE_DIR
dpkg -i --force-depends $LOCAL_PACKAGE_DIR/*.deb

SCRIPT_AS_HADOOP=$TMP_DIR/setup_as_hadoop.sh
cat > $SCRIPT_AS_HADOOP <<NEKO
# Exits if one of the commands fails.
set -o errexit

HADOOP_CONFIG_DIR=\$HOME/hadoop/conf

# Set up SSH keys
mkdir -p \$HOME/.ssh
cp -f $TMP_DIR/$GENERATED_FILES_DIR/ssh-key/* \$HOME/.ssh
mv -f \$HOME/.ssh/id_rsa.pub \$HOME/.ssh/authorized_keys
chmod 600 \$HOME/.ssh/id_rsa
chmod 700 \$HOME/.ssh

# Allow SSH between Hadoop cluster instances without user intervention.
echo "Host *" >> \$HOME/.ssh/config
echo "  StrictHostKeyChecking no" >> \$HOME/.ssh/config
chmod 600 \$HOME/.ssh/config

# Extract Hadoop package and apply patch to update configuration files.
tar zxf $TMP_DIR/$HADOOP_DIR.tar.gz -C \$HOME
patch -d \$HOME -f -p0 < $TMP_DIR/$HADOOP_PATCH
ln -s \$HOME/$HADOOP_DIR \$HOME/hadoop

# Create masters file.
echo $HADOOP_MASTER > \$HADOOP_CONFIG_DIR/masters

# Create slaves file.
rm -f \$HADOOP_CONFIG_DIR/slaves
for ((i = 0; i < $NUM_WORKERS; i++)) ; do
  printf "$WORKER_NAME_TEMPLATE\n" \$i >> \$HADOOP_CONFIG_DIR/slaves
done

# Overwrite Hadoop configuration files.
perl -pi -e "s/###HADOOP_MASTER###/$HADOOP_MASTER/g"  \
    \$HADOOP_CONFIG_DIR/core-site.xml  \
    \$HADOOP_CONFIG_DIR/mapred-site.xml

perl -pi -e "s/###EXTERNAL_IP_ADDRESS###/$EXTERNAL_IP/g"  \
    \$HADOOP_CONFIG_DIR/hdfs-site.xml  \
    \$HADOOP_CONFIG_DIR/mapred-site.xml

# Set PATH for hadoop user
echo "export PATH=\$HOME/hadoop/bin:\$HOME/hadoop/sbin:\\\$PATH" >>  \
    \$HOME/.profile
echo "export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64" >> \$HOME/.profile

NEKO

sudo -u hadoop bash $SCRIPT_AS_HADOOP ||  \
    die "Failed to run set-up command as hadoop user"

rm -rf $TMP_DIR/$GENERATED_FILES_DIR/ssh-key

# Run custom commands.
eval "$CUSTOM_COMMAND" || die "Custom command error: $CUSTOM_COMMAND"

# Error check in CreateTrackerDirIfNeeded() in gslib/util.py in gsutil 3.34
# (line 114) raises exception when called from Hadoop streaming MapReduce,
# saying permission error to create /homes.
perl -pi -e '$.>110 and $.<120 and s/raise$/pass/'  \
    /usr/local/share/google/gsutil/gslib/util.py

function run_as_hadoop() {
  failure_message=$1 ; shift

  sudo -u hadoop -i eval "$@" || die $failure_message
}

# Starts daemons if necessary.
function maybe_start_node() {
  condition=$1 ; shift
  failure_message=$1 ; shift

  if (( $(get_metadata_value $condition) )) ; then
    run_as_hadoop "$failure_message" $@
  fi
}

# Starts NameNode and Secondary NameNode.  Format HDFS if necessary.
function start_namenode() {
  echo "Prepare and start NameNode(s)"

  if (( $FIRST_TIME_MOUNT )) ; then
    run_as_hadoop "Failed to format HDFS"  \
        "echo 'Y' | hadoop namenode -format"
  fi

  # Start NameNode
  run_as_hadoop "Failed to start NameNode" hadoop-daemon.sh start namenode
  # Start Secondary NameNode
  run_as_hadoop "Failed to start Secondary NameNode" hadoop-daemon.sh start  \
      secondarynamenode
}

if (( $(get_metadata_value NameNode) )) ; then
  start_namenode
fi

maybe_start_node DataNode "Failed to start DataNode"  \
    hadoop-daemon.sh start datanode

maybe_start_node JobTracker "Failed to start JobTracker"  \
    hadoop-daemon.sh start jobtracker

maybe_start_node TaskTracker "Failed to start TaskTracker"  \
    hadoop-daemon.sh start tasktracker

echo
echo "Start-up script for Hadoop successfully finished."
echo

echo "success" > $DONE_FILE
chmod 0644 $DONE_FILE

# Start RPC daemon
echo "Start RPC daemon"
python $TMP_DIR/$RPC_DAEMON
