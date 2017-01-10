#!/bin/bash

###################
# Simple script that moves the device directories to a backup folder
# so that the pipelines can re-run. The device pipelines are all clean runs, so
# they simply look for the directory to determine if they should run or not.
# In contrast, the drug pipelines are incremental, so there is no need to move
# them.
##################

set -x

export DATA="$HOME/openfda-internal/data"
export BACKUP="$DATA/device_backup"

DATE_STR=$(date +"%Y-%m-%d-%H-%M")
CURRENT="$BACKUP/$DATE_STR"
mkdir -p $CURRENT

DEVICES="510k classification device_harmonization device_pma device_recall maude registration udi"

for DEVICE in $DEVICES
do
  if [ -d "$DATA/$DEVICE" ]
  then
    echo "Moving $DEVICE to $CURRENT"
    mv "$DATA/$DEVICE" $CURRENT
  else
    echo "No need to move $DEVICE, it does not exist"
  fi
done

