#!/bin/bash

# TODO(mattmo): Only do this for root
ulimit -n 64000
ulimit -l unlimited

# This is a simple "while(1)" script to restart an application if
# it exits unexpectedly.

(
while true; do
  echo "Running command $*"
  nohup $* 
  echo "Command exited with status $?"
  echo "Sleeping 10 seconds..."
  sleep 10
done
) &

disown %1
