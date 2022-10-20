#!/bin/bash

while read line
do
  echo "$line" >> $1
done < /dev/stdin

# we are here after all data related to the shard is read.

####################################
# csv to s3 uplaod logic goes here...
#####################################
