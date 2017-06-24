#!/bin/bash

if [ `grep org.apache.flink.runtime.client.JobCancellationException $1 |wc -l` != 1 ] 
then
  printf "\n!!!!!!!!!!! Job failed (no JobCancellationException)\n"
  echo "Out file: $1"
  exit 71
fi

# One more check just to be sure
if [ `grep "Job execution failed" $1 |wc -l` != 0 ]
then
  printf "\n!!!!!!!!!!! Job failed (We have Job execution failed in the log)\n"
  echo "Out file: $1"
  exit 72
fi

#echo "Job result OK. (JobCancellationException)"
