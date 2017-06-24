#!/bin/bash

# Creates flink-conf for the specified number of machines. Uses the templates in my home dir.

tasksPerMachine=16

# There is this "Resource temporarily unavailable" problem, so retry (see https://stackoverflow.com/questions/5274294/how-can-you-run-a-command-in-bash-over-until-success)
# (This could be improved like this: https://stackoverflow.com/questions/5195607/checking-bash-exit-status-of-several-commands-efficiently)
while :
do
  sed s/gggpara/$(( $1*$tasksPerMachine ))/ /home/ggevay/flink-conf-templates/flink-conf.yaml >./flink-cfl/conf/flink-conf.yaml
  if [ $? -eq 0 ]; then
    break
  else
    echo "Trying again"
  fi
done

while :
do 
  head -n $1 /home/ggevay/flink-conf-templates/slaves >./flink-cfl/conf/slaves
  if [ $? -eq 0 ]; then
    break
  else
    echo "Trying again"
  fi
done


