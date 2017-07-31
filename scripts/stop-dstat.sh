#!/bin/bash


for i in `./machines.sh`
do

  #ssh $i 'ps aux |grep java |grep -v datanode |grep -v namenode |grep -v grep |grep -v kill-all-java |awk '"'"'{print $2}'"'"' |xargs --verbose --no-run-if-empty kill -9'

  ssh $i 'ps aux |grep dstat |grep -v grep |grep -v kill-all-dstat |awk '"'"'{print $2}'"'"' |xargs --verbose --no-run-if-empty kill'"; scp /tmp/ggevay/dstat_out/$i /share/hadoop/ggevay/dstat_out" &

done

wait
