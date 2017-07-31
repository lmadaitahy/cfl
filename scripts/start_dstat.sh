#!/bin/bash

# https://stackoverflow.com/questions/3510673/find-and-kill-a-process-in-one-line-using-bash-and-regex

# See https://stackoverflow.com/questions/1250079/how-to-escape-single-quotes-within-single-quoted-strings
# for the crazy quotation.

for i in `./machines.sh`
do

  echo $i

  #ssh cloud-$i 'ps aux |grep dstat |grep -v grep |grep -v kill-all-dstat |awk '"'"'{print $2}'"'"' |xargs --verbose --no-run-if-empty kill -9'
  
  ssh $i "mkdir -p /tmp/ggevay/dstat_out ; rm /tmp/ggevay/dstat_out/$i 2>/dev/null ; /share/hadoop/peel/systems/dstat-0.7.3/dstat --epoch -c --nocolor --noheaders --output /tmp/ggevay/dstat_out/$i >/dev/null &" &

done

wait
