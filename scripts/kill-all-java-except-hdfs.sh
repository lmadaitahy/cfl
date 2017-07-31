#!/bin/bash

# https://stackoverflow.com/questions/3510673/find-and-kill-a-process-in-one-line-using-bash-and-regex

# See https://stackoverflow.com/questions/1250079/how-to-escape-single-quotes-within-single-quoted-strings
# for the crazy quotation.

for i in {11..37}
do
  echo cloud-$i
  ssh cloud-$i 'ps aux |grep java |grep -v datanode |grep -v namenode |grep -v grep |grep -v kill-all-java |awk '"'"'{print $2}'"'"' |xargs --verbose --no-run-if-empty kill -9'
done
