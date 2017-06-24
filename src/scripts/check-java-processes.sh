#!/bin/bash

for i in {11..37}
do
  echo cloud-$i
  ssh cloud-$i "ps aux |grep java |grep -v datanode |grep -v namenode |grep -v grep"
done
