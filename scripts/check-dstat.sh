#!/bin/bash

for i in {11..37}
do
  echo cloud-$i
  ssh cloud-$i "ps aux |grep dstat |grep -v grep |grep -v check-dstat"
done
