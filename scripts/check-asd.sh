#!/bin/bash

for i in {11..37}
do
  echo cloud-$i
  ssh cloud-$i "ps aux |grep asd |grep -v grep"
done
