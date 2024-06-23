#!/bin/bash

hdfs dfs -D dfs.block.size=32M -put /data.csv / 
hdfs dfsadmin -setSpaceQuota 10g / 
exit
