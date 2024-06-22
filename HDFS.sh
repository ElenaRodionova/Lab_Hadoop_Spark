#!/bin/bash

hdfs dfs -D dfs.block.size=32M -put /customer_shopping_data.csv / # отправляем данные с NameNode в hdfs
hdfs dfs -D dfs.block.size=32M -put /data.csv / # отправляем данные с NameNode в hdfs
hdfs dfsadmin -setSpaceQuota 10g / #ограничение на используемую память
exit # выходим из вложенного терминала Hadoop
