#!/bin/bash

apk add --update make automake gcc g++ 
apk add --update python-dev
apk add linux-headers 
pip install numpy psutil
for ((i=1; i<=30; i++)) 
do 
    /spark/bin/spark-submit /spark_app.py $1 
done 
exit 