#!/bin/bash

N=30 # число запусков для статистики
DATANODES_COUNT=1 # количество рассматриваемых DataNode  
OPTIMIZED=False # флаг, запускать ли оптимизированный Spark


while [[ $# -gt 0 ]]
do
    case "$1" in 
        --optimized) 
            OPTIMIZED="$2"
            shift 2 
            ;; 
        --datanodes)
            DATANODES_COUNT="$2" 
            shift 2 
            ;; 
        *) 
            echo "Неизвестный параметр: $1" 
            exit 1
            ;; 
    esac 
done 
echo "Количество DataNode: $DATANODES_COUNT, оптимизированный запуск Spark: $OPTIMIZED."

if [[ $DATANODES_COUNT -eq 1 ]] 
then
    docker-compose -f docker-compose.yml up -d 
elif [[ $DATANODES_COUNT -eq 3 ]] 
then
    docker-compose -f docker-compose-3.yml up -d 
else
    echo "Переданное количество DataNode=$DATANODES_COUNT не поддерживается!"
    exit 1
fi


docker cp .data/data.csv namenode:/
docker cp ./HDFS.sh namenode:/ 
docker exec -it namenode bash HDFS.sh 

docker cp ./spark_app.py spark-master:/ 
docker cp ./Spark.sh spark-master:/ 
docker exec -it spark-master bash Spark.sh ${OPTIMIZED} 

docker cp spark-master:/log.txt ./logs/log_DataNodes_${DATANODES_COUNT}_opt_${OPTIMIZED}.txt 

if [[ $DATANODES_COUNT -eq 1 ]] 
then
    docker-compose -f docker-compose.yml down 
else
    docker-compose -f docker-compose-3.yml down 
fi