# -*- coding: utf-8 -*-

import numpy as np
import sys
import os
import psutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark import SparkContext, SparkConf
from pyspark.ml import Pipeline
from pyspark.sql.functions import count, when, isnull,col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

OPTIMIZED = True if sys.argv[1] == "True" else False
time_start = time.time()

SparkContext.getOrCreate(SparkConf().setMaster('spark://spark-master:7077')).setLogLevel("INFO")
spark = SparkSession.builder.master("spark://spark-master:7077").appName("practice").getOrCreate() 

data = spark.read.format("csv").option("header", "true").option('inferSchema', 'true').load("hdfs://namenode:9000/data.csv") 

if OPTIMIZED:
    data.cache()
    data.persist()
    data = data.repartition(4)

data = data.withColumn("invoice_date", (unix_timestamp("invoice_date", format='yyyy-MM-dd') / 86400).cast(FloatType()))

for col in ['gender', 'category', 'payment_method', 'shopping_mall']:
    indexer = StringIndexer(inputCol=col, outputCol="{}_index".format(col))
    data = indexer.fit(data).transform(data)
    data = data.drop(col)
    data = data.withColumnRenamed("{}_index".format(col), col)

one_hot_encoder = OneHotEncoder(inputCol='gender', outputCol='gender_one_hot')
one_hot_encoder = one_hot_encoder.fit(data)
data = one_hot_encoder.transform(data)
data = data.drop('gender')

assembler = VectorAssembler(inputCols=["age", "quantity", "invoice_date", "year", "month", "category", "payment_method", "shopping_mall", "gender_one_hot"], outputCol="features")
data = assembler.transform(data)

data_train, data_test = data.randomSplit([0.7, 0.3])
if OPTIMIZED:
    data_train.cache()
    data_train = data_train.repartition(4)
    data_test.cache()
    data_test = data_test.repartition(4)

rf = RandomForestRegressor(featuresCol="features", labelCol="price")
model = rf.fit(data_train)

predictions = model.transform(data_test)
evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
rmse = evaluator.evaluate(predictions)
print('Coefficient of determination (R2) in the test data:', r2)
print('Average absolute error (MAE) in the test data:', mae)
print('Standard error (RMSE) in the test data:', rmse)


time_res = time.time() - time_start
RAM_res = psutil.Process(os.getpid()).memory_info().rss / (float(1024)**2)

spark.stop()

with open('/log.txt', 'a') as f:
    f.write("Time: " + str(time_res) + " seconds, RAM: " + str(RAM_res) + " Mb.\n")
