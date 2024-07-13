# coding:utf8
import os
import string
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import pandas as pd
from pyspark.sql import functions as F
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# python中做不了UDAF，借助pandas的udf来实现UDAF解决纵向迭代问题
if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)
    df = rdd.map(lambda x: [x]).toDF(['num'])

    # 折中的方式 就是使用RDD的mapPartitions 算子来完成聚合操作
    # 如果用mapPartitions API 完成UDAF聚合, 一定要单分区
    single_partition_rdd = df.rdd.repartition(1)

    def process(iter):
        sum = 0
        for row in iter:
            sum += row['num']

        return [sum]    # 一定要嵌套list, 因为mapPartitions方法要求的返回值是list对象


    print(single_partition_rdd.mapPartitions(process).collect())
