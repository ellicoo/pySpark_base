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

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    sc = spark.sparkContext

    # 假设 有三个数字  1 2 3  我们传入数字 ,返回数字所在序号对应的 字母 然后和数字结合形成dict返回
    # 比如传入1 我们返回 {"num":1, "letters": "a"}
    rdd = sc.parallelize([[1], [2], [3]])
    df = rdd.toDF(["num"])

    # 注册UDF
    def process(data):
        return {"num": data, "letters": string.ascii_letters[data]}

    """
    UDF的返回值是字典的话, 需要用StructType来接收
    """
    udf1 = spark.udf.register("udf1", process, StructType().add("num", IntegerType(), nullable=True).\
                              add("letters", StringType(), nullable=True))

    df.selectExpr("udf1(num)").show(truncate=False)
    df.select(udf1(df['num'])).show(truncate=False)
