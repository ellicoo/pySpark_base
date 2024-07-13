# coding:utf8
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

"""
-------------------------------------------------
   Description :	TODO：通过SparkSQL的统一API进行数据读取构建DataFrame
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""

# 共享变量--driver中的本地数据和executor中的rdd数据需要一起进行运算时使用
# 0.设置系统环境变量
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241/'
os.environ['HADOOP_HOME'] = '/export/server/hadoop'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

"""
DataFrame的组成：
1、结构层面：
（1）StrucType对象描述整个DataFrame的表结构
（2）StrucField对象描述一个列的信息
2、数据层面：
（1）Row对象记录一行数据
（2）Column对象记录一列数据并包含列的信息

"""

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext

    # 构建StructType, text数据源, 读取数据的特点是, 将一整行只作为`一个列`读取, 默认列名是value（但我们进行修改） 类型是String
    schema = StructType().add("data", StringType(), nullable=True)

    # spark是SparkSession类的对象

    df = spark.read.format("text"). \
        schema(schema=schema). \
        load("../data/input/sql/people.txt")

    df.printSchema()
    df.show()

    spark.stop()
