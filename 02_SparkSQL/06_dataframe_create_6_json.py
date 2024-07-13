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
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    """
    json数据源比text好，自带Schema信息,所以没有 .schema(schema=schema)
    """
    # 方式1：
    # JSON类型自带有Schema信息
    df = spark.read.format("json").\
        load("../data/input/sql/people.json")

    df.printSchema()
    df.show()
    print('---------------------')
    # 方式2：
    # b.读取变成DF
    input_df = spark.read.json("../data/input/sql/people.json")
    input_df.printSchema()
    input_df.show()

    print('----------------------')
    # 方式3
    # a.读取变成rdd--会将每条数据作为一个字符串，进行输出，并不会解析数据中有列
    input_rdd = spark.sparkContext.textFile("../data/input/sql/people.json")
    input_rdd.foreach(lambda x: print(x))

    spark.stop()

