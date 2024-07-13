# coding:utf8
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

# 使用pandas的dataframe来构建DataFrame
import pandas as pd

"""
-------------------------------------------------
   Description :	TODO：基于RDD，构建DataFrame对象
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

    # 基于Pandas的DataFrame构建SparkSQL的DataFrame对象
    pdf = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["张大仙", "王晓晓", "吕不为"],
            "age": [11, 21, 11]
        }
    )

    df = spark.createDataFrame(pdf)

    df.printSchema()
    df.show()

    spark.stop()
