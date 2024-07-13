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

    # 读取parquet类型的文件
    """
    parquet优点：--体积小，列存储，内置Schma
    parquet:是Spark中常用的一种'列式'存储文件格式
    和hive中的ORC差不多，他两都是列存储格式
    
    parquet内置schema(列名、列类型、是否为空)，不需要.schema(schema=schema)
    存储是以列为存储格式
    存储是序列化存储在文件中的(体积小，因为序列化了)
    
    parquet文件不能直接打开查看，如果想要查看内容可以在pycharm中安装如下插件来查看
    Avro and parquet viewer 底部有Avro/Parquet 点击，然后拖动文件到下面即可看到数据  
    
    """
    df = spark.read.format("parquet").load("../data/input/sql/users.parquet")

    df.printSchema()
    df.show()

    spark.stop()
