# coding:utf8
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
"""
-------------------------------------------------
   Description :	TODO：DataFrame的入门操作
   SourceFile  :	Demo05_MapFunction
   Author      :	81196
   Date	       :	2023/9/7
-------------------------------------------------
"""
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

DataFrame支持两种风格进行编程，分别是：
（1）DSL风格：Domain Specific Language -- 领域特定语言，说人话就是DataFrame的特有API
            DSL风格意思就是以调用API的方式处理data,比如：df.where().limit()

（2）SQL风格：使用SQL语句来处理Dataframe的数据，比如：spark.sql('select * from xxx')

"""

# 在sparkSQL中想要想要使用SQL语法风格
#   把DataFrame这种数据结构注册成一张临时的视图
#   即可使用：SparkSession类对象.sql("SQL语句")

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv").\
        schema("id INT, subject STRING, score INT").\
        load("../data/input/sql/stu_score.txt")

    schema = StructType().add("data", StringType(), nullable=True)
    df1 = spark.read.format("text"). \
        schema(schema=schema). \
        load("../data/input/sql/stu_score.txt")

    # 注册成临时表
    df.createTempView("score") # 注册临时视图(表)
    df.createOrReplaceTempView("score_2") # 注册临时表，如果存在就替换  临时视图
    df.createGlobalTempView("score_3") # 注册全局临时视图 全局临时视图在使用的时候 需要在前面带上global_temp. 前缀
    # 注册全局临时视图--全局表，场景：需要跨越多个SparkSession对象中使用，即一个程序代码文件内有多个SparkSession对象时
    # 全局的场景不多，我们很少在一个程序中创建多个sparksession对象。

    # 可以通过SparkSession对象的sql api来完成sql语句的执行
    # SparkSession对象的sql方法返回的是：DataFrame类对象
    spark.sql("SELECT subject, COUNT(*) AS cnt FROM score GROUP BY subject").show()
    spark.sql("SELECT subject, COUNT(*) AS cnt FROM score_2 GROUP BY subject").show()
    spark.sql("SELECT subject, COUNT(*) AS cnt FROM global_temp.score_3 GROUP BY subject").show()
