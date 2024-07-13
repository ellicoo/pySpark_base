# coding:utf8
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

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
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext  # 操作rdd

    # 基于RDD转换成DataFrame：形如[[张三，11]，[王武，21]]
    # 不解嵌套
    rdd = sc.textFile("../data/input/sql/people.txt"). \
        map(lambda x: x.split(",")). \
        map(lambda x: (x[0], int(x[1])))

    # 构建表结构的描述对象: StructType对象
    # nullable:表示是否允许为空
    # 使用add给DataFrame添加列--得到StructType对象
    schema = StructType().add("name", StringType(), nullable=True). \
        add("age", IntegerType(), nullable=False)

    # 基于StructType对象去构建RDD到DF的转换
    # 构建DataFrame，需要2个基本参数：1）数据row集合(可以是列表或者元祖)，2）表描述信息schema(即指定表名，列名，列的类型)
    # schema对象即StructType对象
    df = spark.createDataFrame(rdd, schema=schema)

    df.printSchema()
    df.show()

    spark.stop()