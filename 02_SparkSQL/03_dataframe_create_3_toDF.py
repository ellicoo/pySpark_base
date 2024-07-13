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
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext

    # 基于RDD转换成DataFrame
    # DataFrame需要有嵌套形式的数据结构,比如列表、 元祖
    # 否则会报错：TypeError: field age: IntegerType can not accept object ' 29' in type <class 'str'>
    rdd = sc.textFile("../data/input/sql/people.txt"). \
        map(lambda x: x.split(",")). \
        map(lambda x: (x[0], int(x[1])))

    # toDF的方式构建DataFrame
    # RDD类的toDF()成员方法：
    #     def toDF(
    #         self: RDD[RowLike],
    #         schema: Optional[List[str]] = ...,
    #         sampleRatio: Optional[float] = ...,
    #     ) -> DataFrame: ...
    df1 = rdd.toDF(["name", "age"]) # 只给列名，列类型靠对rdd的推断，只适合对类型不敏感，想要快速拿来用的
    df1.printSchema()
    df1.show()
    print('------对类型敏感时------')

    # toDF的方式2 --对类型敏感时，使用StructType()指定name列的类型为：StringType()即String类型
    # 通过StructType来构建
    schema = StructType().add("name", StringType(), nullable=True). \
        add("age", IntegerType(), nullable=False)

    df2 = rdd.toDF(schema=schema)
    df2.printSchema()
    df2.show()

    spark.stop()
