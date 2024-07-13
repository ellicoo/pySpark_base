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

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # 通过读取文件夹构建DataFrame对象
    # format("text")时，会报--Text data source does not support int data type.
    # 改成format("csv")

    # 原因分析：当使用Apache Spark中的DataFrame API中的read.format("text")方法时，
    # 你会尝试读取文本文件并将其解释为文本数据。这意味着Spark将文本文件的每一行视为一个字符串，
    # 而不会对其进行解析或拆分。
    # 在这种情况下，如果你尝试为数据模式指定整数类型，
    # 如在schema("id INT, subject STRING, score INT")，就相当于做了三段切割
    # 会导致错误。文本数据源不支持对数据进行强制解析为特定的数据类型，因为它只将每行数据解释为文本。

    # 为了解决这个问题，你应该使用read.format("csv")而不是read.format("text")，因为CSV（逗号分隔值）
    # 格式允许你指定列的数据类型，使Spark能够正确解析和处理数据。当你使用CSV格式时，
    # 你可以将数据模式与数据类型匹配，就像你在你的示例中所做的那样。这样，Spark将能够正确解释每列的数据类型，

    # SparkSession类的read成员方法返回一个DataFrameReader类的对象
    #而DataFrameReader类中有：
    #         format、schema、option、options、load、json、table、parquet、text、csv、orc、jdbc方法

    # 使用 spark.read 即用SparkSession对象的read方法 可以获取 SparkSQL 中的外部数据源访问框架 DataFrameReader
    # DataFrameReader 有三个组件 format, schema, option
    # DataFrameReader 有两种使用方式, 一种是使用 load 加 format 指定格式, 还有一种是使用封装方法 csv, json 等

    df = spark.read.format("csv").\
        schema("id INT, subject STRING, score INT").\
        load("../data/input/sql/stu_score.txt")

    schema = StructType().add("data", StringType(), nullable=True)
    df1 = spark.read.format("text"). \
        schema(schema=schema). \
        load("../data/input/sql/stu_score.txt")

    df1.printSchema()
    df1.show()

    # Column对象的获取
    id_column = df['id']
    subject_column = df['subject']

    # DSL风格演示
    # select--显示列值
    # sparksession类的select语句需要传入DataFrame的Cloumn列对象
    df.select(["id", "subject"]).show() # 写list去传递，也可以传递Str；也可以传递Column
    df.select("id", "subject").show()
    df.select(id_column, subject_column).show() # Column对象是DataFrame中一列数据的集合，且带有列的信息描述
    df.select(df['id'], df['subject']).show()

    # filter API
    df.filter("score < 99").show() # 表达式condition参数是可变参数--接收传入字符串和Column对象
    df.filter(df['score'] < 99).show() # 传入Column对象，拿到Column对象--df['score']

    # where API -- 选行值
    df.where("score < 99").show()
    df.where(df['score'] < 99).show()

    # group By API
    # 能.show的，说明返回值都是DataFrame--类似rdd中的转换类算子，返回值都是rdd
    df.groupBy("subject").count().show() # 先按照subject列分组，再聚合
    df.groupBy(df['subject']).count().show()

    """
    DataFrame对象.groupBy()：返回GroupedData对象
    GroupedData对象.聚合方法如count等：返回DataFramed对象
    所以df.groupBy(df['subject']).count()才可以.show
    """


    # df.groupBy API的返回值 GroupedData
    # GroupedData对象 不是DataFrame
    # 它是一个 有分组关系的数据结构, 有一些API供我们对分组做聚合
    # SQL: group by 后接上聚合: sum avg count min man
    # GroupedData 类似于SQL分组后的数据结构, 同样有上述5种聚合方法
    # GroupedData 调用聚合方法后, 返回值依旧是DataFrame
    # GroupedData 只是一个中转的对象, 最终还是要获得DataFrame的结果
    r = df.groupBy("subject") # 返回GroupedData中转对象
    print(type(r)) # r对象没有show,只要DataFrame对象才有。
    spark.stop()
