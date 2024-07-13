# coding:utf8
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F
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
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    sc = spark.sparkContext

    """
    spark.sql.shuffle.partitions 参数指的是, 在sql计算中, shuffle算子阶段默认的分区数是200个.
    对于集群模式来说, 200个默认也算比较合适
    如果在local下运行, 200个很多, 在调度上会带来额外的损耗
    所以在local下建议修改比较低 比如2\4\10均可
    这个参数和Spark RDD中设置并行度的参数 是相互独立的.
    """

    # 1. 读取数据集
    schema = StructType().add("user_id", StringType(), nullable=True).\
        add("movie_id", IntegerType(), nullable=True).\
        add("rank", IntegerType(), nullable=True).\
        add("ts", StringType(), nullable=True)

    # 使用DataFrameReader类的format、option方法来自定义读取文件的方式，比如切分等，并返回DataFrameReader类的对象
    # 再通过DataFrameReader类对象的schema方法去给读出来的数据赋予一个表结构
    # 假如读取的文件是json、parquet文件，则不需要赋予一个表结构，其自带schema表 结构
    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load("../data/input/sql/u.data")


    # TODO 1: 用户平均分：F.round("avg_rank", 2) 对avg_rank平均分保留两位小数点
    df.groupBy("user_id").\
        avg("rank").\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank", 2)).\
        orderBy("avg_rank", ascending=False).\
        show()

    # TODO 2: 电影的平均分查询
    df.createTempView("movie")
    spark.sql("""
        SELECT movie_id, ROUND(AVG(rank), 2) AS avg_rank FROM movie GROUP BY movie_id ORDER BY avg_rank DESC
    """).show()

    # TODO 3: 查询大于平均分的电影的数量 # Row
    # 对一张整表进行求平均值：df.select(F.avg(df['rank'])，返回的是是一个值，需要转为DataFrame对象
    # 使用DataFrame类对象的first()方法可以返回 第一行，行对象Row类型，而这个Row类型里面只要1个列，把这个列取出来
    print("大于平均分电影的数量: ", df.where(df['rank'] > df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

    # TODO 4: 查询高分电影中(>3)打分次数最多的用户, 此人打分的平均分
    # 先找出这个人
    # limit(1) 返回第一行
    # first()求出第一行，使用时，可以省略limit(1)
    # first()['user_id']是row对象，解析出第一行的id

    user_id = df.where("rank > 3").\
        groupBy("user_id").\
        count().\
        withColumnRenamed("count", "cnt").\
        orderBy("cnt", ascending=False).\
        limit(1).\
        first()['user_id']
        # 使用时，可以省略limit(1)

    # 计算这个人的打分平均分
    df.filter(df['user_id'] == user_id).\
        select(F.round(F.avg("rank"), 2)).show()

    # TODO 5: 查询每个用户的平局打分, 最低打分, 最高打分
    # 按照用户进行分组，一次性做三个聚合---GroupedData 中转对象的agg
    # GroupedData 中转对象的成员方法agg()可以在参数列表中实现多个聚合操作
    df.groupBy("user_id").\
        agg(
            # 单独的column对象不是DataFrame，只有DataFrame对象才有withColumnRenamed更名
            # 要么等这个表操作完后，在底下一次性改列名。而column对象也有更名操作alias
            F.round(F.avg("rank"), 2).alias("avg_rank"),
            F.min("rank").alias("min_rank"),
            F.max("rank").alias("max_rank")
        ).show()

    # TODO 6: 查询评分超过100次的电影, 的平均分 排名 TOP10
    df.groupBy("movie_id").\
        agg(
            F.count("movie_id").alias("cnt"),
            F.round(F.avg("rank"), 2).alias("avg_rank")
        ).where("cnt > 100").\
        orderBy("avg_rank", ascending=False).\
        limit(10).\
        show()

    # time.sleep(10000)

"""
1. agg: 它是GroupedData对象的API, 作用是 在里面可以写多个聚合
2. alias: 它是Column对象的API, 可以针对一个列 进行改名
3. withColumnRenamed: 它是DataFrame的API, 可以对DF中的列进行改名, 一次改一个列, 改多个列 可以链式调用
4. orderBy: DataFrame的API, 进行排序, 参数1是被排序的列, 参数2是 升序(True) 或 降序 False
5. first: DataFrame的API, 取出DF的第一行数据, 返回值结果是Row对象.
# Row对象 就是一个数组, 你可以通过row['列名'] 来取出当前行中, 某一列的具体数值. 返回值不再是DF 或者GroupedData 或者Column而是具体的值(字符串, 数字等)
"""




