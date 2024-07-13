# coding:utf8
import os

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
DataFrame类有列更名操作withColumnRenamed
Column类的列更名操作alias
1、结构层面：
（1）StrucType对象描述整个DataFrame的表结构
（2）StrucField对象描述一个列的信息
2、数据层面：
（1）Row对象记录一行数据
（2）Column对象记录一列数据并包含列的信息，Column对象有更名列名操作alias

DataFrame支持两种风格进行编程，分别是：
（1）DSL风格：Domain Specific Language -- 领域特定语言，说人话就是DataFrame的特有API
            DSL风格意思就是以调用API的方式处理data,比如：df.where().limit()

（2）SQL风格：使用SQL语句来处理Dataframe的数据，比如：spark.sql('select * from xxx')

pyspark 提供了一系列计算函数的 pyspark.sql.functions包 提供给SparkSQL使用，其操作的返回值多数都是Column对象

说你人话：pyspark.sql.functions包里面的函数可以在SparkSQL中使用

"""
"""
pyspark.sql.functions 模块包含了许多用于在Spark中进行数据处理和转换的函数。
这些函数可以用于操作和处理Spark DataFrame 中的列。以下是一些常见的功能：

(1)数学和统计函数：

col(): 引用列。
lit(): 创建一个常量列。
abs(): 绝对值。
sqrt(): 平方根。
sum(): 求和。
avg(): 平均值。
min(): 最小值。
max(): 最大值。

(2)字符串函数：

concat(): 连接字符串。
substring(): 获取子字符串。
length(): 字符串长度。
trim(): 去除字符串两侧的空格。
lower(), upper(): 转换为小写或大写。

(3)日期和时间函数：

current_date(), current_timestamp(): 获取当前日期或时间戳。
datediff(): 计算日期之间的天数差异。
date_add(), date_sub(): 增加或减少日期。
year(), month(), day(): 提取年、月、日。

(4)条件表达式和空值处理：

when(), otherwise(): 条件表达式。
coalesce(): 返回第一个非空值。
isnan(), isnull(): 检查是否为 NaN 或 NULL。

(5)数组和集合函数：
array(): 创建数组。
explode(): 展开数组中的元素。
size(): 数组大小。
array_contains(): 检查数组是否包含特定值。

(6)窗口函数：
window(): 定义窗口规范。
row_number(), rank(), dense_rank(): 窗口排名函数。

(7)其他功能：
rand(), randn(): 生成随机数。
first(), last(): 获取第一个或最后一个元素。
lead(), lag(): 获取下一个或上一个元素。

这只是 pyspark.sql.functions 中一小部分功能的介绍。
你可以根据具体的需求查看Spark文档以获取更详细的信息和用法。
这些函数在Spark SQL和DataFrame API中都可以使用。

"""
if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        getOrCreate()
    sc = spark.sparkContext

    # TODO 1: SQL 风格进行处理
    # flatMap展开成[word,word,word,....],这种rdd的数据结构DataFrame是不支持的，DataFrame需要有嵌套形式的数据结构
    # 否则会报错：Can not infer schema for type: <class 'str'>
    rdd = sc.textFile("../data/input/words.txt"). \
        flatMap(lambda x: x.split(" ")). \
        map(lambda x: [x])

    print(rdd.collect())
    df = rdd.toDF(["word"])

    # 注册DF为表格
    df.createTempView("words")
    # SparkSession对象的sql方法返回的是：DataFrame类对象，从而使用DataFrame类对象的show()方法
    spark.sql("SELECT word, COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()

    # TODO 2: DSL 风格处理
    # text方式会以整行作为一列的内容
    # 如果指定读取模式为text格式文件时，列名字默认为value
    df = spark.read.format("text").load("../data/input/words.txt")

    # withColumn方法
    # 方法功能: 用旧列的值创造新列，对已存在的列进行操作, 返回一个新的列, 如果名字和老列相同, 那么替换, 否则作为新列存在
    # withColumn是DataFame类的成员方法，返回值为DataFame类对象，从而df2就是一个DataFame对象，此时就可以使用DataFrame类的
    # 另一个函数：groupBy
    df2 = df.withColumn("value", F.explode(F.split(df['value'], " "))) # 切分df的value列
    #
    # df2.groupBy("value").\
    #     count().\
    #     withColumnRenamed("value", "word").\
    #     withColumnRenamed("count", "cnt").\
    #     orderBy("cnt", ascending=False).\
    #     show()

    # 使用了 "count" 别名，而 "count" 是一个关键字，表示聚合函数，而不是列名。因此，Spark 引擎无法解析 "count" 别名。
    # 错误示例--经过groupBy和count产生的是新对象，而不是df2旧的DataFrame对象,
    # 否则报Cannot resolve column name "count" among (value)错误，因为新生产的count列还没有被df2引用
    # 如果使用withColumnRenamed，则可以直接进行链式计算，因为sparksession类中的select方法，则需要已经完成更新的对象

    # 当然，如果没有groupBy和count函数，可以直接使用select(df2["value"].alias("word"),df2["count"].alias("cnt"))
    # df2.groupBy("value"). \
    #     count(). \
    #     select(df2["value"].alias("word"),df2["count"].alias("cnt")) .\
    #     orderBy("cnt", ascending=False). \
    #     show()

    # 正确示例
    # 单独的column对象不是DataFrame，只有DataFrame对象才有withColumnRenamed更名
    # 要么等这个表操作完后，在底下一次性改列名。而column对象也有更名操作alias
    df3 = df2.groupBy("value").count()
    df3.select(df3["value"].alias("word"), df3["count"].alias("cnt")). \
        orderBy("cnt", ascending=False). \
        show()

