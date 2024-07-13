# coding:utf8
import os

# from pyspark.pandas import DataFrame
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

"""
-------------------------------------------------
   Description :	TODO：SparkSQL模版
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

Pyspark 中有多个 DataFrame 相关的类，这可能会有点令人困惑。以下是其中一些常见的 DataFrame 类，以及它们之间的层级关系：

pyspark.sql.DataFrame：这是 Spark SQL 模块中的主要 DataFrame 类，它用于处理分布式数据。它支持 SQL 查询、结构化数据操作等。

pyspark.pandas.DataFrame：这是 PySpark 的扩展，它允许你使用 Pandas 风格的 API 处理数据。虽然它称为 DataFrame，
                          但它不是 Spark SQL 中的 DataFrame。这是因为它是一个在 Spark 集群中使用 Pandas 数据结构的封装，
                          通常用于小规模数据集。

pyspark.sql.pandas.DataFrame：这是 PySpark 3.0.0 版本引入的新类，用于将 Pandas DataFrame 
                              转换为 Spark DataFrame 以便在 Spark 集群上运行。这是一个用于集成 Pandas 和 Spark 的工具。

层级关系如下：

pyspark.sql.DataFrame 是 Spark SQL 的核心数据结构，用于大规模分布式数据处理。
pyspark.pandas.DataFrame 是 PySpark 对 Pandas DataFrame 的封装，用于在小规模数据集上使用 Pandas 风格的操作。
pyspark.sql.pandas.DataFrame 是用于在 Spark 集群上执行 Pandas 操作的工具，
可以将 Pandas DataFrame 转换为 Spark DataFrame 以便分布式处理。


pandas DataFrame 的 DataFrame。它在内部维护了 Spark DataFrame
DataFrame的组成：
1、结构层面：
（1）StrucType对象描述整个DataFrame的表结构
（2）StrucField对象描述一个列的信息
2、数据层面：
（1）Row对象记录一行数据
（2）Column对象记录一列数据并包含列的信息
 (3)class Column(object):中有def alias(self, *alias, **kwargs):成员方法，给列字段名进行更名操作
"""

if __name__ == '__main__':
    # 1.构建SparkSession
    # 建造者模式：类名.builder.配置…….getOrCreate()
    # 自动帮你构建一个SparkSession对象，只要指定你需要哪些配置就可
    # 其中getOrCreate是内部类builder的成员方法，返回SparkSession外部类的对象
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("SparkSQLAppName") \
        .getOrCreate()

    # 2.数据输入

    # 3.数据

    # 4.数据输出

    # 5.关闭SparkContext
    spark.stop()

"""
# 虽然 Pandas-on-Spark（PySpark Pandas）等工具可以在 Spark 中使用 Pandas 的语法和操作，
# 但这并不等同于将 Pandas DataFrame 转变为分布式的 DataFrame。
# 只是允许在 Spark 中使用 Pandas 的一些功能
# Spark DataFrame 具有自己的 API（应用程序编程接口），用于在分布式计算环境中处理数据。
# Spark 提供了一组丰富的 DataFrame 操作和转换方法，使您能够在大规模数据集上执行数据处理、查询、过滤、聚合等操作。
# 这个 API 的设计使得 Spark 能够针对分布式计算进行优化，允许 Spark 作出智能的执行计划，以便高效地处理数据。

分析对象结构：
pyspark.sql.DataFrame 是 Spark SQL 的核心数据结构，用于大规模分布式数据处理，pyspark.sql中的DataFrame类中存在的常用成员方法：
    （1）
     select()--返回DataFrame类对象
    （2）
     alias("字段新别名")--返回DataFramae类对象
    （3）
     groupBy()--返回中间类 GroupedData 类的对象，还要调用 GroupedData类中的聚合方法才可以返回DataFrame类对象，进而才可以show
      
    

result_df2 = input_df.select(F.split('value',' ').alias("arrs"))\
    .select(F.explode('arrs').alias("word")).groupBy('word').agg(F.count('word').alias('cnt'))
其中input_df是pyspark.sql.DataFrame类的对象。这个DataFrame类对象中有select、alis、groupBy、agg

"""


class A(DataFrame):
    """
    pyspark.sql.DataFrame 是 Spark SQL 的核心数据结构，用于大规模分布式数据处理。

    """
    pass

