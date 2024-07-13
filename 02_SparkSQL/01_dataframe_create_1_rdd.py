# coding:utf8
import os

from pyspark.sql import SparkSession

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

    # 基于RDD转换成DataFrame
    rdd = sc.textFile("../data/input/sql/people.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: [x[0], int(x[1])])

    # 构建DataFrame对象
    # 参数1 被转换的RDD--首先需要处理成元祖或者列表
    # 参数2 指定列名, 通过list的形式指定, 按照顺序依次提供字符串名称即可
    df = spark.createDataFrame(rdd, schema=['name', 'age']) # 不指定列存储内容的类型时，可以自动识别类型

    # 打印DataFrame的表结构
    df.printSchema()

    # 打印df中的数据
    # 参数1 表示 展示出多少条数据, 默认不传的话是20
    # 参数2 表示是否对列进行截断, 如果列的数据长度超过20个字符串长度, 后续的内容不显示以...代替
    #      表示不阶段全部显示, 默认是True
    df.show(20, False) # False不进行截断

    # 将DF对象转换成临时视图表, 可供sql语句查询
    print('-------使用createOrReplaceTempView---------')

    df.createOrReplaceTempView("people")
    spark.sql("SELECT * FROM people WHERE age < 30").show()

    print('-------使用createTempView---------')

    df.createTempView("humen")
    spark.sql("SELECT * FROM humen WHERE age < 30").show()

    spark.stop()

