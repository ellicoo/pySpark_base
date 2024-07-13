# coding:utf8

# 导入Spark的相关包
import os

from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：通过parallelize创建rdd
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


if __name__ == '__main__':
    """
    SparkCore--spark的模块，无标准的数据结构，什么都可以，存什么数据都可以
    """

    # 0. 初始化执行环境 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 演示通过并行化集合的方式去创建RDD, 本地集合 -> 分布式对象(RDD)
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    # parallelize方法, 没有给定 分区数, 默认分区数是多少?  根据CPU核心来定
    print("默认分区数: ", rdd.getNumPartitions())

    rdd = sc.parallelize([1, 2, 3], 10)
    print("分区数: ", rdd.getNumPartitions())

    # collect方法, 是将RDD(分布式对象)中每个分区的数据, 都发送到Driver中, 形成一个Python List对象
    # collect: 分布式 转 -> 本地集合
    print("rdd的内容是: ", rdd.collect())

    # 5.关闭SparkContext
    sc.stop()