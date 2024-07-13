# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：collect算子
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
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('c', 3), ('f', 1), ('b', 11), ('c', 3), ('a', 1), ('c', 5), ('e', 1), ('n', 9), ('a', 1)], 3)
    # 使用sortBy对rdd执行排序

    # 按照value 数字进行排序
    # 参数1函数, 表示的是 ,  告知Spark 按照数据的哪个列进行排序
    # 参数2: True表示升序 False表示降序
    # 参数3: 排序的分区数
    """注意: 如果要全局有序, 排序分区数请设置为1"""

    # 使用collect算子要预判数据集不太大时--把可
    # collect是action算子 --
    print(rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=1).collect())

    # 按照key来进行排序
    print(rdd.sortBy(lambda x: x[0], ascending=False, numPartitions=1).collect())

    sc.stop()

    # collect：

    # collect--将rdd分布式对象中的每个分区数据，都发送到Driver中，形成一个python list 对象

    # collect 分布式 --转-- 本地集合
    # 相对于
    # parallelize 本地或者单机对象 --转-- 分布式的带有分区的rdd集合对象

    # collect之前时rdd分布式对象，collect之后变成本地的python集合对象了
