# coding:utf8
import os
from my_utils.get_local_file_system_absolute_path import get_absolute_path
from pyspark import SparkConf, SparkContext

"""
-------------------------------------------------
   Description :	TODO：action算子开始
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
    # saveAsTextFile--支持写数据到本地文件系统和hdfs文件系统
    rdd1 = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 1)
    rdd2 = sc.parallelize([1, 3, 2, 4, 7, 9, 6], 3)
    # 分区数不同时，几个分区对应几个文件，因为数据没有拉到driver，与foreach这点一样--分区执行的，形成分区对应的文件

    rdd1.saveAsTextFile(get_absolute_path('../data/output/out1'))
    rdd2.saveAsTextFile(get_absolute_path('../data/output/out2'))
    rdd1.saveAsTextFile("hdfs://node1:8020/output/out1")
    rdd2.saveAsTextFile("hdfs://node1:8020/output/out2")
    sc.stop()

# 使用foreach,saveAsTextFile产生的结果集很好，但是如果有10TB的结果集合往控制台输出需要使用saveAsTextFile，就不会爆内存


