# coding:utf8
import os
# from my_utils.get_local_file_system_absolute_path import get_absolute_path
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

    rdd = sc.parallelize([('hadoop', 1), ('spark', 1), ('hello', 1), ('flink', 1), ('hadoop', 1), ('spark', 1)])

    # rdd五大特性中：每个KV类型的RDD可以有分区器，默认是hash，也可以进行自定义的分区操作
    # partitionBy(参数1，参数2)算子--就是对KV型的rdd进行 自定义分区操作的算子
    # 参数1：给分区数量
    # 参数2：自定义分区规则，函数传入--(K)->int  函数接收一个参数(任意类型)进来，返回一个int 分区号
    #       将key传给这个函数，你自己写逻辑，决定返回一个分区编号，分区编号从0开始，不要超出分区数-1

    # 使用par titionBy 自定义 分区

    def process(k):
        if 'hadoop' == k or 'hello' == k: return 1
        if 'spark' == k: return 2
        return 3


    print(rdd.partitionBy(3, process).glom().collect())
    # 参数3要求自定义分区函数必须返回的分区号为整型，推荐：0、1、2，这个根据分区号3来的推荐
    sc.stop()
