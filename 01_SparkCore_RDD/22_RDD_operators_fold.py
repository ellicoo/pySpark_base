# coding:utf8
import os
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

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    # 带有初始值的reduce算子 -- action算子
    # 这个初始值10是三个分区的，每个分区10总共30,分区间的初始值也是10，三个区间则初始值是10---所有一起共40
    # 先分区内聚合再分区间聚合
    # 初始值在分区内生效，分区间也生效
    # 很少使用初始值的聚合方法--常用reduce
    print(rdd.fold(10, lambda a, b: a + b))
    sc.stop()
